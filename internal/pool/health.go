/*
 * Copyright (C) 2025 rtparityd contributors
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package pool

import (
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

const (
	HealthCheckInterval = 5 * time.Minute
	MaxFailureCount     = 3
	FailureWindow       = 30 * time.Minute
)

type HealthStatus string

const (
	HealthOK       HealthStatus = "ok"
	HealthSuspect  HealthStatus = "suspect"
	HealthFailing  HealthStatus = "failing"
	HealthFailed   HealthStatus = "failed"
	HealthExcluded HealthStatus = "excluded"
)

type DiskHealth struct {
	DiskID       string
	DiskUUID     string
	Status       HealthStatus
	FailureCount int
	LastCheck    time.Time
	LastError    string
	SMARTData    *SMARTData
	ResponseTime time.Duration
}

type SMARTData struct {
	PowerOnHours         int64
	ReallocatedSectors   int
	CurrentPending       int
	OfflineUncorrectable int
	TempCelsius          int
	ReadErrorRate        int64
	WriteErrorRate       int64
}

type HealthMonitor struct {
	mu            sync.RWMutex
	disks         map[string]*DiskHealth
	poolID        string
	checkInterval time.Duration
	maxFailures   int
	failureWindow time.Duration
	stopCh        chan struct{}
	excludeHook   func(diskID string) error
}

func NewHealthMonitor(poolID string) *HealthMonitor {
	return &HealthMonitor{
		disks:         make(map[string]*DiskHealth),
		poolID:        poolID,
		checkInterval: HealthCheckInterval,
		maxFailures:   MaxFailureCount,
		failureWindow: FailureWindow,
		stopCh:        make(chan struct{}),
	}
}

func (m *HealthMonitor) RegisterDisk(diskID, diskUUID string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.disks[diskID]; !exists {
		m.disks[diskID] = &DiskHealth{
			DiskID:    diskID,
			DiskUUID:  diskUUID,
			Status:    HealthOK,
			LastCheck: time.Now(),
		}
	}
}

func (m *HealthMonitor) SetExcludeHook(hook func(diskID string) error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.excludeHook = hook
}

func (m *HealthMonitor) Start() {
	go m.monitorLoop()
}

func (m *HealthMonitor) Stop() {
	close(m.stopCh)
}

func (m *HealthMonitor) monitorLoop() {
	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.performPeriodicCheck()
		}
	}
}

func (m *HealthMonitor) performPeriodicCheck() {
	m.mu.RLock()
	allHealth := make(map[string]*DiskHealth)
	for k, v := range m.disks {
		allHealth[k] = v
	}
	m.mu.RUnlock()

	for diskID, health := range allHealth {
		if health.Status == HealthExcluded || health.Status == HealthFailed {
			continue
		}

		devicePath, err := VerifyDiskPath(health.DiskUUID)
		if err != nil {
			m.mu.Lock()
			if h, ok := m.disks[diskID]; ok {
				h.FailureCount++
				h.LastError = fmt.Sprintf("device not found: %v", err)
				if h.FailureCount >= m.maxFailures {
					h.Status = HealthFailing
				}
			}
			m.mu.Unlock()
			continue
		}

		if err := m.CheckDisk(devicePath, diskID); err != nil {
		}
	}
}

func (m *HealthMonitor) CheckDisk(devicePath, diskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	health, exists := m.disks[diskID]
	if !exists {
		return fmt.Errorf("disk %s not registered", diskID)
	}

	start := time.Now()

	if err := m.checkSMART(devicePath, health); err != nil {
		health.FailureCount++
		health.LastError = err.Error()
		health.Status = HealthSuspect

		if health.FailureCount >= m.maxFailures {
			health.Status = HealthFailing
		}

		if m.excludeHook != nil && health.FailureCount >= m.maxFailures {
			go m.excludeHook(diskID)
		}
	} else {
		if health.FailureCount > 0 && health.Status == HealthSuspect {
			health.FailureCount--
		}
		if health.FailureCount == 0 {
			health.Status = HealthOK
		}
		health.LastError = ""
	}

	health.ResponseTime = time.Since(start)
	health.LastCheck = time.Now()

	return nil
}

func (m *HealthMonitor) checkSMART(devicePath string, health *DiskHealth) error {
	output, err := exec.Command("smartctl", "-a", "-j", devicePath).Output()
	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			if exitErr.ExitCode() == 2 {
				return fmt.Errorf("SMART not supported or device not found")
			}
		}
		return fmt.Errorf("smartctl failed: %w", err)
	}

	jsonOutput := string(output)

	smartData, err := parseSMARTJSON(jsonOutput)
	if err != nil {
		return fmt.Errorf("parse SMART data: %w", err)
	}

	if err := m.validateSMARTHealth(smartData); err != nil {
		return err
	}

	health.SMARTData = smartData
	return nil
}

func parseSMARTJSON(output string) (*SMARTData, error) {
	data := &SMARTData{}

	re := regexp.MustCompile(`"id":(\d+),"name":"(\w+)","value":(\d+)`)
	matches := re.FindAllStringSubmatch(output, -1)

	for _, match := range matches {
		if len(match) != 4 {
			continue
		}
		id, _ := strconv.Atoi(match[1])
		value, _ := strconv.ParseInt(match[3], 10, 64)

		switch id {
		case 9:
			data.PowerOnHours = value
		case 5:
			data.ReallocatedSectors = int(value)
		case 197:
			data.CurrentPending = int(value)
		case 198:
			data.OfflineUncorrectable = int(value)
		case 194:
			data.TempCelsius = int(value)
		case 200:
			data.ReadErrorRate = value
		case 201:
			data.WriteErrorRate = value
		}
	}

	return data, nil
}

func (m *HealthMonitor) validateSMARTHealth(data *SMARTData) error {
	if data.ReallocatedSectors > 100 {
		return fmt.Errorf("reallocated sectors too high: %d", data.ReallocatedSectors)
	}
	if data.CurrentPending > 50 {
		return fmt.Errorf("current pending sectors too high: %d", data.CurrentPending)
	}
	if data.OfflineUncorrectable > 0 {
		return fmt.Errorf("offline uncorrectable sectors: %d", data.OfflineUncorrectable)
	}
	if data.TempCelsius > 60 {
		return fmt.Errorf("temperature too high: %d°C", data.TempCelsius)
	}
	if data.ReadErrorRate > 1000000 || data.WriteErrorRate > 1000000 {
		return fmt.Errorf("error rate too high")
	}
	return nil
}

func (m *HealthMonitor) GetDiskHealth(diskID string) (*DiskHealth, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	h, ok := m.disks[diskID]
	return h, ok
}

func (m *HealthMonitor) GetAllDiskHealth() map[string]*DiskHealth {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]*DiskHealth)
	for k, v := range m.disks {
		result[k] = v
	}
	return result
}

func (m *HealthMonitor) ExcludeDisk(diskID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	health, exists := m.disks[diskID]
	if !exists {
		return fmt.Errorf("disk %s not registered", diskID)
	}

	health.Status = HealthExcluded
	return nil
}

func (m *HealthMonitor) IsDiskHealthy(diskID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	health, ok := m.disks[diskID]
	if !ok {
		return false
	}
	return health.Status == HealthOK || health.Status == HealthSuspect
}

type HealthReporter struct {
	monitor *HealthMonitor
	state   *metadata.SampleState
}

func NewHealthReporter(monitor *HealthMonitor, state *metadata.SampleState) *HealthReporter {
	return &HealthReporter{
		monitor: monitor,
		state:   state,
	}
}

func (r *HealthReporter) GenerateReport() string {
	var sb strings.Builder
	sb.WriteString("=== Pool Health Report ===\n\n")

	allHealth := r.monitor.GetAllDiskHealth()
	for _, health := range allHealth {
		sb.WriteString(fmt.Sprintf("Disk: %s\n", health.DiskID))
		sb.WriteString(fmt.Sprintf("  Status: %s\n", health.Status))
		sb.WriteString(fmt.Sprintf("  Failures: %d\n", health.FailureCount))
		sb.WriteString(fmt.Sprintf("  Last Check: %s\n", health.LastCheck.Format(time.RFC3339)))
		if health.LastError != "" {
			sb.WriteString(fmt.Sprintf("  Last Error: %s\n", health.LastError))
		}
		if health.SMARTData != nil {
			sb.WriteString(fmt.Sprintf("  Temperature: %d°C\n", health.SMARTData.TempCelsius))
			sb.WriteString(fmt.Sprintf("  Reallocated Sectors: %d\n", health.SMARTData.ReallocatedSectors))
			sb.WriteString(fmt.Sprintf("  Power On Hours: %d\n", health.SMARTData.PowerOnHours))
		}
		sb.WriteString("\n")
	}

	return sb.String()
}

type QuickDiskCheck struct {
	devicePath string
	timeout    time.Duration
}

func NewQuickDiskCheck(devicePath string) *QuickDiskCheck {
	return &QuickDiskCheck{
		devicePath: devicePath,
		timeout:    30 * time.Second,
	}
}

func (c *QuickDiskCheck) Perform() error {
	done := make(chan error, 1)

	go func() {
		done <- c.performCheck()
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(c.timeout):
		return fmt.Errorf("disk check timed out after %v", c.timeout)
	}
}

func (c *QuickDiskCheck) performCheck() error {
	cmd := exec.Command("smartctl", "-H", c.devicePath)
	output, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("smartctl failed: %w", err)
	}

	outputStr := string(output)
	if strings.Contains(outputStr, "FAILED") {
		return fmt.Errorf("SMART health check failed")
	}
	if strings.Contains(outputStr, "PASSED") {
		return nil
	}

	return nil
}

func CheckDiskAccessibility(devicePath string) error {
	file, err := os.OpenFile(devicePath, os.O_RDONLY, 0o644)
	if err != nil {
		return fmt.Errorf("cannot open device: %w", err)
	}
	file.Close()

	cmd := exec.Command("hdparm", "-t", devicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("hdparm failed: %w", err)
	}

	if strings.Contains(string(output), "Timing buffered disk reads") {
		reader := bufio.NewReader(strings.NewReader(string(output)))
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			if strings.Contains(line, "bad sector") || strings.Contains(line, "error") {
				return fmt.Errorf("bad sector detected: %s", line)
			}
		}
	}

	return nil
}

func VerifyDiskPath(diskUUID string) (string, error) {
	output, err := exec.Command("blkid", "-U", diskUUID).Output()
	if err != nil {
		return "", fmt.Errorf("blkid -U failed: %w", err)
	}
	path := strings.TrimSpace(string(output))
	if path == "" {
		return "", fmt.Errorf("no device found for UUID %s", diskUUID)
	}
	return path, nil
}
