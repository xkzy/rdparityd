package parity

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/rand"

	"lukechampine.com/blake3"
)

type Config struct {
	DataDisks       int   `json:"data_disks"`
	ExtentCount     int   `json:"extent_count"`
	ExtentSizeBytes int   `json:"extent_size_bytes"`
	Seed            int64 `json:"seed"`

	InjectCorruption bool `json:"inject_corruption"`
	CorruptDisk      int  `json:"corrupt_disk,omitempty"`
	CorruptExtent    int  `json:"corrupt_extent,omitempty"`
}

type ExtentReport struct {
	Disk             string `json:"disk"`
	Index            int    `json:"index"`
	ExpectedChecksum string `json:"expected_checksum"`
	ActualChecksum   string `json:"actual_checksum"`
	Status           string `json:"status"`
}

type Summary struct {
	Config              Config         `json:"config"`
	ParityChecksum      string         `json:"parity_checksum"`
	MismatchesDetected  int            `json:"mismatches_detected"`
	RecoveriesPerformed int            `json:"recoveries_performed"`
	ParityVerified      bool           `json:"parity_verified"`
	AllExtentsHealthy   bool           `json:"all_extents_healthy"`
	Reports             []ExtentReport `json:"reports"`
}

func Run(cfg Config) (Summary, error) {
	cfg = cfg.withDefaults()
	if err := cfg.Validate(); err != nil {
		return Summary{}, err
	}

	rng := rand.New(rand.NewSource(cfg.Seed))
	current := make([][][]byte, cfg.DataDisks)
	expected := make([][]string, cfg.DataDisks)
	for diskIndex := 0; diskIndex < cfg.DataDisks; diskIndex++ {
		current[diskIndex] = make([][]byte, cfg.ExtentCount)
		expected[diskIndex] = make([]string, cfg.ExtentCount)
		for extentIndex := 0; extentIndex < cfg.ExtentCount; extentIndex++ {
			buf := make([]byte, cfg.ExtentSizeBytes)
			if _, err := rng.Read(buf); err != nil {
				return Summary{}, fmt.Errorf("fill random extent: %w", err)
			}
			current[diskIndex][extentIndex] = append([]byte(nil), buf...)
			expected[diskIndex][extentIndex] = digest(buf)
		}
	}

	parityExtents := make([][]byte, cfg.ExtentCount)
	for extentIndex := 0; extentIndex < cfg.ExtentCount; extentIndex++ {
		parityExtents[extentIndex] = computeParity(current, extentIndex, cfg.ExtentSizeBytes)
	}

	if cfg.InjectCorruption {
		current[cfg.CorruptDisk][cfg.CorruptExtent][0] ^= 0xFF
	}

	summary := Summary{
		Config:         cfg,
		ParityChecksum: digest(bytes.Join(parityExtents, nil)),
	}

	reports := make([]ExtentReport, 0, cfg.DataDisks*cfg.ExtentCount)
	allHealthy := true
	for diskIndex := 0; diskIndex < cfg.DataDisks; diskIndex++ {
		for extentIndex := 0; extentIndex < cfg.ExtentCount; extentIndex++ {
			expectedChecksum := expected[diskIndex][extentIndex]
			actualChecksum := digest(current[diskIndex][extentIndex])
			status := "healthy"

			if actualChecksum != expectedChecksum {
				summary.MismatchesDetected++
				status = "corrupted"

				rebuilt := reconstructExtent(current, parityExtents[extentIndex], diskIndex, extentIndex)
				if digest(rebuilt) == expectedChecksum {
					copy(current[diskIndex][extentIndex], rebuilt)
					actualChecksum = digest(current[diskIndex][extentIndex])
					status = "recovered"
					summary.RecoveriesPerformed++
				}
			}

			if actualChecksum != expectedChecksum {
				allHealthy = false
			}

			reports = append(reports, ExtentReport{
				Disk:             fmt.Sprintf("disk-%02d", diskIndex),
				Index:            extentIndex,
				ExpectedChecksum: expectedChecksum,
				ActualChecksum:   actualChecksum,
				Status:           status,
			})
		}
	}

	summary.Reports = reports
	summary.AllExtentsHealthy = allHealthy
	summary.ParityVerified = verifyParity(current, parityExtents, cfg.ExtentSizeBytes)
	return summary, nil
}

func (c Config) withDefaults() Config {
	if c.DataDisks == 0 {
		c.DataDisks = 3
	}
	if c.ExtentCount == 0 {
		c.ExtentCount = 8
	}
	if c.ExtentSizeBytes == 0 {
		c.ExtentSizeBytes = 4096
	}
	if c.Seed == 0 {
		c.Seed = 7
	}
	return c
}

func (c Config) Validate() error {
	if c.DataDisks < 2 {
		return fmt.Errorf("at least 2 data disks are required")
	}
	if c.ExtentCount < 1 {
		return fmt.Errorf("extent count must be positive")
	}
	if c.ExtentSizeBytes < 256 {
		return fmt.Errorf("extent size must be at least 256 bytes")
	}
	if c.InjectCorruption {
		if c.CorruptDisk < 0 || c.CorruptDisk >= c.DataDisks {
			return fmt.Errorf("corrupt disk index %d out of range", c.CorruptDisk)
		}
		if c.CorruptExtent < 0 || c.CorruptExtent >= c.ExtentCount {
			return fmt.Errorf("corrupt extent index %d out of range", c.CorruptExtent)
		}
	}
	return nil
}

func computeParity(current [][][]byte, extentIndex, extentSize int) []byte {
	parity := make([]byte, extentSize)
	for diskIndex := range current {
		xorInto(parity, current[diskIndex][extentIndex])
	}
	return parity
}

func reconstructExtent(current [][][]byte, parity []byte, missingDisk, extentIndex int) []byte {
	rebuilt := append([]byte(nil), parity...)
	for diskIndex := range current {
		if diskIndex == missingDisk {
			continue
		}
		xorInto(rebuilt, current[diskIndex][extentIndex])
	}
	return rebuilt
}

func verifyParity(current [][][]byte, parityExtents [][]byte, extentSize int) bool {
	for extentIndex := range parityExtents {
		recomputed := computeParity(current, extentIndex, extentSize)
		if !bytes.Equal(recomputed, parityExtents[extentIndex]) {
			return false
		}
	}
	return true
}

func xorInto(dst, src []byte) {
	for i := range dst {
		dst[i] ^= src[i]
	}
}

func digest(data []byte) string {
	h := blake3.Sum256(data)
	return hex.EncodeToString(h[:])
}
