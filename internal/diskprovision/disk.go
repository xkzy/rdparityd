package diskprovision

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

type ProvisionResult struct {
	Device        string `json:"device"`
	DevicePath    string `json:"device_path"`
	Partition     string `json:"partition"`
	PartitionPath string `json:"partition_path"`
	MountPoint    string `json:"mount_point"`
	SizeBytes     int64  `json:"size_bytes"`
	FSType        string `json:"fs_type"`
	UUID          string `json:"uuid,omitempty"`
}

type ProvisionOptions struct {
	Device     string
	MountPoint string
	FSType     string
	Label      string
}

func Provision(opts ProvisionOptions) (*ProvisionResult, error) {
	if opts.FSType == "" {
		opts.FSType = "ext4"
	}

	fi, err := os.Stat(opts.Device)
	if err != nil {
		return nil, fmt.Errorf("stat device: %w", err)
	}
	isBlockDevice := fi.Mode()&os.ModeDevice != 0

	deviceInfo, err := getDeviceInfo(opts.Device)
	if err != nil {
		return nil, fmt.Errorf("get device info: %w", err)
	}

	if err := createGPTPartition(opts.Device, deviceInfo.Size); err != nil {
		return nil, fmt.Errorf("create partition: %w", err)
	}

	var partitionPath string
	var loopDevice string

	if isBlockDevice {
		partitionPath = opts.Device + "1"
	} else {
		loopDev, err := setupLoopDevice(opts.Device)
		if err != nil {
			return nil, fmt.Errorf("setup loop device: %w", err)
		}
		loopDevice = loopDev
		partitionPath = loopDevice + "p1"
	}

	if err := formatPartition(partitionPath, opts.FSType, opts.Label); err != nil {
		return nil, fmt.Errorf("format partition: %w", err)
	}

	uuid, _ := getPartitionUUID(partitionPath)

	result := &ProvisionResult{
		Device:        opts.Device,
		DevicePath:    opts.Device,
		Partition:     filepath.Base(partitionPath),
		PartitionPath: partitionPath,
		MountPoint:    opts.MountPoint,
		SizeBytes:     deviceInfo.Size,
		FSType:        opts.FSType,
		UUID:          uuid,
	}

	return result, nil
}

func setupLoopDevice(imagePath string) (string, error) {
	cmd := exec.Command("losetup", "-fP", imagePath)
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("losetup -fP failed: %w", err)
	}

	output, err := exec.Command("losetup", "-j", imagePath).Output()
	if err != nil {
		return "", fmt.Errorf("losetup -j failed: %w", err)
	}

	line := strings.TrimSpace(string(output))
	if line == "" {
		return "", fmt.Errorf("loop device not found for %s", imagePath)
	}

	parts := strings.Split(line, ":")
	if len(parts) < 1 {
		return "", fmt.Errorf("unexpected losetup output: %s", line)
	}

	loopDev := strings.Split(parts[0], " ")[0]
	return loopDev, nil
}

type deviceInfo struct {
	Size       int64
	SectorSize int64
}

func getDeviceInfo(device string) (*deviceInfo, error) {
	fi, err := os.Stat(device)
	if err != nil {
		return nil, fmt.Errorf("stat device: %w", err)
	}

	var size int64
	var sectorSize int64 = 512

	if fi.Mode()&os.ModeDevice != 0 {
		output, err := exec.Command("blockdev", "--getsize64", device).Output()
		if err == nil {
			size, _ = strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
		}
	} else {
		size = fi.Size()
	}

	if size == 0 {
		return nil, fmt.Errorf("could not determine device size")
	}

	return &deviceInfo{
		Size:       size,
		SectorSize: sectorSize,
	}, nil
}

func createGPTPartition(device string, sizeBytes int64) error {
	sectorSize := int64(512)
	totalSectors := sizeBytes / sectorSize

	startSector := int64(2048)
	endSector := totalSectors - 34

	if endSector <= startSector {
		endSector = totalSectors - 1
	}

	sizeSectors := endSector - startSector

	script := fmt.Sprintf(`label: gpt
device: %s
unit: sectors
sector-size: %d

%s1 : start=%d, size=%d, type=0FC63DAF-8483-4772-8E79-3D69D8477DE4
`, device, sectorSize, device, startSector, sizeSectors)

	cmd := exec.Command("sfdisk", device)
	cmd.Stdin = strings.NewReader(script)
	cmd.Stderr = &bytes.Buffer{}

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("sfdisk failed: %w (stderr: %s)", err, cmd.Stderr.(*bytes.Buffer).String())
	}

	return nil
}

func formatPartition(partitionPath, fsType, label string) error {
	var cmd *exec.Cmd

	switch fsType {
	case "ext4":
		args := []string{"-F"}
		if label != "" {
			args = append(args, "-L", label)
		}
		args = append(args, partitionPath)
		cmd = exec.Command("mkfs.ext4", args...)
	case "xfs":
		args := []string{"-f"}
		if label != "" {
			args = append(args, "-L", label)
		}
		args = append(args, partitionPath)
		cmd = exec.Command("mkfs.xfs", args...)
	default:
		return fmt.Errorf("unsupported filesystem type: %s", fsType)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("mkfs.%s failed: %w (output: %s)", fsType, err, string(output))
	}

	return nil
}

func getPartitionUUID(partitionPath string) (string, error) {
	output, err := exec.Command("blkid", "-s", "UUID", "-o", "value", partitionPath).Output()
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func getMountPoint(devicePath string) (string, error) {
	output, err := exec.Command("findmnt", "-n", "-o", "TARGET", devicePath).Output()
	if err != nil {
		if strings.Contains(err.Error(), "exit status 1") {
			return "", nil
		}
		return "", err
	}
	return strings.TrimSpace(string(output)), nil
}

func isMounted(devicePath string) (bool, error) {
	mnt, err := getMountPoint(devicePath)
	if err != nil {
		return false, err
	}
	return mnt != "", nil
}

func isPartitioned(device string) (bool, error) {
	output, err := exec.Command("sfdisk", "--dump", device).Output()
	if err != nil {
		if strings.Contains(string(output), "does not contain a recognized partition table") {
			return false, nil
		}
		return false, err
	}

	return !strings.Contains(string(output), "does not contain a recognized partition table"), nil
}

func getDeviceSize(device string) (int64, error) {
	var size int64

	output, err := exec.Command("blockdev", "--getsize64", device).Output()
	if err == nil {
		size, _ = strconv.ParseInt(strings.TrimSpace(string(output)), 10, 64)
	}

	if size == 0 {
		fi, err := os.Stat(device)
		if err != nil {
			return 0, err
		}
		size = fi.Size()
	}

	return size, nil
}

func buildMountCommand(partitionPath, mountPoint string) string {
	return fmt.Sprintf("pkexec mount -t ext4 %s %s", partitionPath, mountPoint)
}

func BuildMountInstructions(result *ProvisionResult) string {
	var buf bytes.Buffer

	buf.WriteString("# Disk Provisioned Successfully!\n\n")
	buf.WriteString(fmt.Sprintf("Device: %s\n", result.Device))
	buf.WriteString(fmt.Sprintf("Partition: %s\n", result.PartitionPath))
	buf.WriteString(fmt.Sprintf("Size: %d bytes (%.2f GB)\n", result.SizeBytes, float64(result.SizeBytes)/1e9))
	buf.WriteString(fmt.Sprintf("Filesystem: %s\n\n", result.FSType))

	if result.UUID != "" {
		buf.WriteString(fmt.Sprintf("UUID: %s\n\n", result.UUID))
	}

	buf.WriteString("# To mount the partition, run:\n\n")
	buf.WriteString(fmt.Sprintf("  # Create mount point and mount:\n"))
	buf.WriteString(fmt.Sprintf("  sudo mkdir -p %s\n", result.MountPoint))
	buf.WriteString(fmt.Sprintf("  sudo mount %s %s\n\n", result.PartitionPath, result.MountPoint))

	buf.WriteString(fmt.Sprintf("  # Or use pkexec (will prompt for password):\n"))
	buf.WriteString(fmt.Sprintf("  pkexec mount -t %s %s %s\n\n", result.FSType, result.PartitionPath, result.MountPoint))

	buf.WriteString(fmt.Sprintf("# To auto-mount on boot, add to /etc/fstab:\n"))
	if result.UUID != "" {
		buf.WriteString(fmt.Sprintf("  UUID=%s %s %s defaults 0 2\n", result.UUID, result.MountPoint, result.FSType))
	} else {
		buf.WriteString(fmt.Sprintf("  %s %s %s defaults 0 2\n", result.PartitionPath, result.MountPoint, result.FSType))
	}

	return buf.String()
}

func ValidateDevicePath(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("device not found: %s", path)
	}

	isBlock := fi.Mode()&os.ModeDevice != 0
	isRegular := fi.Mode().IsRegular()

	if !isBlock && !isRegular {
		return fmt.Errorf("not a valid file or block device: %s", path)
	}

	return nil
}
