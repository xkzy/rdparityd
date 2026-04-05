package metadata

import "time"

// ChecksumAlgorithm is the hash function used for all extent and parity
// checksums. It is defined here (in the metadata package) so that both the
// journal layer and the allocator can reference it without creating a circular
// import.
const ChecksumAlgorithm = "blake3"

type Pool struct {
	PoolID          string    `json:"pool_id"`
	Name            string    `json:"name"`
	Version         string    `json:"version"`
	ExtentSizeBytes int64     `json:"extent_size_bytes"`
	ParityMode      string    `json:"parity_mode"`
	CreatedAt       time.Time `json:"created_at"`
}

type DiskRole string

const (
	DiskRoleData     DiskRole = "data"
	DiskRoleParity   DiskRole = "parity"
	DiskRoleMetadata DiskRole = "metadata"
)

type Disk struct {
	DiskID         string   `json:"disk_id"`
	UUID           string   `json:"uuid"`
	Role           DiskRole `json:"role"`
	FilesystemType string   `json:"filesystem_type"`
	Mountpoint     string   `json:"mountpoint"`
	CapacityBytes  int64    `json:"capacity_bytes"`
	FreeBytes      int64    `json:"free_bytes"`
	HealthStatus   string   `json:"health_status"`
	Generation     int64    `json:"generation"`
}

type FileState string

const (
	FileStateAllocated FileState = "allocated"
)

type FileRecord struct {
	FileID    string    `json:"file_id"`
	Path      string    `json:"path"`
	SizeBytes int64     `json:"size_bytes"`
	MTime     time.Time `json:"mtime"`
	CTime     time.Time `json:"ctime"`
	Policy    string    `json:"policy"`
	State     FileState `json:"state"`
}

type Locator struct {
	RelativePath string `json:"relative_path"`
	OffsetBytes  int64  `json:"offset_bytes"`
	LengthBytes  int64  `json:"length_bytes"`
}

type ExtentState string

const (
	ExtentStateAllocated ExtentState = "allocated"
)

type Extent struct {
	ExtentID        string      `json:"extent_id"`
	FileID          string      `json:"file_id"`
	LogicalOffset   int64       `json:"logical_offset"`
	Length          int64       `json:"length"`
	DataDiskID      string      `json:"data_disk_id"`
	PhysicalLocator Locator     `json:"physical_locator"`
	Checksum        string      `json:"checksum"`
	ChecksumAlg     string      `json:"checksum_alg"`
	Generation      int64       `json:"generation"`
	ParityGroupID   string      `json:"parity_group_id"`
	State           ExtentState `json:"state"`
}

type ParityGroup struct {
	ParityGroupID   string   `json:"parity_group_id"`
	ParityDiskID    string   `json:"parity_disk_id"`
	MemberExtentIDs []string `json:"member_extent_ids"`
	ParityChecksum  string   `json:"parity_checksum"`
	Generation      int64    `json:"generation"`
}

type Transaction struct {
	TxID              string     `json:"tx_id"`
	State             string     `json:"state"`
	StartedAt         time.Time  `json:"started_at"`
	CommittedAt       *time.Time `json:"committed_at,omitempty"`
	AffectedExtentIDs []string   `json:"affected_extent_ids"`
	OldGeneration     int64      `json:"old_generation"`
	NewGeneration     int64      `json:"new_generation"`
	ReplayRequired    bool       `json:"replay_required"`
}

type ScrubRun struct {
	RunID                string    `json:"run_id"`
	StartedAt            time.Time `json:"started_at"`
	CompletedAt          time.Time `json:"completed_at"`
	Repair               bool      `json:"repair"`
	Healthy              bool      `json:"healthy"`
	FilesChecked         int       `json:"files_checked"`
	ExtentsChecked       int       `json:"extents_checked"`
	ParityGroupsChecked  int       `json:"parity_groups_checked"`
	HealedCount          int       `json:"healed_count"`
	FailedCount          int       `json:"failed_count"`
	IssueCount           int       `json:"issue_count"`
	HealedExtentIDs      []string  `json:"healed_extent_ids,omitempty"`
	HealedParityGroupIDs []string  `json:"healed_parity_group_ids,omitempty"`
}

type SampleState struct {
	Pool         Pool          `json:"pool"`
	Disks        []Disk        `json:"disks"`
	Files        []FileRecord  `json:"files"`
	Extents      []Extent      `json:"extents"`
	ParityGroups []ParityGroup `json:"parity_groups"`
	Transactions []Transaction `json:"transactions"`
	ScrubHistory []ScrubRun    `json:"scrub_history,omitempty"`
}

func PrototypeState(name string) SampleState {
	now := time.Now().UTC()
	committedAt := now

	return SampleState{
		Pool: Pool{
			PoolID:          "pool-demo",
			Name:            name,
			Version:         "v1alpha1",
			ExtentSizeBytes: 1 << 20,
			ParityMode:      "single",
			CreatedAt:       now,
		},
		Disks: []Disk{
			{
				DiskID:         "disk-01",
				UUID:           "11111111-1111-1111-1111-111111111111",
				Role:           DiskRoleData,
				FilesystemType: "xfs",
				Mountpoint:     "/mnt/data01",
				CapacityBytes:  4 << 40,
				FreeBytes:      3 << 40,
				HealthStatus:   "online",
				Generation:     1,
			},
			{
				DiskID:         "disk-02",
				UUID:           "22222222-2222-2222-2222-222222222222",
				Role:           DiskRoleData,
				FilesystemType: "xfs",
				Mountpoint:     "/mnt/data02",
				CapacityBytes:  8 << 40,
				FreeBytes:      7 << 40,
				HealthStatus:   "online",
				Generation:     1,
			},
			{
				DiskID:         "disk-parity",
				UUID:           "33333333-3333-3333-3333-333333333333",
				Role:           DiskRoleParity,
				FilesystemType: "xfs",
				Mountpoint:     "/mnt/parity",
				CapacityBytes:  8 << 40,
				FreeBytes:      7 << 40,
				HealthStatus:   "online",
				Generation:     1,
			},
			{
				DiskID:         "disk-meta",
				UUID:           "44444444-4444-4444-4444-444444444444",
				Role:           DiskRoleMetadata,
				FilesystemType: "ext4",
				Mountpoint:     "/var/lib/rtparityd",
				CapacityBytes:  64 << 30,
				FreeBytes:      48 << 30,
				HealthStatus:   "online",
				Generation:     1,
			},
		},
		Files:        []FileRecord{},
		Extents:      []Extent{},
		ParityGroups: []ParityGroup{},
		Transactions: []Transaction{
			{
				TxID:              "tx-bootstrap",
				State:             "committed",
				StartedAt:         now,
				CommittedAt:       &committedAt,
				AffectedExtentIDs: []string{"extent-000001"},
				OldGeneration:     0,
				NewGeneration:     1,
				ReplayRequired:    false,
			},
		},
		ScrubHistory: []ScrubRun{},
	}
}
