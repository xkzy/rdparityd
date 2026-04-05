package journal

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/xkzy/rdparityd/internal/metadata"
)

// InvariantViolation describes a single broken storage invariant.
// The Code field matches the identifiers in docs/invariants.md (e.g. "E1").
type InvariantViolation struct {
	Code    string `json:"code"`    // short identifier: E1, P3, M1, J2, …
	Kind    string `json:"kind"`    // "extent", "parity", "metadata", "journal"
	Entity  string `json:"entity"`  // ID of the affected object
	Message string `json:"message"` // human-readable description
}

func (v InvariantViolation) Error() string {
	return fmt.Sprintf("[%s] %s (%s): %s", v.Code, v.Kind, v.Entity, v.Message)
}

// checkPreCommitInvariants runs all structural invariants that must hold
// before any metadata snapshot is persisted. It deliberately excludes J3
// (replay_required check) because in-flight writes at StateMetadataWritten
// legitimately set ReplayRequired=true as a crash-recovery breadcrumb.
// J3 is enforced post-recovery by Recover(), not by the write path.
//
// Invariants checked: M3, M1, M2, E2, E3, P1, P4.
func checkPreCommitInvariants(state metadata.SampleState) []InvariantViolation {
	var vs []InvariantViolation
	vs = append(vs, checkM3UniqueExtentIDs(state)...)
	vs = append(vs, checkM1FileIDsExist(state)...)
	vs = append(vs, checkM2DiskIDsExist(state)...)
	vs = append(vs, checkE2ChecksumAlg(state)...)
	vs = append(vs, checkE3PositiveLength(state)...)
	vs = append(vs, checkP1ParityGroupsReferenced(state)...)
	vs = append(vs, checkP4NoDiskAliasing(state)...)
	// J3 excluded: see function doc.
	return vs
}

// CheckStateInvariants verifies all structural invariants that do not require
// reading from disk. These are fast enough to run after every metadata mutation.
//
// Invariants checked: M1, M2, M3, E2, E3, P1, P4, J3.
func CheckStateInvariants(state metadata.SampleState) []InvariantViolation {
	var vs []InvariantViolation
	// Ordering follows docs/invariants.md §"Invariant verification ordering"
	// (structural checks before IO-bound checks).
	vs = append(vs, checkM3UniqueExtentIDs(state)...)
	vs = append(vs, checkM1FileIDsExist(state)...)
	vs = append(vs, checkM2DiskIDsExist(state)...)
	vs = append(vs, checkE2ChecksumAlg(state)...)
	vs = append(vs, checkE3PositiveLength(state)...)
	vs = append(vs, checkP1ParityGroupsReferenced(state)...)
	vs = append(vs, checkP4NoDiskAliasing(state)...)
	vs = append(vs, checkJ3NoReplayRequired(state)...)
	return vs
}

// CheckIntegrityInvariants verifies all invariants, including those that require
// reading extent and parity data from disk. rootDir is the directory that
// contains the extent data tree and the parity/ sub-directory.
//
// This function includes all CheckStateInvariants checks first, then the
// IO-bound checks E1, P2, P3 in the recommended order.
func CheckIntegrityInvariants(rootDir string, state metadata.SampleState) []InvariantViolation {
	vs := CheckStateInvariants(state)
	vs = append(vs, checkE1ExtentChecksums(rootDir, state)...)
	vs = append(vs, checkP2ParityChecksums(rootDir, state)...)
	vs = append(vs, checkP3ParityXOR(rootDir, state)...)
	return vs
}

// CheckTargetedWriteIntegrity verifies integrity only for the extents and
// parity groups touched by a just-committed write. Unlike the full-pool
// CheckIntegrityInvariants, this is safe to run on the write path even when
// unrelated extents elsewhere in the pool are already degraded and awaiting
// scrub/repair.
func CheckTargetedWriteIntegrity(rootDir string, state metadata.SampleState, extents []metadata.Extent) []InvariantViolation {
	if len(extents) == 0 {
		return nil
	}
	vs := CheckStateInvariants(state)

	affectedExtentIDs := make(map[string]struct{}, len(extents))
	affectedGroupIDs := make(map[string]struct{}, len(extents))
	for _, extent := range extents {
		affectedExtentIDs[extent.ExtentID] = struct{}{}
		if extent.ParityGroupID != "" {
			affectedGroupIDs[extent.ParityGroupID] = struct{}{}
		}
	}

	for _, v := range checkE1ExtentChecksums(rootDir, state) {
		if _, ok := affectedExtentIDs[v.Entity]; ok {
			vs = append(vs, v)
		}
	}
	for _, v := range checkP2ParityChecksums(rootDir, state) {
		if _, ok := affectedGroupIDs[v.Entity]; ok {
			vs = append(vs, v)
		}
	}
	for _, v := range checkP3ParityXOR(rootDir, state) {
		if _, ok := affectedGroupIDs[v.Entity]; ok {
			vs = append(vs, v)
		}
	}
	return vs
}

// CheckJournalInvariants verifies that journal records are internally consistent
// and follow the allowed state machine. This can be called on any []Record
// slice loaded from the journal file.
//
// Invariants checked: J1, J2.
func CheckJournalInvariants(records []Record) []InvariantViolation {
	var vs []InvariantViolation
	vs = append(vs, checkJ1RecordChecksums(records)...)
	vs = append(vs, checkJ2StateTransitions(records)...)
	return vs
}

// ─── Group E: Extent integrity ────────────────────────────────────────────────

// E1: every extent with a non-empty checksum has on-disk bytes matching it.
// For compressed extents, the on-disk size is CompressedSize and checksum
// is computed on the compressed data.
func checkE1ExtentChecksums(rootDir string, state metadata.SampleState) []InvariantViolation {
	var vs []InvariantViolation
	for _, extent := range state.Extents {
		if extent.Checksum == "" {
			continue // not yet committed; skip
		}
		path := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)
		data, err := os.ReadFile(path)
		if err != nil {
			vs = append(vs, InvariantViolation{
				Code:    "E1",
				Kind:    "extent",
				Entity:  extent.ExtentID,
				Message: fmt.Sprintf("cannot read extent file %s: %v", path, err),
			})
			continue
		}
		// For compressed extents, check compressed size and checksum
		if extent.CompressionAlg != "" && extent.CompressionAlg != metadata.CompressionNone {
			if extent.CompressedSize > 0 && int64(len(data)) != extent.CompressedSize {
				vs = append(vs, InvariantViolation{
					Code:    "E1",
					Kind:    "extent",
					Entity:  extent.ExtentID,
					Message: fmt.Sprintf("compressed length mismatch: metadata_compressed=%d disk=%d", extent.CompressedSize, len(data)),
				})
				continue
			}
		} else {
			// For uncompressed extents, check original length
			if int64(len(data)) != extent.Length {
				vs = append(vs, InvariantViolation{
					Code:    "E1",
					Kind:    "extent",
					Entity:  extent.ExtentID,
					Message: fmt.Sprintf("length mismatch: metadata=%d disk=%d", extent.Length, len(data)),
				})
				continue
			}
		}
		if got := digestBytes(data); got != extent.Checksum {
			vs = append(vs, InvariantViolation{
				Code:   "E1",
				Kind:   "extent",
				Entity: extent.ExtentID,
				Message: fmt.Sprintf("checksum mismatch: metadata=%s disk=%s",
					extent.Checksum, got),
			})
		}
	}
	return vs
}

// E2: every extent with a non-empty checksum must declare algorithm ChecksumAlgorithm ("blake3").
func checkE2ChecksumAlg(state metadata.SampleState) []InvariantViolation {
	var vs []InvariantViolation
	for _, extent := range state.Extents {
		if extent.Checksum == "" {
			continue
		}
		if extent.ChecksumAlg != ChecksumAlgorithm {
			vs = append(vs, InvariantViolation{
				Code:   "E2",
				Kind:   "extent",
				Entity: extent.ExtentID,
				Message: fmt.Sprintf("unsupported or missing checksum_alg %q (expected %q)",
					extent.ChecksumAlg, ChecksumAlgorithm),
			})
		}
	}
	return vs
}

// E3: every extent must have a positive length.
func checkE3PositiveLength(state metadata.SampleState) []InvariantViolation {
	var vs []InvariantViolation
	for _, extent := range state.Extents {
		if extent.Length <= 0 {
			vs = append(vs, InvariantViolation{
				Code:    "E3",
				Kind:    "extent",
				Entity:  extent.ExtentID,
				Message: fmt.Sprintf("non-positive length %d", extent.Length),
			})
		}
	}
	return vs
}

// ─── Group M: Metadata consistency ───────────────────────────────────────────

// M1: every extent's file_id must reference an existing file.
func checkM1FileIDsExist(state metadata.SampleState) []InvariantViolation {
	files := make(map[string]struct{}, len(state.Files))
	for _, f := range state.Files {
		files[f.FileID] = struct{}{}
	}
	var vs []InvariantViolation
	for _, extent := range state.Extents {
		if _, ok := files[extent.FileID]; !ok {
			vs = append(vs, InvariantViolation{
				Code:    "M1",
				Kind:    "metadata",
				Entity:  extent.ExtentID,
				Message: fmt.Sprintf("file_id %q not found in files list", extent.FileID),
			})
		}
	}
	return vs
}

// M2: every extent's data_disk_id must reference an existing disk.
func checkM2DiskIDsExist(state metadata.SampleState) []InvariantViolation {
	disks := make(map[string]struct{}, len(state.Disks))
	for _, d := range state.Disks {
		disks[d.DiskID] = struct{}{}
	}
	var vs []InvariantViolation
	for _, extent := range state.Extents {
		if _, ok := disks[extent.DataDiskID]; !ok {
			vs = append(vs, InvariantViolation{
				Code:    "M2",
				Kind:    "metadata",
				Entity:  extent.ExtentID,
				Message: fmt.Sprintf("data_disk_id %q not found in disks list", extent.DataDiskID),
			})
		}
	}
	return vs
}

// M3: no two extents may share the same extent_id.
func checkM3UniqueExtentIDs(state metadata.SampleState) []InvariantViolation {
	seen := make(map[string]bool, len(state.Extents))
	var vs []InvariantViolation
	for _, extent := range state.Extents {
		if seen[extent.ExtentID] {
			vs = append(vs, InvariantViolation{
				Code:    "M3",
				Kind:    "metadata",
				Entity:  extent.ExtentID,
				Message: "duplicate extent_id",
			})
		}
		seen[extent.ExtentID] = true
	}
	return vs
}

// ─── Group P: Parity consistency ─────────────────────────────────────────────

// P1: every extent's parity_group_id must exist in state.ParityGroups, and
// every member_extent_id in a parity group must exist in state.Extents.
func checkP1ParityGroupsReferenced(state metadata.SampleState) []InvariantViolation {
	groupByID := make(map[string]struct{}, len(state.ParityGroups))
	for _, g := range state.ParityGroups {
		groupByID[g.ParityGroupID] = struct{}{}
	}
	extentByID := make(map[string]struct{}, len(state.Extents))
	for _, e := range state.Extents {
		extentByID[e.ExtentID] = struct{}{}
	}

	var vs []InvariantViolation
	// Forward: extent → parity group must exist.
	for _, extent := range state.Extents {
		if extent.ParityGroupID == "" {
			continue
		}
		if _, ok := groupByID[extent.ParityGroupID]; !ok {
			vs = append(vs, InvariantViolation{
				Code:   "P1",
				Kind:   "parity",
				Entity: extent.ExtentID,
				Message: fmt.Sprintf("parity_group_id %q not found in parity_groups",
					extent.ParityGroupID),
			})
		}
	}
	// Reverse: parity group member → extent must exist.
	for _, group := range state.ParityGroups {
		for _, memberID := range group.MemberExtentIDs {
			if _, ok := extentByID[memberID]; !ok {
				vs = append(vs, InvariantViolation{
					Code:   "P1",
					Kind:   "parity",
					Entity: group.ParityGroupID,
					Message: fmt.Sprintf("member_extent_id %q not found in extents",
						memberID),
				})
			}
		}
	}
	return vs
}

// P2: every parity group's parity_checksum must match its on-disk parity file.
func checkP2ParityChecksums(rootDir string, state metadata.SampleState) []InvariantViolation {
	var vs []InvariantViolation
	for _, group := range state.ParityGroups {
		if group.ParityChecksum == "" {
			continue // not yet written
		}
		path := filepath.Join(rootDir, "parity", group.ParityGroupID+".bin")
		data, err := os.ReadFile(path)
		if err != nil {
			vs = append(vs, InvariantViolation{
				Code:    "P2",
				Kind:    "parity",
				Entity:  group.ParityGroupID,
				Message: fmt.Sprintf("cannot read parity file %s: %v", path, err),
			})
			continue
		}
		if got := digestBytes(data); got != group.ParityChecksum {
			vs = append(vs, InvariantViolation{
				Code:   "P2",
				Kind:   "parity",
				Entity: group.ParityGroupID,
				Message: fmt.Sprintf("parity checksum mismatch: metadata=%s disk=%s",
					group.ParityChecksum, got),
			})
		}
	}
	return vs
}

// P3: the parity file bytes must equal the XOR of all member extent bytes
// (each zero-padded to the length of the longest member).
func checkP3ParityXOR(rootDir string, state metadata.SampleState) []InvariantViolation {
	// Build extent-by-ID index for fast lookup.
	extentByID := make(map[string]metadata.Extent, len(state.Extents))
	for _, e := range state.Extents {
		extentByID[e.ExtentID] = e
	}

	var vs []InvariantViolation
	for _, group := range state.ParityGroups {
		if len(group.MemberExtentIDs) == 0 {
			continue
		}

		// Read all member extent files. For compressed extents, decompress first.
		memberData := make([][]byte, 0, len(group.MemberExtentIDs))
		maxLen := 0
		groupOK := true
		for _, memberID := range group.MemberExtentIDs {
			extent, ok := extentByID[memberID]
			if !ok {
				groupOK = false
				break
			}
			path := filepath.Join(rootDir, extent.PhysicalLocator.RelativePath)
			data, err := os.ReadFile(path)
			if err != nil {
				vs = append(vs, InvariantViolation{
					Code:    "P3",
					Kind:    "parity",
					Entity:  group.ParityGroupID,
					Message: fmt.Sprintf("cannot read member extent file %s: %v", path, err),
				})
				groupOK = false
				break
			}
			// Decompress if needed for XOR verification
			if extent.CompressionAlg != "" && extent.CompressionAlg != metadata.CompressionNone {
				decompressed, err := decompress(data, CompressionAlg(extent.CompressionAlg))
				if err != nil {
					vs = append(vs, InvariantViolation{
						Code:    "P3",
						Kind:    "parity",
						Entity:  group.ParityGroupID,
						Message: fmt.Sprintf("cannot decompress extent %s: %v", extent.ExtentID, err),
					})
					groupOK = false
					break
				}
				data = decompressed
			}
			memberData = append(memberData, data)
			if len(data) > maxLen {
				maxLen = len(data)
			}
		}
		if !groupOK {
			continue
		}

		// Recompute XOR.
		recomputed := make([]byte, maxLen)
		for _, data := range memberData {
			xorInto(recomputed, data)
		}

		// Read actual parity file.
		parityPath := filepath.Join(rootDir, "parity", group.ParityGroupID+".bin")
		actual, err := os.ReadFile(parityPath)
		if err != nil {
			vs = append(vs, InvariantViolation{
				Code:    "P3",
				Kind:    "parity",
				Entity:  group.ParityGroupID,
				Message: fmt.Sprintf("cannot read parity file: %v", err),
			})
			continue
		}

		if digestBytes(actual) != digestBytes(recomputed) {
			vs = append(vs, InvariantViolation{
				Code:    "P3",
				Kind:    "parity",
				Entity:  group.ParityGroupID,
				Message: "parity file does not equal XOR of member extents",
			})
		}
	}
	return vs
}

// P4: no parity group may have two members on the same data disk.
func checkP4NoDiskAliasing(state metadata.SampleState) []InvariantViolation {
	extentByID := make(map[string]metadata.Extent, len(state.Extents))
	for _, e := range state.Extents {
		extentByID[e.ExtentID] = e
	}

	var vs []InvariantViolation
	for _, group := range state.ParityGroups {
		seen := make(map[string]string) // diskID → first extentID
		for _, memberID := range group.MemberExtentIDs {
			extent, ok := extentByID[memberID]
			if !ok {
				continue // P1 will catch this
			}
			if first, conflict := seen[extent.DataDiskID]; conflict {
				vs = append(vs, InvariantViolation{
					Code:   "P4",
					Kind:   "parity",
					Entity: group.ParityGroupID,
					Message: fmt.Sprintf(
						"extents %s and %s both reside on disk %s",
						first, memberID, extent.DataDiskID),
				})
			} else {
				seen[extent.DataDiskID] = memberID
			}
		}
	}
	return vs
}

// ─── Group J: Journal consistency ────────────────────────────────────────────

// J1: every journal record must have valid payload and record checksums.
func checkJ1RecordChecksums(records []Record) []InvariantViolation {
	var vs []InvariantViolation
	for i, record := range records {
		if err := validateRecord(record); err != nil {
			vs = append(vs, InvariantViolation{
				Code:    "J1",
				Kind:    "journal",
				Entity:  fmt.Sprintf("record[%d] tx=%s state=%s", i, record.TxID, record.State),
				Message: err.Error(),
			})
		}
	}
	return vs
}

// J2: per-transaction state sequences must follow the allowed state machine.
func checkJ2StateTransitions(records []Record) []InvariantViolation {
	// Group records by TxID preserving encounter order.
	grouped := make(map[string][]Record)
	order := make([]string, 0)
	for _, r := range records {
		if _, seen := grouped[r.TxID]; !seen {
			order = append(order, r.TxID)
		}
		grouped[r.TxID] = append(grouped[r.TxID], r)
	}

	var vs []InvariantViolation
	for _, txID := range order {
		if err := ValidateRecordSequence(grouped[txID]); err != nil {
			vs = append(vs, InvariantViolation{
				Code:    "J2",
				Kind:    "journal",
				Entity:  txID,
				Message: err.Error(),
			})
		}
	}
	return vs
}

// J3: after recovery, no transaction in state.Transactions may have
// replay_required == true.
func checkJ3NoReplayRequired(state metadata.SampleState) []InvariantViolation {
	var vs []InvariantViolation
	for _, tx := range state.Transactions {
		if tx.ReplayRequired {
			vs = append(vs, InvariantViolation{
				Code:    "J3",
				Kind:    "journal",
				Entity:  tx.TxID,
				Message: "transaction has replay_required=true; crash recovery did not complete",
			})
		}
	}
	return vs
}
