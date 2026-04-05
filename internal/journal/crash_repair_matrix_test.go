package journal

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xkzy/rdparityd/internal/metadata"
)

func firstExtentPath(t *testing.T, metaPath string, ext metadata.Extent) string {
	t.Helper()
	return filepath.Join(filepath.Dir(metaPath), ext.PhysicalLocator.RelativePath)
}

func TestCrashRepairExtentScrubRecoveryMatrix(t *testing.T) {
	for _, crashState := range []State{StatePrepared, StateDataWritten} {
		t.Run(string(crashState), func(t *testing.T) {
			dir := t.TempDir()
			metaPath := filepath.Join(dir, "metadata.bin")
			journalPath := filepath.Join(dir, "journal.log")
			coord := NewCoordinator(metaPath, journalPath)

			writeResult, err := coord.WriteFile(WriteRequest{
				PoolName:       "scrub-repair-crash",
				LogicalPath:    "/test/extent.bin",
				AllowSynthetic: true,
				SizeBytes:      4096,
			})
			if err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			if len(writeResult.Extents) == 0 {
				t.Fatal("expected extents")
			}

			extentPath := firstExtentPath(t, metaPath, writeResult.Extents[0])
			data, err := os.ReadFile(extentPath)
			if err != nil {
				t.Fatalf("read extent: %v", err)
			}
			data[0] ^= 0xFF
			if err := os.WriteFile(extentPath, data, 0o600); err != nil {
				t.Fatalf("corrupt extent: %v", err)
			}

			result, err := coord.scrubWithRepairFailAfter(true, crashState)
			if err != nil {
				t.Fatalf("scrubWithRepairFailAfter returned unexpected error: %v", err)
			}
			if result.FailedCount == 0 {
				t.Fatalf("expected failed scrub repair after injected crash at %s", crashState)
			}

			recovery, err := NewCoordinator(metaPath, journalPath).RecoverWithState(metadata.PrototypeState("scrub-repair-crash"))
			if err != nil {
				t.Fatalf("RecoverWithState: %v", err)
			}
			if len(recovery.RecoveredTxIDs) == 0 {
				t.Fatalf("expected recovered repair transaction after %s crash", crashState)
			}

			summary, err := NewStore(journalPath).Replay()
			if err != nil {
				t.Fatalf("Replay: %v", err)
			}
			if summary.RequiresReplay {
				t.Fatalf("journal still requires replay after recovery: %+v", summary)
			}

			finalResult, err := NewCoordinator(metaPath, journalPath).Scrub(false)
			if err != nil {
				t.Fatalf("Scrub(false): %v", err)
			}
			if !finalResult.Healthy {
				t.Fatalf("pool not healthy after extent repair recovery: %+v", finalResult)
			}
		})
	}
}

func TestCrashRepairParityScrubRecoveryMatrix(t *testing.T) {
	for _, crashState := range []State{StatePrepared, StateDataWritten} {
		t.Run(string(crashState), func(t *testing.T) {
			dir := t.TempDir()
			metaPath := filepath.Join(dir, "metadata.bin")
			journalPath := filepath.Join(dir, "journal.log")
			coord := NewCoordinator(metaPath, journalPath)

			writeResult, err := coord.WriteFile(WriteRequest{
				PoolName:       "parity-repair-crash",
				LogicalPath:    "/test/parity.bin",
				AllowSynthetic: true,
				SizeBytes:      4096,
			})
			if err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			if len(writeResult.Extents) == 0 {
				t.Fatal("expected extents")
			}
			groupID := writeResult.Extents[0].ParityGroupID
			parityPath := filepath.Join(dir, "parity", groupID+".bin")
			data, err := os.ReadFile(parityPath)
			if err != nil {
				t.Fatalf("read parity: %v", err)
			}
			data[0] ^= 0xAA
			if err := os.WriteFile(parityPath, data, 0o600); err != nil {
				t.Fatalf("corrupt parity: %v", err)
			}

			result, err := coord.scrubWithRepairFailAfter(true, crashState)
			if err != nil {
				t.Fatalf("scrubWithRepairFailAfter returned unexpected error: %v", err)
			}
			if result.FailedCount == 0 {
				t.Fatalf("expected failed parity scrub repair after injected crash at %s", crashState)
			}

			recovery, err := NewCoordinator(metaPath, journalPath).RecoverWithState(metadata.PrototypeState("parity-repair-crash"))
			if err != nil {
				t.Fatalf("RecoverWithState: %v", err)
			}
			if len(recovery.RecoveredTxIDs) == 0 {
				t.Fatalf("expected recovered parity repair transaction after %s crash", crashState)
			}

			finalResult, err := NewCoordinator(metaPath, journalPath).Scrub(false)
			if err != nil {
				t.Fatalf("Scrub(false): %v", err)
			}
			if !finalResult.Healthy {
				t.Fatalf("pool not healthy after parity repair recovery: %+v", finalResult)
			}
		})
	}
}

func TestCrashRepairRebuildRecoveryMatrix(t *testing.T) {
	for _, crashState := range []State{StatePrepared, StateDataWritten} {
		t.Run(string(crashState), func(t *testing.T) {
			dir := t.TempDir()
			metaPath := filepath.Join(dir, "metadata.bin")
			journalPath := filepath.Join(dir, "journal.log")
			coord := NewCoordinator(metaPath, journalPath)

			writeResult, err := coord.WriteFile(WriteRequest{
				PoolName:       "rebuild-repair-crash",
				LogicalPath:    "/test/rebuild.bin",
				AllowSynthetic: true,
				SizeBytes:      4096,
			})
			if err != nil {
				t.Fatalf("WriteFile: %v", err)
			}
			target := writeResult.Extents[0]
			extentPath := firstExtentPath(t, metaPath, target)
			if err := os.Remove(extentPath); err != nil {
				t.Fatalf("remove extent: %v", err)
			}

			result, err := coord.rebuildDataDiskWithRepairFailAfter(target.DataDiskID, crashState)
			if err != nil {
				t.Fatalf("rebuildDataDiskWithRepairFailAfter returned error: %v", err)
			}
			if result.FailedCount == 0 {
				t.Fatalf("expected failed rebuild verification after injected crash at %s", crashState)
			}

			recovery, err := NewCoordinator(metaPath, journalPath).RecoverWithState(metadata.PrototypeState("rebuild-repair-crash"))
			if err != nil {
				t.Fatalf("RecoverWithState: %v", err)
			}
			if len(recovery.RecoveredTxIDs) == 0 {
				t.Fatalf("expected recovered rebuild repair transaction after %s crash", crashState)
			}

			final, err := NewCoordinator(metaPath, journalPath).RebuildDataDisk(target.DataDiskID)
			if err != nil {
				t.Fatalf("final RebuildDataDisk: %v", err)
			}
			if !final.Healthy {
				t.Fatalf("rebuild still unhealthy after recovery: %+v", final)
			}

			readResult, err := NewCoordinator(metaPath, journalPath).ReadFile("/test/rebuild.bin")
			if err != nil {
				t.Fatalf("ReadFile: %v", err)
			}
			if !readResult.Verified {
				t.Fatal("rebuilt file was not verified after recovery")
			}
		})
	}
}
