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

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/xkzy/rdparityd/internal/fusefs"
	"github.com/xkzy/rdparityd/internal/journal"
	"github.com/xkzy/rdparityd/internal/metadata"
	"github.com/xkzy/rdparityd/internal/parity"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	var err error
	switch os.Args[1] {
	case "simulate":
		err = runSimulate(os.Args[2:])
	case "sample-pool":
		err = runSamplePool()
	case "journal-demo":
		err = runJournalDemo()
	case "allocate-demo":
		err = runAllocateDemo(os.Args[2:])
	case "write-demo":
		err = runWriteDemo(os.Args[2:])
	case "read-demo":
		err = runReadDemo(os.Args[2:])
	case "scrub-demo":
		err = runScrubDemo(os.Args[2:])
	case "scrub-history":
		err = runScrubHistory(os.Args[2:])
	case "rebuild-demo":
		err = runRebuildDemo(os.Args[2:])
	case "rebuild-all-demo":
		err = runRebuildAllDemo(os.Args[2:])
	case "check-invariants":
		err = runCheckInvariants(os.Args[2:])
	case "mount":
		err = runMount(os.Args[2:])
	case "trim":
		err = runTrim(os.Args[2:])
	case "defrag":
		err = runDefrag(os.Args[2:])
	case "snapshot":
		err = runSnapshot(os.Args[2:])
	case "enable-sleep":
		err = runEnableSleep(os.Args[2:])
	case "disable-sleep":
		err = runDisableSleep(os.Args[2:])
	case "wake-disk":
		err = runWakeDisk(os.Args[2:])
	case "sleep-disk":
		err = runSleepDisk(os.Args[2:])
	case "sleep-status":
		err = runSleepStatus(os.Args[2:])
	case "protection-status":
		err = runProtectionStatus(os.Args[2:])
	case "set-protection":
		err = runSetProtection(os.Args[2:])
	default:
		usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `rtpctl manages the rtparityd prototype.

Usage:
  rtpctl simulate [flags]
  rtpctl sample-pool
  rtpctl journal-demo
  rtpctl allocate-demo [flags]
  rtpctl write-demo [flags]
  rtpctl read-demo [flags]
  rtpctl scrub-demo [flags]
  rtpctl scrub-history [flags]
  rtpctl rebuild-demo [flags]
  rtpctl rebuild-all-demo [flags]
  rtpctl check-invariants [flags]
  rtpctl mount [flags]
  rtpctl trim [flags]
  rtpctl defrag [flags]
  rtpctl snapshot [flags]
  rtpctl enable-sleep [flags]
  rtpctl disable-sleep [flags]
  rtpctl wake-disk [flags]
  rtpctl sleep-disk [flags]
  rtpctl sleep-status [flags]
  rtpctl protection-status [flags]
  rtpctl set-protection [flags]

Unsupported in the current production feature set:
  trim, defrag, snapshot, enable-sleep, disable-sleep, wake-disk, sleep-disk, sleep-status, set-protection
`)
}

func runSimulate(args []string) error {
	fs := flag.NewFlagSet("simulate", flag.ContinueOnError)
	dataDisks := fs.Int("disks", 3, "number of data disks")
	extents := fs.Int("extents", 8, "number of extents per disk")
	extentBytes := fs.Int("extent-bytes", 4096, "bytes per extent")
	seed := fs.Int64("seed", 7, "deterministic random seed")
	corruptDisk := fs.Int("corrupt-disk", -1, "0-based disk index to corrupt; -1 disables corruption")
	corruptExtent := fs.Int("corrupt-extent", 0, "0-based extent index to corrupt")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg := parity.Config{
		DataDisks:       *dataDisks,
		ExtentCount:     *extents,
		ExtentSizeBytes: *extentBytes,
		Seed:            *seed,
	}
	if *corruptDisk >= 0 {
		cfg.InjectCorruption = true
		cfg.CorruptDisk = *corruptDisk
		cfg.CorruptExtent = *corruptExtent
	}

	summary, err := parity.Run(cfg)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(summary)
}

func runSamplePool() error {
	state := metadata.PrototypeState("demo")
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(state)
}

func runJournalDemo() error {
	path := filepath.Join(os.TempDir(), fmt.Sprintf("rtparityd-journal-%d.log", time.Now().UnixNano()))
	store := journal.NewStore(path)

	entries := []journal.Record{
		{TxID: "tx-complete", State: journal.StatePrepared},
		{TxID: "tx-complete", State: journal.StateDataWritten},
		{TxID: "tx-complete", State: journal.StateParityWritten},
		{TxID: "tx-complete", State: journal.StateMetadataWritten},
		{TxID: "tx-complete", State: journal.StateCommitted},
		{TxID: "tx-replay", State: journal.StatePrepared},
		{TxID: "tx-replay", State: journal.StateDataWritten},
	}
	for _, entry := range entries {
		if _, err := store.Append(entry); err != nil {
			return err
		}
	}

	summary, err := store.Replay()
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{
		"journal_path": path,
		"summary":      summary,
	})
}

func runAllocateDemo(args []string) error {
	fs := flag.NewFlagSet("allocate-demo", flag.ContinueOnError)
	poolName := fs.String("pool-name", "demo", "prototype pool name")
	metadataPath := fs.String("metadata-path", filepath.Join(os.TempDir(), fmt.Sprintf("rtparityd-metadata-%d.json", time.Now().UnixNano())), "metadata snapshot path")
	filePath := fs.String("path", "/shares/demo/example.bin", "logical file path to allocate")
	sizeBytes := fs.Int64("size-bytes", 2*(1<<20)+123, "file size in bytes")
	if err := fs.Parse(args); err != nil {
		return err
	}

	store := metadata.NewStore(*metadataPath)
	state, err := store.LoadOrCreate(metadata.PrototypeState(*poolName))
	if err != nil {
		return err
	}

	allocator := metadata.NewAllocator(&state)
	file, extents, err := allocator.AllocateFile(*filePath, *sizeBytes)
	if err != nil {
		return err
	}
	snapshot, err := store.Save(state)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{
		"metadata_path":  *metadataPath,
		"state_checksum": snapshot.StateChecksum,
		"file":           file,
		"extents":        extents,
		"managed_files":  len(state.Files),
		"total_extents":  len(state.Extents),
	})
}

func runWriteDemo(args []string) error {
	fs := flag.NewFlagSet("write-demo", flag.ContinueOnError)
	poolName := fs.String("pool-name", "demo", "prototype pool name")
	metadataPath := fs.String("metadata-path", filepath.Join(os.TempDir(), fmt.Sprintf("rtparityd-metadata-%d.json", time.Now().UnixNano())), "metadata snapshot path")
	journalPath := fs.String("journal-path", filepath.Join(os.TempDir(), fmt.Sprintf("rtparityd-journal-%d.log", time.Now().UnixNano())), "journal log path")
	filePath := fs.String("path", "/shares/demo/write.bin", "logical file path to write")
	inputFile := fs.String("input-file", "", "path to a real file whose bytes will be written (required for production)")
	sizeBytes := fs.Int64("size-bytes", 2*(1<<20)+123, "file size in bytes (requires -synthetic flag)")
	synthetic := fs.Bool("synthetic", false, "allow synthetic deterministic data for testing (not for production)")
	failAfter := fs.String("fail-after", "", "optional stop stage: prepared|data-written|parity-written|metadata-written")
	if err := fs.Parse(args); err != nil {
		return err
	}

	if *inputFile == "" && !*synthetic {
		return fmt.Errorf("production write requires -input-file; use -synthetic only for testing")
	}

	req := journal.WriteRequest{
		PoolName:       *poolName,
		LogicalPath:    *filePath,
		AllowSynthetic: *synthetic,
		SizeBytes:      *sizeBytes,
		FailAfter:      journal.State(*failAfter),
	}

	if *inputFile != "" {
		data, err := os.ReadFile(*inputFile)
		if err != nil {
			return fmt.Errorf("read input file: %w", err)
		}
		req.Payload = data
	}

	coordinator := journal.NewCoordinator(*metadataPath, *journalPath)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := coordinator.WriteFile(ctx, req)
	if err != nil {
		return err
	}

	summary, err := journal.NewStore(*journalPath).Replay()
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{
		"result":  result,
		"summary": summary,
	})
}

func runReadDemo(args []string) error {
	fs := flag.NewFlagSet("read-demo", flag.ContinueOnError)
	metadataPath := fs.String("metadata-path", filepath.Join(os.TempDir(), "rtparityd-metadata.bin"), "binary metadata snapshot path")
	journalPath := fs.String("journal-path", filepath.Join(os.TempDir(), "rtparityd-journal.bin"), "journal log path")
	filePath := fs.String("path", "/shares/demo/write.bin", "logical file path to read and verify")
	if err := fs.Parse(args); err != nil {
		return err
	}

	result, err := journal.NewCoordinator(*metadataPath, *journalPath).ReadFile(*filePath)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func runScrubDemo(args []string) error {
	fs := flag.NewFlagSet("scrub-demo", flag.ContinueOnError)
	metadataPath := fs.String("metadata-path", filepath.Join(os.TempDir(), "rtparityd-metadata.bin"), "binary metadata snapshot path")
	journalPath := fs.String("journal-path", filepath.Join(os.TempDir(), "rtparityd-journal.bin"), "journal log path")
	repair := fs.Bool("repair", true, "repair corrupted extents or parity when possible")
	if err := fs.Parse(args); err != nil {
		return err
	}

	result, err := journal.NewCoordinator(*metadataPath, *journalPath).Scrub(*repair)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func runScrubHistory(args []string) error {
	fs := flag.NewFlagSet("scrub-history", flag.ContinueOnError)
	metadataPath := fs.String("metadata-path", filepath.Join(os.TempDir(), "rtparityd-metadata.bin"), "binary metadata snapshot path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	state, err := metadata.NewStore(*metadataPath).LoadOrCreate(metadata.PrototypeState("demo"))
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{
		"metadata_path": *metadataPath,
		"count":         len(state.ScrubHistory),
		"history":       state.ScrubHistory,
	})
}

func runRebuildDemo(args []string) error {
	fs := flag.NewFlagSet("rebuild-demo", flag.ContinueOnError)
	metadataPath := fs.String("metadata-path", filepath.Join(os.TempDir(), "rtparityd-metadata.bin"), "binary metadata snapshot path")
	journalPath := fs.String("journal-path", filepath.Join(os.TempDir(), "rtparityd-journal.bin"), "journal log path")
	diskID := fs.String("disk", "disk-01", "data disk id to rebuild from parity")
	if err := fs.Parse(args); err != nil {
		return err
	}

	result, err := journal.NewCoordinator(*metadataPath, *journalPath).RebuildDataDisk(*diskID)
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func runRebuildAllDemo(args []string) error {
	fs := flag.NewFlagSet("rebuild-all-demo", flag.ContinueOnError)
	metadataPath := fs.String("metadata-path", filepath.Join(os.TempDir(), "rtparityd-metadata.bin"), "binary metadata snapshot path")
	journalPath := fs.String("journal-path", filepath.Join(os.TempDir(), "rtparityd-journal.bin"), "journal log path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	result, err := journal.NewCoordinator(*metadataPath, *journalPath).RebuildAllDataDisks()
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func runCheckInvariants(args []string) error {
	fs := flag.NewFlagSet("check-invariants", flag.ContinueOnError)
	metadataPath := fs.String("metadata-path", filepath.Join(os.TempDir(), "rtparityd-metadata.bin"), "binary metadata snapshot path")
	journalPath := fs.String("journal-path", filepath.Join(os.TempDir(), "rtparityd-journal.bin"), "journal log path")
	full := fs.Bool("full", false, "also verify on-disk extent and parity data (requires IO)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	state, err := metadata.NewStore(*metadataPath).Load()
	if err != nil {
		return fmt.Errorf("load metadata: %w", err)
	}

	var stateViolations []journal.InvariantViolation
	if *full {
		stateViolations = journal.CheckIntegrityInvariants(filepath.Dir(*metadataPath), state)
	} else {
		stateViolations = journal.CheckStateInvariants(state)
	}

	records, err := journal.NewStore(*journalPath).Load()
	if err != nil {
		return fmt.Errorf("load journal: %w", err)
	}
	journalViolations := journal.CheckJournalInvariants(records)

	all := append(stateViolations, journalViolations...)
	healthy := len(all) == 0

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(map[string]any{
		"metadata_path":   *metadataPath,
		"journal_path":    *journalPath,
		"full_integrity":  *full,
		"healthy":         healthy,
		"violation_count": len(all),
		"violations":      all,
	})
}

func runMount(args []string) error {
	fset := flag.NewFlagSet("mount", flag.ContinueOnError)
	metadataPath := fset.String("metadata-path", "/tmp/rtparityd/metadata.bin", "binary metadata snapshot path")
	journalPath := fset.String("journal-path", "/tmp/rtparityd/journal.bin", "journal path")
	poolName := fset.String("pool-name", "demo", "pool name used for new writes")
	debug := fset.Bool("debug", false, "enable verbose FUSE operation logging")
	mountpoint := fset.String("mountpoint", "", "directory to mount the pool filesystem at (required)")
	if err := fset.Parse(args); err != nil {
		return err
	}
	if *mountpoint == "" {
		return fmt.Errorf("flag -mountpoint is required")
	}

	coord := journal.NewCoordinator(*metadataPath, *journalPath)

	// Run startup recovery so any interrupted transactions from a previous
	// session are resolved before we start serving the filesystem.
	recovery, err := coord.Recover()
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if len(recovery.RecoveredTxIDs) > 0 {
		log.Printf("startup recovery: rolled forward %d transaction(s): %v",
			len(recovery.RecoveredTxIDs), recovery.RecoveredTxIDs)
	}
	if len(recovery.AbortedTxIDs) > 0 {
		log.Printf("startup recovery: aborted %d transaction(s): %v",
			len(recovery.AbortedTxIDs), recovery.AbortedTxIDs)
	}

	srv, err := fusefs.Mount(*mountpoint, coord, fusefs.Options{
		PoolName: *poolName,
		Debug:    *debug,
	})
	if err != nil {
		return fmt.Errorf("mount %s: %w", *mountpoint, err)
	}
	if err := srv.WaitMount(); err != nil {
		return fmt.Errorf("wait mount: %w", err)
	}

	log.Printf("rdparityd pool mounted at %s (pool=%s, metadata=%s)",
		*mountpoint, *poolName, *metadataPath)
	log.Printf("press Ctrl-C or run 'fusermount -u %s' to unmount", *mountpoint)

	// Wait for SIGINT/SIGTERM then unmount cleanly.
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch

	log.Printf("unmounting %s …", *mountpoint)
	if err := srv.Unmount(); err != nil {
		return fmt.Errorf("unmount: %w", err)
	}
	log.Printf("unmounted successfully")
	return nil
}

func runTrim(args []string) error {
	fset := flag.NewFlagSet("trim", flag.ContinueOnError)
	metadataPath := fset.String("metadata-path", "/tmp/rtparityd/metadata.bin", "binary metadata snapshot path")
	journalPath := fset.String("journal-path", "/tmp/rtparityd/journal.bin", "journal path")
	if err := fset.Parse(args); err != nil {
		return err
	}

	coord := journal.NewCoordinator(*metadataPath, *journalPath)

	recovery, err := coord.Recover()
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if len(recovery.RecoveredTxIDs) > 0 {
		log.Printf("startup recovery: rolled forward %d transaction(s)", len(recovery.RecoveredTxIDs))
	}

	result, err := coord.Trim()
	if err != nil {
		return fmt.Errorf("trim failed: %w", err)
	}

	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

func runDefrag(args []string) error {
	fset := flag.NewFlagSet("defrag", flag.ContinueOnError)
	metadataPath := fset.String("metadata-path", "/tmp/rtparityd/metadata.bin", "binary metadata snapshot path")
	journalPath := fset.String("journal-path", "/tmp/rtparityd/journal.bin", "journal path")
	if err := fset.Parse(args); err != nil {
		return err
	}

	coord := journal.NewCoordinator(*metadataPath, *journalPath)

	recovery, err := coord.Recover()
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if len(recovery.RecoveredTxIDs) > 0 {
		log.Printf("startup recovery: rolled forward %d transaction(s)", len(recovery.RecoveredTxIDs))
	}

	result, err := coord.Defrag()
	if err != nil {
		return fmt.Errorf("defrag failed: %w", err)
	}

	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

func runSnapshot(args []string) error {
	fset := flag.NewFlagSet("snapshot", flag.ContinueOnError)
	metadataPath := fset.String("metadata-path", "/tmp/rtparityd/metadata.bin", "binary metadata snapshot path")
	journalPath := fset.String("journal-path", "/tmp/rtparityd/journal.bin", "journal path")
	name := fset.String("name", "", "snapshot name (optional, defaults to timestamp)")
	if err := fset.Parse(args); err != nil {
		return err
	}

	coord := journal.NewCoordinator(*metadataPath, *journalPath)

	recovery, err := coord.Recover()
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if len(recovery.RecoveredTxIDs) > 0 {
		log.Printf("startup recovery: rolled forward %d transaction(s)", len(recovery.RecoveredTxIDs))
	}

	result, err := coord.Snapshot(*name)
	if err != nil {
		return fmt.Errorf("snapshot failed: %w", err)
	}

	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

func runEnableSleep(args []string) error {
	fset := flag.NewFlagSet("enable-sleep", flag.ContinueOnError)
	metadataPath := fset.String("metadata-path", "/tmp/rtparityd/metadata.bin", "binary metadata snapshot path")
	journalPath := fset.String("journal-path", "/tmp/rtparityd/journal.bin", "journal path")
	timeoutSec := fset.Int("timeout", 300, "seconds of inactivity before sleep")
	minActiveSec := fset.Int("min-active", 60, "minimum seconds disk must be active before considering sleep")
	if err := fset.Parse(args); err != nil {
		return err
	}

	coord := journal.NewCoordinator(*metadataPath, *journalPath)

	recovery, err := coord.Recover()
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if len(recovery.RecoveredTxIDs) > 0 {
		log.Printf("startup recovery: rolled forward %d transaction(s)", len(recovery.RecoveredTxIDs))
	}

	if err := coord.EnableSleep(*timeoutSec, *minActiveSec); err != nil {
		return fmt.Errorf("enable sleep failed: %w", err)
	}

	fmt.Printf("Sleep enabled: timeout=%ds, min-active=%ds\n", *timeoutSec, *minActiveSec)
	return nil
}

func runDisableSleep(args []string) error {
	fset := flag.NewFlagSet("disable-sleep", flag.ContinueOnError)
	metadataPath := fset.String("metadata-path", "/tmp/rtparityd/metadata.bin", "binary metadata snapshot path")
	journalPath := fset.String("journal-path", "/tmp/rtparityd/journal.bin", "journal path")
	if err := fset.Parse(args); err != nil {
		return err
	}

	coord := journal.NewCoordinator(*metadataPath, *journalPath)

	recovery, err := coord.Recover()
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if len(recovery.RecoveredTxIDs) > 0 {
		log.Printf("startup recovery: rolled forward %d transaction(s)", len(recovery.RecoveredTxIDs))
	}

	if err := coord.DisableSleep(); err != nil {
		return fmt.Errorf("disable sleep failed: %w", err)
	}

	fmt.Println("Sleep disabled")
	return nil
}

func runWakeDisk(args []string) error {
	fset := flag.NewFlagSet("wake-disk", flag.ContinueOnError)
	metadataPath := fset.String("metadata-path", "/tmp/rtparityd/metadata.bin", "binary metadata snapshot path")
	journalPath := fset.String("journal-path", "/tmp/rtparityd/journal.bin", "journal path")
	diskID := fset.String("disk-id", "", "disk ID to wake (required)")
	if err := fset.Parse(args); err != nil {
		return err
	}
	if *diskID == "" {
		return fmt.Errorf("flag -disk-id is required")
	}

	coord := journal.NewCoordinator(*metadataPath, *journalPath)

	recovery, err := coord.Recover()
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if len(recovery.RecoveredTxIDs) > 0 {
		log.Printf("startup recovery: rolled forward %d transaction(s)", len(recovery.RecoveredTxIDs))
	}

	result, err := coord.WakeDisk(*diskID)
	if err != nil {
		return fmt.Errorf("wake disk failed: %w", err)
	}

	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

func runSleepDisk(args []string) error {
	fset := flag.NewFlagSet("sleep-disk", flag.ContinueOnError)
	metadataPath := fset.String("metadata-path", "/tmp/rtparityd/metadata.bin", "binary metadata snapshot path")
	journalPath := fset.String("journal-path", "/tmp/rtparityd/journal.bin", "journal path")
	diskID := fset.String("disk-id", "", "disk ID to sleep (required)")
	if err := fset.Parse(args); err != nil {
		return err
	}
	if *diskID == "" {
		return fmt.Errorf("flag -disk-id is required")
	}

	coord := journal.NewCoordinator(*metadataPath, *journalPath)

	recovery, err := coord.Recover()
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if len(recovery.RecoveredTxIDs) > 0 {
		log.Printf("startup recovery: rolled forward %d transaction(s)", len(recovery.RecoveredTxIDs))
	}

	result, err := coord.SleepDisk(*diskID)
	if err != nil {
		return fmt.Errorf("sleep disk failed: %w", err)
	}

	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

func runSleepStatus(args []string) error {
	fset := flag.NewFlagSet("sleep-status", flag.ContinueOnError)
	metadataPath := fset.String("metadata-path", "/tmp/rtparityd/metadata.bin", "binary metadata snapshot path")
	journalPath := fset.String("journal-path", "/tmp/rtparityd/journal.bin", "journal path")
	if err := fset.Parse(args); err != nil {
		return err
	}

	coord := journal.NewCoordinator(*metadataPath, *journalPath)

	recovery, err := coord.Recover()
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if len(recovery.RecoveredTxIDs) > 0 {
		log.Printf("startup recovery: rolled forward %d transaction(s)", len(recovery.RecoveredTxIDs))
	}

	result, err := coord.GetSleepStatus()
	if err != nil {
		return fmt.Errorf("get sleep status failed: %w", err)
	}

	output, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

func runProtectionStatus(args []string) error {
	fset := flag.NewFlagSet("protection-status", flag.ContinueOnError)
	metadataPath := fset.String("metadata-path", "/tmp/rtparityd/metadata.bin", "metadata snapshot path")
	journalPath := fset.String("journal-path", "/tmp/rtparityd/journal.bin", "journal path")
	if err := fset.Parse(args); err != nil {
		return err
	}

	coord := journal.NewCoordinator(*metadataPath, *journalPath)

	recovery, err := coord.Recover()
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if len(recovery.RecoveredTxIDs) > 0 {
		log.Printf("startup recovery: rolled forward %d transaction(s)", len(recovery.RecoveredTxIDs))
	}

	result, err := coord.ProtectionStatus()
	if err != nil {
		return fmt.Errorf("get protection status failed: %w", err)
	}

	state, err := coord.ProtectionState()
	if err != nil {
		return fmt.Errorf("get protection state failed: %w", err)
	}

	type extendedStatus struct {
		State  metadata.PoolProtectionState `json:"protection_state"`
		Status journal.ProtectionStatus     `json:"status"`
	}

	ext := extendedStatus{
		State:  state,
		Status: result,
	}

	output, err := json.MarshalIndent(ext, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(output))
	return nil
}

func runSetProtection(args []string) error {
	fset := flag.NewFlagSet("set-protection", flag.ContinueOnError)
	metadataPath := fset.String("metadata-path", "/tmp/rtparityd/metadata.bin", "metadata snapshot path")
	journalPath := fset.String("journal-path", "/tmp/rtparityd/journal.bin", "journal path")
	targetState := fset.String("state", "", "target protection state: integrity_only, mirrored, or parity")
	if err := fset.Parse(args); err != nil {
		return err
	}
	if *targetState == "" {
		return fmt.Errorf("state is required (integrity_only, mirrored, or parity)")
	}

	validStates := map[string]metadata.PoolProtectionState{
		"integrity_only": metadata.ProtectionIntegrityOnly,
		"mirrored":       metadata.ProtectionMirrored,
		"parity":         metadata.ProtectionParity,
	}
	newState, ok := validStates[*targetState]
	if !ok {
		return fmt.Errorf("invalid state: %s (must be integrity_only, mirrored, or parity)", *targetState)
	}

	coord := journal.NewCoordinator(*metadataPath, *journalPath)

	recovery, err := coord.Recover()
	if err != nil {
		return fmt.Errorf("startup recovery: %w", err)
	}
	if len(recovery.RecoveredTxIDs) > 0 {
		log.Printf("startup recovery: rolled forward %d transaction(s)", len(recovery.RecoveredTxIDs))
	}

	state, err := coord.ProtectionState()
	if err != nil {
		return fmt.Errorf("get protection state: %w", err)
	}
	if state == newState {
		fmt.Printf("protection state already is %s, no change needed\n", *targetState)
		return nil
	}

	if err := coord.SetPoolProtectionState(newState); err != nil {
		return fmt.Errorf("set protection state: %w", err)
	}

	fmt.Printf("protection state changed from %s to %s\n", state, newState)
	return nil
}
