package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/rtparityd/rtparityd/internal/journal"
	"github.com/rtparityd/rtparityd/internal/metadata"
	"github.com/rtparityd/rtparityd/internal/parity"
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
