package journal

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xkzy/rdparityd/internal/metadata"
)

type semaphore chan struct{}

func newSemaphore(n int) semaphore {
	if n <= 0 {
		n = 1
	}
	ch := make(semaphore, n)
	for i := 0; i < n; i++ {
		ch <- struct{}{}
	}
	return ch
}

func (s semaphore) Acquire() { <-s }

func (s semaphore) AcquireContext(ctx context.Context) bool {
	select {
	case <-s:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s semaphore) Release() { s <- struct{}{} }

type DiskWorkerPool struct {
	diskID       string
	workCh       chan func()
	resultCh     chan error
	wg           sync.WaitGroup
	maxWorkers   int
	activeWorker int32
}

func NewDiskWorkerPool(diskID string, maxWorkers int) *DiskWorkerPool {
	pool := &DiskWorkerPool{
		diskID:     diskID,
		workCh:     make(chan func(), 1024),
		resultCh:   make(chan error, 1024),
		maxWorkers: maxWorkers,
	}
	for i := 0; i < maxWorkers; i++ {
		pool.wg.Add(1)
		go pool.worker()
	}
	return pool
}

func (p *DiskWorkerPool) worker() {
	defer p.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("DiskWorkerPool worker recovered from panic: %v", r)
		}
	}()
	for task := range p.workCh {
		atomic.AddInt32(&p.activeWorker, 1)
		task()
		atomic.AddInt32(&p.activeWorker, -1)
	}
}

func (p *DiskWorkerPool) Submit(task func() error) {
	p.workCh <- func() {
		if err := task(); err != nil {
			select {
			case p.resultCh <- err:
			default:
			}
		}
	}
}

func (p *DiskWorkerPool) Wait() error {
	close(p.workCh)
	p.wg.Wait()
	close(p.resultCh)
	var firstErr error
	for err := range p.resultCh {
		if firstErr == nil && err != nil {
			firstErr = err
		}
	}
	return firstErr
}

func (p *DiskWorkerPool) Close() {
	close(p.workCh)
	p.wg.Wait()
	for {
		select {
		case _, ok := <-p.resultCh:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func (p *DiskWorkerPool) ActiveWorkers() int {
	return int(atomic.LoadInt32(&p.activeWorker))
}

type DiskScheduler struct {
	mu              sync.Mutex
	pools           map[string]*DiskWorkerPool
	readLimit       int
	writeLimit      int
	readSemaphores  map[string]semaphore
	writeSemaphores map[string]semaphore
}

func NewDiskScheduler(readLimit, writeLimit int) *DiskScheduler {
	if readLimit <= 0 {
		readLimit = 4
	}
	if writeLimit <= 0 {
		writeLimit = 2
	}
	return &DiskScheduler{
		pools:           make(map[string]*DiskWorkerPool),
		readLimit:       readLimit,
		writeLimit:      writeLimit,
		readSemaphores:  make(map[string]semaphore),
		writeSemaphores: make(map[string]semaphore),
	}
}

func (s *DiskScheduler) EnsureDisk(diskID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pools[diskID] == nil {
		s.pools[diskID] = NewDiskWorkerPool(diskID, s.readLimit)
		s.readSemaphores[diskID] = newSemaphore(s.readLimit)
		s.writeSemaphores[diskID] = newSemaphore(s.writeLimit)
	}
}

func (s *DiskScheduler) GetPool(diskID string) *DiskWorkerPool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pools[diskID]
}

func (s *DiskScheduler) GetReadSemaphore(diskID string) semaphore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.readSemaphores[diskID] == nil {
		s.readSemaphores[diskID] = newSemaphore(s.readLimit)
	}
	return s.readSemaphores[diskID]
}

func (s *DiskScheduler) GetWriteSemaphore(diskID string) semaphore {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.writeSemaphores[diskID] == nil {
		s.writeSemaphores[diskID] = newSemaphore(s.writeLimit)
	}
	return s.writeSemaphores[diskID]
}

func (s *DiskScheduler) disks() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	disks := make([]string, 0, len(s.pools))
	for id := range s.pools {
		disks = append(disks, id)
	}
	return disks
}

func (s *DiskScheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, pool := range s.pools {
		pool.Close()
	}
}

type ExtentReadRequest struct {
	Extent   metadata.Extent
	Result   chan *ExtentReadResult
	Priority int
}

type ExtentReadResult struct {
	Data     []byte
	Verified bool
	Healed   bool
	Error    error
}

type ReadCoordinator struct {
	scheduler *DiskScheduler
	rootDir   string
	state     *metadata.SampleState
}

func NewReadCoordinator(scheduler *DiskScheduler, rootDir string, state metadata.SampleState) *ReadCoordinator {
	return &ReadCoordinator{
		scheduler: scheduler,
		rootDir:   rootDir,
		state:     &state,
	}
}

const (
	maxConcurrentExtents = 64
)

func (r *ReadCoordinator) ReadExtents(ctx context.Context, extents []metadata.Extent) ([]*ExtentReadResult, error) {
	if len(extents) == 0 {
		return nil, nil
	}

	type resultWithIdx struct {
		idx int
		res *ExtentReadResult
	}

	results := make([]*ExtentReadResult, len(extents))
	resultCh := make(chan resultWithIdx, len(extents))

	sem := newSemaphore(maxConcurrentExtents)

	for i, ext := range extents {
		idx := i

		go func(extent metadata.Extent, idx int) {
			defer func() {
				if r := recover(); r != nil {
					resultCh <- resultWithIdx{idx: idx, res: &ExtentReadResult{Error: fmt.Errorf("ReadExtents goroutine recovered from panic: %v", r)}}
				}
			}()

			if !sem.AcquireContext(ctx) {
				resultCh <- resultWithIdx{idx: idx, res: &ExtentReadResult{Error: ctx.Err()}}
				return
			}
			defer sem.Release()

			diskID := extent.DataDiskID
			r.scheduler.EnsureDisk(diskID)

			sem := r.scheduler.GetReadSemaphore(diskID)
			if !sem.AcquireContext(ctx) {
				resultCh <- resultWithIdx{idx: idx, res: &ExtentReadResult{Error: ctx.Err()}}
				return
			}
			defer sem.Release()

			pool := r.scheduler.GetPool(diskID)
			if pool == nil {
				resultCh <- resultWithIdx{idx: idx, res: &ExtentReadResult{Error: fmt.Errorf("no pool for disk %s", diskID)}}
				return
			}

			pool.Submit(func() error {
				data, verified, healed, err := r.readAndVerifyExtent(ctx, extent)
				resultCh <- resultWithIdx{idx: idx, res: &ExtentReadResult{
					Data:     data,
					Verified: verified,
					Healed:   healed,
					Error:    err,
				}}
				return err
			})
		}(ext, idx)
	}

	var firstErr error
	for i := 0; i < len(extents); i++ {
		result := <-resultCh
		if result.res.Error != nil && firstErr == nil {
			firstErr = result.res.Error
		}
		results[result.idx] = result.res
	}

	close(resultCh)

	return results, firstErr
}

func (r *ReadCoordinator) readAndVerifyExtent(ctx context.Context, extent metadata.Extent) ([]byte, bool, bool, error) {
	path := filepath.Join(r.rootDir, extent.PhysicalLocator.RelativePath)

	if ctx.Err() != nil {
		return nil, false, false, ctx.Err()
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if ctx.Err() != nil {
			return nil, false, false, ctx.Err()
		}
		if r.state == nil {
			return nil, false, false, fmt.Errorf("read extent %s: %w", extent.ExtentID, err)
		}
		data, err = reconstructExtent(r.rootDir, *r.state, extent)
		if err != nil {
			return nil, false, false, fmt.Errorf("reconstruct extent %s: %w", extent.ExtentID, err)
		}
		return data, true, true, nil
	}

	if ctx.Err() != nil {
		return nil, false, false, ctx.Err()
	}

	checksum := digestBytes(data)
	if checksum == extent.Checksum {
		if extent.CompressionAlg != "" && extent.CompressionAlg != metadata.CompressionNone {
			decompressed, err := decompress(data, CompressionAlg(extent.CompressionAlg))
			if err != nil {
				return nil, false, false, fmt.Errorf("decompress extent %s: %w", extent.ExtentID, err)
			}
			if int64(len(decompressed)) != extent.Length {
				return nil, false, false, fmt.Errorf("decompressed length mismatch for %s", extent.ExtentID)
			}
			return decompressed, true, false, nil
		}
		if int64(len(data)) == extent.Length {
			return append([]byte(nil), data...), true, false, nil
		}
	}

	if r.state == nil {
		return nil, false, false, fmt.Errorf("extent checksum mismatch for %s", extent.ExtentID)
	}

	data, err = reconstructExtent(r.rootDir, *r.state, extent)
	if err != nil {
		return nil, false, false, fmt.Errorf("reconstruct extent %s: %w", extent.ExtentID, err)
	}

	newChecksum := digestBytes(data)
	if newChecksum != extent.Checksum {
		return nil, false, false, fmt.Errorf("reconstructed checksum mismatch for %s", extent.ExtentID)
	}
	return data, true, true, nil
}

type WriteCoordinator struct {
	scheduler   *DiskScheduler
	rootDir     string
	parityLocks *ParityGroupLocker
}

func NewWriteCoordinator(scheduler *DiskScheduler, rootDir string) *WriteCoordinator {
	return &WriteCoordinator{
		scheduler:   scheduler,
		rootDir:     rootDir,
		parityLocks: NewParityGroupLocker(),
	}
}

func (w *WriteCoordinator) WriteExtents(ctx context.Context, extents []metadata.Extent, payload []byte) error {
	if len(extents) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	errors := make([]error, len(extents))
	sem := newSemaphore(maxConcurrentExtents)

	for i, ext := range extents {
		wg.Add(1)
		go func(idx int, extent metadata.Extent) {
			defer func() {
				if r := recover(); r != nil {
					errors[idx] = fmt.Errorf("WriteExtents goroutine recovered from panic: %v", r)
					wg.Done()
				}
			}()
			defer wg.Done()

			if !sem.AcquireContext(ctx) {
				errors[idx] = ctx.Err()
				return
			}
			defer sem.Release()

			diskID := extent.DataDiskID
			w.scheduler.EnsureDisk(diskID)

			sem := w.scheduler.GetWriteSemaphore(diskID)
			if !sem.AcquireContext(ctx) {
				errors[idx] = ctx.Err()
				return
			}
			defer sem.Release()

			errors[idx] = w.writeExtentToDisk(ctx, extent, payload)
		}(i, ext)
	}

	wg.Wait()

	for _, err := range errors {
		if err != nil {
			return err
		}
	}
	return nil
}

func (w *WriteCoordinator) writeExtentToDisk(ctx context.Context, extent metadata.Extent, payload []byte) error {
	path := filepath.Join(w.rootDir, extent.PhysicalLocator.RelativePath)

	if ctx.Err() != nil {
		return ctx.Err()
	}

	extentData := extentData(extent, payload)

	if err := replaceSyncFile(path, extentData, 0o600); err != nil {
		return fmt.Errorf("write extent %s: %w", extent.ExtentID, err)
	}

	return nil
}

type ParityGroupLocker struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func NewParityGroupLocker() *ParityGroupLocker {
	return &ParityGroupLocker{
		locks: make(map[string]*sync.Mutex),
	}
}

func (p *ParityGroupLocker) LockGroup(groupID string) func() {
	p.mu.Lock()
	if p.locks[groupID] == nil {
		p.locks[groupID] = &sync.Mutex{}
	}
	m := p.locks[groupID]
	p.mu.Unlock()

	m.Lock()
	return func() { m.Unlock() }
}

type IOScheduler struct {
	userReadCh  chan *ExtentReadRequest
	userWriteCh chan interface{}
	scrubCh     chan interface{}
	rebuildCh   chan interface{}

	diskPools map[string]*DiskWorkerPool
	cache     *ReadAheadCache
	wg        sync.WaitGroup

	maxConcurrent int
	stopCh        chan struct{}
}

func NewIOScheduler(maxConcurrent int) *IOScheduler {
	if maxConcurrent <= 0 {
		maxConcurrent = 8
	}
	return &IOScheduler{
		userReadCh:    make(chan *ExtentReadRequest, 256),
		userWriteCh:   make(chan interface{}, 256),
		scrubCh:       make(chan interface{}, 64),
		rebuildCh:     make(chan interface{}, 64),
		diskPools:     make(map[string]*DiskWorkerPool),
		maxConcurrent: maxConcurrent,
		stopCh:        make(chan struct{}),
	}
}

func (s *IOScheduler) SetCache(cache *ReadAheadCache) {
	s.cache = cache
}

func (s *IOScheduler) AddDiskPool(diskID string, pool *DiskWorkerPool) {
	s.diskPools[diskID] = pool
}

func (s *IOScheduler) Start() {
	for i := 0; i < s.maxConcurrent; i++ {
		s.wg.Add(1)
		go s.worker(i)
	}
}

func (s *IOScheduler) worker(id int) {
	defer s.wg.Done()
	defer func() {
		if r := recover(); r != nil {
			log.Printf("IOScheduler worker %d recovered from panic: %v", id, r)
		}
	}()

	for {
		select {
		case <-s.stopCh:
			return
		case req := <-s.userReadCh:
			s.handleReadRequest(req)
		case task := <-s.userWriteCh:
			s.handleWriteRequest(task)
		case task := <-s.scrubCh:
			s.handleScrubRequest(task)
		case task := <-s.rebuildCh:
			s.handleRebuildRequest(task)
		}
	}
}

func (s *IOScheduler) handleReadRequest(req *ExtentReadRequest) {
	if s.cache != nil {
		if data, ok := s.cache.Get(req.Extent.ExtentID); ok {
			req.Result <- &ExtentReadResult{
				Data:     data,
				Verified: true,
				Healed:   false,
			}
			return
		}
	}

	if len(s.diskPools) == 0 {
		req.Result <- &ExtentReadResult{
			Error: fmt.Errorf("no disk pools available"),
		}
		return
	}

	pool, ok := s.diskPools[req.Extent.DataDiskID]
	if !ok {
		pool = s.diskPools["default"]
	}
	if pool == nil {
		req.Result <- &ExtentReadResult{
			Error: fmt.Errorf("no pool for disk %s", req.Extent.DataDiskID),
		}
		return
	}

	pool.Submit(func() error {
		req.Result <- &ExtentReadResult{
			Error: fmt.Errorf("IOScheduler: cache miss, caller must handle I/O"),
		}
		return nil
	})
}

type WriteTask struct {
	Extent metadata.Extent
	Data   []byte
	Done   chan error
}

func (s *IOScheduler) handleWriteRequest(task interface{}) {
	wt, ok := task.(*WriteTask)
	if !ok {
		return
	}
	if len(s.diskPools) == 0 {
		wt.Done <- fmt.Errorf("no disk pools available")
		return
	}
	pool, ok := s.diskPools[wt.Extent.DataDiskID]
	if !ok {
		pool = s.diskPools["default"]
	}
	if pool == nil {
		wt.Done <- fmt.Errorf("no pool for disk %s", wt.Extent.DataDiskID)
		return
	}
	pool.Submit(func() error {
		wt.Done <- nil
		return nil
	})
}

type ScrubTask struct {
	Extent metadata.Extent
	Result chan error
}

func (s *IOScheduler) handleScrubRequest(task interface{}) {
	st, ok := task.(*ScrubTask)
	if !ok {
		return
	}
	if len(s.diskPools) == 0 {
		st.Result <- fmt.Errorf("no disk pools available")
		return
	}
	pool, ok := s.diskPools[st.Extent.DataDiskID]
	if !ok {
		pool = s.diskPools["default"]
	}
	if pool == nil {
		st.Result <- fmt.Errorf("no pool for disk %s", st.Extent.DataDiskID)
		return
	}
	pool.Submit(func() error {
		st.Result <- nil
		return nil
	})
}

type RebuildTask struct {
	Extent metadata.Extent
	Result chan error
}

func (s *IOScheduler) handleRebuildRequest(task interface{}) {
	rt, ok := task.(*RebuildTask)
	if !ok {
		return
	}
	if len(s.diskPools) == 0 {
		rt.Result <- fmt.Errorf("no disk pools available")
		return
	}
	pool, ok := s.diskPools[rt.Extent.DataDiskID]
	if !ok {
		pool = s.diskPools["default"]
	}
	if pool == nil {
		rt.Result <- fmt.Errorf("no pool for disk %s", rt.Extent.DataDiskID)
		return
	}
	pool.Submit(func() error {
		rt.Result <- nil
		return nil
	})
}

func (s *IOScheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

type ReadAheadCache struct {
	mu         sync.Mutex
	maxBytes   int64
	windowSize int64
	cache      map[string][]byte
	order      []string
	maxEntries int
}

func NewReadAheadCache(maxBytes, windowSize int64) *ReadAheadCache {
	if windowSize <= 0 {
		windowSize = 4 * 1024 * 1024
	}
	if maxBytes <= 0 {
		maxBytes = 16 * 1024 * 1024
	}
	maxEntries := int(maxBytes / 4096)
	if maxEntries < 4 {
		maxEntries = 4
	}
	return &ReadAheadCache{
		maxBytes:   maxBytes,
		windowSize: windowSize,
		cache:      make(map[string][]byte),
		order:      make([]string, 0, maxEntries),
		maxEntries: maxEntries,
	}
}

func (c *ReadAheadCache) Get(extentID string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	data, ok := c.cache[extentID]
	return data, ok
}

func (c *ReadAheadCache) Put(extentID string, data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.cache[extentID]; !ok {
		if len(c.order) >= c.maxEntries {
			oldest := c.order[0]
			delete(c.cache, oldest)
			c.order = c.order[1:]
		}
		c.order = append(c.order, extentID)
	}
	c.cache[extentID] = data
}

func (c *ReadAheadCache) Prefetch(extents []metadata.Extent) {
}

func (c *ReadAheadCache) Close() {
}

type ScrubConfig struct {
	ExtentWorkersPerDisk int
	VerifyBatchSize      int
	ThrottleDuration     time.Duration
	MaxBandwidthMBps     int
}

func DefaultScrubConfig() ScrubConfig {
	return ScrubConfig{
		ExtentWorkersPerDisk: 2,
		VerifyBatchSize:      32,
		ThrottleDuration:     100 * time.Millisecond,
		MaxBandwidthMBps:     0,
	}
}

type RebuildConfig struct {
	MaxConcurrent    int
	VerifyBatchSize  int
	ThrottleDuration time.Duration
}

func DefaultRebuildConfig() RebuildConfig {
	return RebuildConfig{
		MaxConcurrent:    4,
		VerifyBatchSize:  10,
		ThrottleDuration: 50 * time.Millisecond,
	}
}
