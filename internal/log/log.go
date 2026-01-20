package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/tom-ok1/proglog/api/v1"
)

type Log struct {
	mu sync.RWMutex

	Dir           string
	Config        Config
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	var baseOffsets []uint64
	for _, file := range files {
		offStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}
	sort.Slice(baseOffsets, func(l, r int) bool {
		return baseOffsets[l] < baseOffsets[r]
	})

	for i := 0; i < len(baseOffsets); i++ {
		seg, err := newSegment(l.Dir, baseOffsets[i], l.Config)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, seg)
		l.activeSegment = seg

		// baseOffsets consist of index and store files
		// so we need to skip the duplicate index
		i++
	}

	if l.segments == nil {
		seg, err := newSegment(l.Dir, l.Config.Segment.InitialOffset, l.Config)
		if err != nil {
			return err
		}
		l.segments = append(l.segments, seg)
		l.activeSegment = seg
	}

	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	highestOff, err := l.highestOffset()
	if err != nil {
		return 0, err
	}

	if l.activeSegment.IsMaxed() {
		seg, err := newSegment(l.Dir, highestOff+1, l.Config)
		if err != nil {
			return 0, err
		}
		l.segments = append(l.segments, seg)
		l.activeSegment = seg
	}

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	return off, nil
}

func (l *Log) Read(off uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	highestOff, err := l.highestOffset()
	if err != nil {
		return nil, err
	}

	if off > highestOff {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	i := sort.Search(len(l.segments), func(i int) bool {
		return l.segments[i].baseOffset > off
	})

	return l.segments[i-1].Read(off)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	return os.RemoveAll(l.Dir)
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOff() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.highestOffset()
}

func (l *Log) highestOffset() (uint64, error) {
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	var i int
	for i < len(l.segments) && l.segments[i].nextOffset-1 <= lowest {
		if err := l.segments[i].Remove(); err != nil {
			return err
		}
		i++
	}

	l.segments = l.segments[i:]
	return nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}

	return io.MultiReader(readers...)
}

type originReader struct {
	*store
	off int64
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}
