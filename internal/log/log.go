package log

import (
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
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
	}

	return nil
}
