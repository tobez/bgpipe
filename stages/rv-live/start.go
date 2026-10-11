package rvlive

import (
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// parseSince parses the --since value: RFC 3339, Unix seconds (from 2001 on),
// or YYYYMMDD.HHMM in UTC as in RouteViews archive file names.
// Times after now are rejected.
func parseSince(v string, now time.Time) (time.Time, error) {
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		t, err = time.ParseInLocation("20060102.1504", v, time.UTC)
	}
	if err != nil {
		sec, perr := strconv.ParseInt(v, 10, 64)
		if perr != nil || sec < 1_000_000_000 {
			return time.Time{}, fmt.Errorf("invalid time %q: want RFC 3339, Unix seconds or YYYYMMDD.HHMM (UTC)", v)
		}
		t = time.Unix(sec, 0)
	}
	if t.After(now) {
		return time.Time{}, fmt.Errorf("time %q is in the future", v)
	}
	return t.UTC(), nil
}

// resumeOffsets makes assigned partitions with a tracked offset start there.
// The others keep their offset, which resolves to the reset offset
// (--since or the end of the topic). Returns the number of resumed partitions.
func resumeOffsets(assigned map[string]map[int32]kgo.Offset, tracked map[string]map[int32]int64) int {
	n := 0
	for topic, parts := range assigned {
		for part := range parts {
			if off, ok := tracked[topic][part]; ok {
				parts[part] = kgo.NewOffset().At(off)
				n++
			}
		}
	}
	return n
}
