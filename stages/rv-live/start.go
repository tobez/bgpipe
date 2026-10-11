package rvlive

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
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

// truncation is a partition whose log no longer reaches back to --since:
// First is the time of its oldest retained record.
type truncation struct {
	Topic     string
	Partition int32
	First     time.Time
}

// findTruncated returns untracked partitions where the first record after
// --since is also the log start, while older records were deleted (start > 0).
// Then records between --since and First may be lost.
func findTruncated(after, start kadm.ListedOffsets, tracked map[string]map[int32]int64) []truncation {
	var out []truncation
	after.Each(func(a kadm.ListedOffset) {
		if _, ok := tracked[a.Topic][a.Partition]; ok || a.Err != nil || a.Timestamp < 0 {
			return
		}
		st, ok := start.Lookup(a.Topic, a.Partition)
		if !ok || st.Err != nil || st.Offset <= 0 || st.Offset != a.Offset {
			return
		}
		out = append(out, truncation{Topic: a.Topic, Partition: a.Partition, First: time.UnixMilli(a.Timestamp).UTC()})
	})
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

// checkRetention warns once per topic whose retention does not reach --since.
func (s *RvLive) checkRetention(ctx context.Context, admin *kadm.Client, topics []string) {
	after, err := admin.ListOffsetsAfterMilli(ctx, s.since.UnixMilli(), topics...)
	if err != nil {
		s.Warn().Err(err).Msg("retention check: listing offsets after --since failed")
		return
	}
	start, err := admin.ListStartOffsets(ctx, topics...)
	if err != nil {
		s.Warn().Err(err).Msg("retention check: listing start offsets failed")
		return
	}

	s.state_mu.Lock()
	defer s.state_mu.Unlock()
	for _, tr := range findTruncated(after, start, s.state.Offsets) {
		if s.retention_warned[tr.Topic] {
			continue
		}
		s.retention_warned[tr.Topic] = true
		s.Warn().Str("topic", tr.Topic).Int32("partition", tr.Partition).
			Time("first", tr.First).Dur("gap", tr.First.Sub(s.since)).
			Msg("Kafka retention does not reach --since, updates before first may be lost")
	}
}
