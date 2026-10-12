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

// startFrom returns the time where partitions without a tracked offset start:
// --since, else where the previous connection's untracked partitions started,
// else the save time of the loaded state file. Zero means the end of the topic.
func startFrom(since, prev, saved time.Time) time.Time {
	switch {
	case !since.IsZero():
		return since
	case !prev.IsZero():
		return prev
	default:
		return saved
	}
}

// nextFrom returns the prev value for startFrom on the next connection:
// the from-time of a connection that began at began, or began itself if it
// started at the end (its untracked partitions have nothing before began).
func nextFrom(from, began time.Time) time.Time {
	if from.IsZero() {
		return began
	}
	return from
}

// startOffset returns the Kafka offset for a startFrom time.
func startOffset(from time.Time) kgo.Offset {
	if from.IsZero() {
		return kgo.NewOffset().AtEnd()
	}
	return kgo.NewOffset().AfterMilli(from.UnixMilli())
}

// resumeOffsets makes assigned partitions with a tracked offset start there.
// The others keep their offset, which franz-go sets to the start offset
// (see startOffset). Returns the number of resumed partitions.
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

// truncation is a partition whose log no longer reaches back to the start time:
// First is the time of its oldest retained record.
type truncation struct {
	Topic     string
	Partition int32
	First     time.Time
}

// findTruncated returns untracked partitions where the first record after
// the start time is also the log start, while older records were deleted
// (start > 0). Then records between the start time and First may be lost.
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
		return partitionLess(out[i].Topic, out[i].Partition, out[j].Topic, out[j].Partition)
	})
	return out
}

// expiry is a partition whose saved offset lies below its log start:
// records from Saved to Start were deleted before they could be read.
type expiry struct {
	Topic     string
	Partition int32
	Saved     int64
	Start     int64
}

// findExpired returns tracked partitions whose offset is below the log start.
func findExpired(start kadm.ListedOffsets, tracked map[string]map[int32]int64) []expiry {
	var out []expiry
	start.Each(func(st kadm.ListedOffset) {
		saved, ok := tracked[st.Topic][st.Partition]
		if !ok || st.Err != nil || saved >= st.Offset {
			return
		}
		out = append(out, expiry{Topic: st.Topic, Partition: st.Partition, Saved: saved, Start: st.Offset})
	})
	sort.Slice(out, func(i, j int) bool {
		return partitionLess(out[i].Topic, out[i].Partition, out[j].Topic, out[j].Partition)
	})
	return out
}

// partitionLess orders partitions by topic, then partition number.
func partitionLess(topic1 string, part1 int32, topic2 string, part2 int32) bool {
	if topic1 != topic2 {
		return topic1 < topic2
	}
	return part1 < part2
}

// checkRetention warns once per topic whose retention does not reach the
// start time from (unless zero), and once per topic whose retention deleted
// records after a saved offset.
func (s *RvLive) checkRetention(ctx context.Context, admin *kadm.Client, topics []string, from time.Time) {
	s.state_mu.Lock()
	resuming := len(s.state.Offsets) > 0
	s.state_mu.Unlock()
	if from.IsZero() && !resuming {
		return
	}

	start, err := admin.ListStartOffsets(ctx, topics...)
	if err != nil {
		s.Warn().Err(err).Msg("retention check: listing start offsets failed")
		return
	}
	var after kadm.ListedOffsets
	if !from.IsZero() {
		after, err = admin.ListOffsetsAfterMilli(ctx, from.UnixMilli(), topics...)
		if err != nil {
			s.Warn().Err(err).Msg("retention check: listing offsets after the start time failed")
			return
		}
	}

	s.state_mu.Lock()
	defer s.state_mu.Unlock()
	for _, tr := range findTruncated(after, start, s.state.Offsets) {
		if s.retention_warned[tr.Topic] {
			continue
		}
		s.retention_warned[tr.Topic] = true
		s.Warn().Str("topic", tr.Topic).Int32("partition", tr.Partition).
			Time("from", from).Time("first", tr.First).Dur("gap", tr.First.Sub(from)).
			Msg("Kafka retention does not reach the start time, updates before first may be lost")
	}
	for _, ex := range findExpired(start, s.state.Offsets) {
		if s.expiry_warned[ex.Topic] {
			continue
		}
		s.expiry_warned[ex.Topic] = true
		s.Warn().Str("topic", ex.Topic).Int32("partition", ex.Partition).
			Int64("saved", ex.Saved).Int64("start", ex.Start).
			Msg("Kafka retention deleted records after the saved offset, updates before start are lost")
	}
}
