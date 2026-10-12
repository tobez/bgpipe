package rvlive

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestParseSince(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	want := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)

	for _, in := range []string{
		"2026-10-10T12:00:00Z",
		"2026-10-10T14:00:00+02:00",
		"1791633600",
		"20261010.1200",
	} {
		got, err := parseSince(in, now)
		require.NoError(t, err, in)
		require.True(t, want.Equal(got), "%s: got %v", in, got)
	}

	for _, in := range []string{
		"20261010",             // date only: must not become Unix seconds in 1970
		"999999999",            // Unix seconds before 2001
		"2026-10-12T00:00:00Z", // in the future
		"20261012.0000",        // in the future
		"1791763200",           // Unix seconds in the future
		"20261010.12",
		"yesterday",
		"-1791633600",
	} {
		_, err := parseSince(in, now)
		require.Error(t, err, in)
	}
}

func TestResumeOffsets(t *testing.T) {
	const (
		tracked1 = "routeviews.phoix.142271.bmp_raw"
		fresh    = "routeviews.phoix.9927.bmp_raw" // assigned, never consumed (e.g. new peer)
		gone     = "routeviews.linx.6762.bmp_raw"  // tracked, not assigned now
	)
	assigned := map[string]map[int32]kgo.Offset{
		tracked1: {0: kgo.NewOffset()},
		fresh:    {0: kgo.NewOffset()},
	}
	tracked := map[string]map[int32]int64{
		tracked1: {0: 1234},
		gone:     {0: 99},
	}

	n := resumeOffsets(assigned, tracked)

	require.Equal(t, 1, n)
	require.Equal(t, kgo.NewOffset().At(1234), assigned[tracked1][0])
	require.Equal(t, kgo.NewOffset(), assigned[fresh][0], "untracked partition keeps the start offset")
	require.NotContains(t, assigned, gone, "tracked but unassigned topics are not added")
}

func TestFindTruncated(t *testing.T) {
	since := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	first := since.Add(90 * time.Minute)
	ts := first.UnixMilli()

	lo := func(topic string, off, ts int64, err error) map[int32]kadm.ListedOffset {
		return map[int32]kadm.ListedOffset{0: {Topic: topic, Partition: 0, Offset: off, Timestamp: ts, Err: err}}
	}
	after := kadm.ListedOffsets{
		"a.truncated": lo("a.truncated", 500, ts, nil), // first record after since is the log start
		"b.ok":        lo("b.ok", 700, ts, nil),        // older records still retained
		"c.never":     lo("c.never", 0, ts, nil),       // log start 0: nothing ever deleted
		"d.quiet":     lo("d.quiet", 500, -1, nil),     // no record after since
		"e.tracked":   lo("e.tracked", 500, ts, nil),   // resumes from a tracked offset
		"f.err":       lo("f.err", 500, ts, errors.New("x")),
		"g.nostart":   lo("g.nostart", 500, ts, nil), // missing from start
		"h.starterr":  lo("h.starterr", 500, ts, nil),
	}
	start := kadm.ListedOffsets{
		"a.truncated": lo("a.truncated", 500, -1, nil),
		"b.ok":        lo("b.ok", 500, -1, nil),
		"c.never":     lo("c.never", 0, -1, nil),
		"d.quiet":     lo("d.quiet", 500, -1, nil),
		"e.tracked":   lo("e.tracked", 500, -1, nil),
		"f.err":       lo("f.err", 500, -1, nil),
		"h.starterr":  lo("h.starterr", 500, -1, errors.New("x")),
	}
	tracked := map[string]map[int32]int64{"e.tracked": {0: 42}}

	got := findTruncated(after, start, tracked)

	require.Equal(t, []truncation{{Topic: "a.truncated", Partition: 0, First: first}}, got)
}

func TestStartFrom(t *testing.T) {
	since := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	prev := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)
	saved := time.Date(2026, 10, 11, 8, 0, 0, 0, time.UTC)
	var zero time.Time

	for _, tc := range []struct {
		name               string
		since, prev, saved time.Time
		want               time.Time
	}{
		{"since wins over all", since, prev, saved, since},
		{"since alone", since, zero, zero, since},
		{"previous connection wins over state", zero, prev, saved, prev},
		{"previous connection alone", zero, prev, zero, prev},
		{"state file save time", zero, zero, saved, saved},
		{"nothing: end of topic", zero, zero, zero, zero},
	} {
		require.Equal(t, tc.want, startFrom(tc.since, tc.prev, tc.saved), tc.name)
	}
}

func TestNextFrom(t *testing.T) {
	from := time.Date(2026, 10, 11, 8, 0, 0, 0, time.UTC)
	began := time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)

	require.Equal(t, from, nextFrom(from, began), "untracked partitions of the previous connection started at from")
	require.Equal(t, began, nextFrom(time.Time{}, began), "previous connection started at the end")
}

func TestStartOffset(t *testing.T) {
	from := time.Date(2026, 10, 11, 8, 0, 0, 0, time.UTC)

	require.Equal(t, kgo.NewOffset().AtEnd(), startOffset(time.Time{}))
	require.Equal(t, kgo.NewOffset().AfterMilli(from.UnixMilli()), startOffset(from))
}

func TestFindExpired(t *testing.T) {
	lo := func(topic string, off int64, err error) map[int32]kadm.ListedOffset {
		return map[int32]kadm.ListedOffset{0: {Topic: topic, Partition: 0, Offset: off, Timestamp: -1, Err: err}}
	}
	start := kadm.ListedOffsets{
		"a.expired":   lo("a.expired", 500, nil), // saved offset deleted by retention
		"b.equal":     lo("b.equal", 500, nil),   // saved offset is the log start
		"c.above":     lo("c.above", 500, nil),
		"d.untracked": lo("d.untracked", 500, nil),
		"e.err":       lo("e.err", 500, errors.New("x")),
	}
	tracked := map[string]map[int32]int64{
		"a.expired": {0: 42},
		"b.equal":   {0: 500},
		"c.above":   {0: 600},
		"e.err":     {0: 42},
		"f.missing": {0: 42}, // not in start
	}

	got := findExpired(start, tracked)

	require.Equal(t, []expiry{{Topic: "a.expired", Partition: 0, Saved: 42, Start: 500}}, got)
}
