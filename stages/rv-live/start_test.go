package rvlive

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
	require.Equal(t, kgo.NewOffset(), assigned[fresh][0], "untracked partition keeps the reset offset")
	require.NotContains(t, assigned, gone, "tracked but unassigned topics are not added")
}
