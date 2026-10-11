package rvlive

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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
