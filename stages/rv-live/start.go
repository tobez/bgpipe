package rvlive

import (
	"fmt"
	"strconv"
	"time"
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
