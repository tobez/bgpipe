package rvlive

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// runKafka consumes until an error; partitions without a tracked offset
// start at from, or at the end of the topic if from is zero.
func (s *RvLive) runKafka(from time.Time) error {
	s.Info().Str("broker", s.broker).Str("group", s.group).Msg("connecting")

	// Build client options - based on libbgpstream/rdkafka configuration
	opts := []kgo.Opt{
		kgo.SeedBrokers(s.broker),
		kgo.ConsumerGroup(s.group),
		// untracked partitions start here; a saved offset that retention
		// already deleted (OFFSET_OUT_OF_RANGE on the first fetch) resets to
		// the oldest retained record
		kgo.ConsumeStartOffset(startOffset(from)),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),

		// Use roundrobin balancer for even partition distribution (like bgpstream)
		// Default "range" can cause uneven distribution with many partitions/topics
		kgo.Balancers(kgo.RoundRobinBalancer()),

		// Connection settings
		kgo.ConnIdleTimeout(5 * time.Minute), // Keep connections alive longer

		// Fetch settings - optimized for real-time streaming (like bgpstream)
		kgo.FetchMaxWait(100 * time.Millisecond), // Don't wait long - we want real-time data
		kgo.FetchMaxPartitionBytes(128 * 1024),   // 128KB max per partition (prevents slow consumer issues)

		// Consumer group settings for large partition counts
		kgo.RebalanceTimeout(120 * time.Second), // Allow time for rebalancing 1800+ partitions

		// Log partition assignments
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			count := 0
			for _, parts := range assigned {
				count += len(parts)
			}
			s.Info().Int("topics", len(assigned)).Int("partitions", count).Msg("partitions assigned")
		}),
		// start where we left off (state file or an earlier connection), else from the start time
		kgo.AdjustFetchOffsetsFn(func(_ context.Context, assigned map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
			s.state_mu.Lock()
			resumed := resumeOffsets(assigned, s.state.Offsets)
			s.state_mu.Unlock()
			total := 0
			for _, parts := range assigned {
				total += len(parts)
			}
			ev := s.Info().Int("resumed", resumed).Int("partitions", total)
			if from.IsZero() {
				ev = ev.Str("from", "end")
			} else {
				ev = ev.Time("from", from)
			}
			ev.Msg("start offsets")
			return assigned, nil
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			count := 0
			for _, parts := range revoked {
				count += len(parts)
			}
			s.Debug().Int("topics", len(revoked)).Int("partitions", count).Msg("partitions revoked")
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			count := 0
			for _, parts := range lost {
				count += len(parts)
			}
			s.Warn().Int("topics", len(lost)).Int("partitions", count).Msg("partitions lost")
		}),
	}

	// Create client
	s.Debug().Msg("creating Kafka client")
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("failed to create kafka client: %w", err)
	}
	defer client.Close()

	// Discover and subscribe to topics
	s.Debug().Str("pattern", s.topics).Msg("discovering topics")
	topics, err := s.discoverTopics(client, from)
	if err != nil {
		return fmt.Errorf("failed to discover topics: %w", err)
	} else if len(topics) == 0 {
		return fmt.Errorf("no matching topics found for pattern: %s", s.topics)
	} else {
		s.Debug().Strs("topics", topics).Msg("discovered topics")
	}

	s.Info().Int("count", len(topics)).Msg("subscribing to topics")
	client.AddConsumeTopics(topics...)

	// Start state saver goroutine
	stateSaverDone := make(chan struct{})
	if s.state_file != "" {
		go s.stateSaver(stateSaverDone)
	}

	// Start topic refresher goroutine
	refreshDone := make(chan struct{})
	go s.topicRefresher(client, refreshDone, from)
	s.Debug().Dur("interval", s.refresh).Msg("started topic refresher")

	// Consume messages
	err = s.consume(client)

	// Cleanup
	close(refreshDone)
	if s.state_file != "" {
		close(stateSaverDone)
		if err := s.saveState(); err != nil {
			s.Warn().Err(err).Msg("failed to do final save state")
		}
	}

	return err
}

func (s *RvLive) discoverTopics(client *kgo.Client, from time.Time) ([]string, error) {
	ctx, cancel := context.WithTimeout(s.Ctx, s.timeout)
	defer cancel()

	hasPrefix := func(name string, prefixes []string) bool {
		for _, prefix := range prefixes {
			if len(name) >= len(prefix) && name[:len(prefix)] == prefix {
				return true
			}
		}
		return false
	}

	admin := kadm.NewClient(client)
	endOffsets, err := admin.ListEndOffsets(ctx)
	if err != nil {
		return nil, err
	}

	var topics []string
	for topic, partOffsets := range endOffsets {
		if !s.topics_re.MatchString(topic) {
			continue
		}

		// extract collector + router from topic name
		if len(s.collector) > 0 && !hasPrefix(topic, s.collector) {
			s.Trace().Str("topic", topic).Msg("skipping non-matching collector")
			continue
		}
		if len(s.collector_not) > 0 && hasPrefix(topic, s.collector_not) {
			s.Trace().Str("topic", topic).Msg("skipping excluded collector")
			continue
		}

		// skip dead topics (all partitions have offset 0)
		for _, po := range partOffsets {
			if po.Err == nil && po.Offset > 0 {
				s.Trace().Str("topic", topic).Msg("discovered matching topic")
				topics = append(topics, topic)
				break
			}
		}
	}

	if len(topics) > 0 {
		s.checkRetention(ctx, admin, topics, from)
	}

	return topics, nil
}

func (s *RvLive) topicRefresher(client *kgo.Client, done <-chan struct{}, from time.Time) {
	if s.refresh <= 0 {
		return // disabled
	}

	ticker := time.NewTicker(s.refresh)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case <-s.Ctx.Done():
			return
		case <-ticker.C:
			s.Debug().Msg("refreshing topic list")
			topics, err := s.discoverTopics(client, from)
			if err != nil {
				s.Warn().Err(err).Msg("failed to refresh topics")
				continue
			}

			s.Debug().Int("count", len(topics)).Msg("topic refresh complete")
			// Add any new topics (franz-go handles duplicates)
			client.AddConsumeTopics(topics...)
		}
	}
}

func (s *RvLive) consume(client *kgo.Client) error {
	lastData := time.Now()
	lastStaleCheck := time.Now()
	consecutiveErrors := 0

	for s.Ctx.Err() == nil {
		fetches := client.PollFetches(s.Ctx)

		// Handle errors
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.Canceled || e.Err == context.DeadlineExceeded {
					return nil
				}
				s.Warn().Err(e.Err).Str("topic", e.Topic).Int32("partition", e.Partition).Msg("fetch error")
			}
			consecutiveErrors++
			if consecutiveErrors >= 10 {
				return fmt.Errorf("too many consecutive fetch errors (%d)", consecutiveErrors)
			}
			continue
		}
		consecutiveErrors = 0

		// Check staleness periodically
		if s.stale > 0 && fetches.NumRecords() == 0 && time.Since(lastStaleCheck) >= 30*time.Second {
			lastStaleCheck = time.Now()
			if staleDuration := time.Since(lastData); staleDuration >= s.stale {
				return fmt.Errorf("connection stale: no data for %v", staleDuration.Round(time.Second))
			}
		}

		// Process records
		iter := fetches.RecordIter()
		for !iter.Done() && s.Ctx.Err() == nil {
			record := iter.Next()
			lastData = time.Now()

			if err := s.processRecord(record); err != nil {
				s.Warn().Err(err).
					Str("topic", record.Topic).
					Int32("partition", record.Partition).
					Int64("offset", record.Offset).
					Msg("process error")
				continue
			}

			s.updateOffset(record.Topic, record.Partition, record.Offset+1)
		}
	}

	return nil
}
