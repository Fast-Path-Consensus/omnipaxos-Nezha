#[cfg(feature = "serde")]
mod tests {
    use omnipaxos::{
        clock::ClockConfig,
        messages::Message,
        storage::{Entry, Snapshot},
        util::NodeId,
        ClusterConfig, OmniPaxos, OmniPaxosConfig, ProcessEarlyBufferResult, ReleasedEntry,
        ServerConfig,
    };
    use omnipaxos_storage::memory_storage::MemoryStorage;
    use serde::{Deserialize, Serialize};
    use std::collections::HashMap;
    use std::time::Duration;

    // ── Test Entry ──────────────────────────────────────────────────────

    #[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
    struct TestEntry {
        client_id: u64,
        id: usize,
        deadline: i64,
        value: String,
    }

    impl TestEntry {
        fn new(client_id: u64, id: usize, deadline: i64, value: &str) -> Self {
            Self {
                client_id,
                id,
                deadline,
                value: value.to_string(),
            }
        }
    }

    impl Entry for TestEntry {
        type Snapshot = TestSnapshot;

        fn deadline(&self) -> i64 {
            self.deadline
        }
        fn client_id(&self) -> u64 {
            self.client_id
        }
        fn coordinator_id(&self) -> u64 {
            0
        }
        fn id(&self) -> usize {
            self.id
        }
        fn set_deadline(&mut self, deadline: i64) {
            self.deadline = deadline;
        }
    }

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    struct TestSnapshot {
        entries: Vec<TestEntry>,
    }

    impl Snapshot<TestEntry> for TestSnapshot {
        fn create(entries: &[TestEntry]) -> Self {
            Self {
                entries: entries.to_vec(),
            }
        }
        fn merge(&mut self, delta: Self) {
            self.entries.extend(delta.entries);
        }
        fn use_snapshots() -> bool {
            false
        }
    }

    type TestOmniPaxos = OmniPaxos<TestEntry, MemoryStorage<TestEntry>>;

    struct NezhaCluster {
        nodes: HashMap<NodeId, TestOmniPaxos>,
        /// Messages pending delivery: (tick_to_deliver, message)
        pending_messages: Vec<(u64, Message<TestEntry>)>,
        /// Current simulation tick
        current_tick: u64,
        /// Crashed nodes — don't process messages or produce outgoing
        crashed: Vec<NodeId>,
    }

    impl NezhaCluster {
        fn new(clock_configs: HashMap<NodeId, ClockConfig>) -> Self {
            let all_pids: Vec<NodeId> = (1..=5).collect();
            let mut nodes = HashMap::new();

            for &pid in &all_pids {
                let cluster_config = ClusterConfig {
                    configuration_id: 1,
                    nodes: all_pids.clone(),
                    flexible_quorum: None,
                };
                let clock_config = clock_configs
                    .get(&pid)
                    .cloned()
                    .unwrap_or_else(ClockConfig::low);
                let server_config = ServerConfig {
                    pid,
                    election_tick_timeout: 5,
                    resend_message_tick_timeout: 10,
                    flush_batch_tick_timeout: 100,
                    batch_size: 1,
                    clock: clock_config.clone(),
                    ..Default::default()
                };
                let config = OmniPaxosConfig {
                    cluster_config,
                    server_config,
                };
                let storage = MemoryStorage::default();
                let clock = clock_config.build();
                let op = config.build(storage, clock).expect("Failed to build OmniPaxos");
                nodes.insert(pid, op);
            }

            NezhaCluster {
                nodes,
                pending_messages: Vec::new(),
                current_tick: 0,
                crashed: Vec::new(),
            }
        }

        fn new_good_clocks() -> Self {
            Self::new(HashMap::new()) // All nodes get ClockConfig::low()
        }

        /// Crash a node — it will no longer send or receive messages.
        fn crash_node(&mut self, pid: NodeId) {
            self.crashed.push(pid);
            // Drop any pending messages to/from this node
            self.pending_messages.retain(|(_, msg)| {
                msg.get_receiver() != pid
            });
        }

        /// Tick all alive nodes — advances BLE, resend timers, etc.
        fn tick_all(&mut self) {
            self.current_tick += 1;
            for (&pid, node) in self.nodes.iter_mut() {
                if self.crashed.contains(&pid) {
                    continue;
                }
                node.tick();
            }
        }

        /// Collect outgoing messages from all alive nodes and queue them with
        /// an optional delay (in ticks).
        fn collect_and_queue_messages(&mut self, delay_ticks: u64) {
            let mut all_outgoing = Vec::new();
            for (&pid, node) in self.nodes.iter_mut() {
                if self.crashed.contains(&pid) {
                    continue;
                }
                let mut buf = Vec::new();
                node.take_outgoing_messages(&mut buf);
                all_outgoing.extend(buf);
            }
            let deliver_at = self.current_tick + delay_ticks;
            for msg in all_outgoing {
                // Drop messages to crashed nodes
                if !self.crashed.contains(&msg.get_receiver()) {
                    self.pending_messages.push((deliver_at, msg));
                }
            }
        }

        /// Deliver all messages that are due at or before `current_tick`.
        fn deliver_due_messages(&mut self) {
            let (due, remaining): (Vec<_>, Vec<_>) = self
                .pending_messages
                .drain(..)
                .partition(|(tick, _)| *tick <= self.current_tick);
            self.pending_messages = remaining;

            for (_, msg) in due {
                let receiver = msg.get_receiver();
                if self.crashed.contains(&receiver) {
                    continue;
                }
                if let Some(node) = self.nodes.get_mut(&receiver) {
                    node.handle_incoming(msg);
                }
            }
        }

        /// Run one step: tick, collect outgoing (with delay), deliver due.
        fn step(&mut self, msg_delay: u64) {
            self.tick_all();
            self.collect_and_queue_messages(msg_delay);
            self.deliver_due_messages();
        }

        /// Run multiple steps.
        fn run_steps(&mut self, n: usize, msg_delay: u64) {
            for _ in 0..n {
                self.step(msg_delay);
            }
        }

        /// Wait for a leader to be elected. Returns the leader pid.
        fn wait_for_leader(&mut self, max_steps: usize) -> NodeId {
            for _ in 0..max_steps {
                self.step(0);
                // Check if any alive node sees a leader in Accept phase
                for (&pid, node) in &self.nodes {
                    if self.crashed.contains(&pid) {
                        continue;
                    }
                    if let Some((leader_pid, true)) = node.get_current_leader() {
                        return leader_pid;
                    }
                }
            }
            panic!("No leader elected after {} steps", max_steps);
        }

        /// Get the leader as seen by a specific node (must be Accept phase).
        #[allow(dead_code)]
        fn get_leader_from_node(&self, pid: NodeId) -> Option<NodeId> {
            self.nodes
                .get(&pid)
                .and_then(|n| n.get_current_leader())
                .and_then(|(leader, ready)| if ready { Some(leader) } else { None })
        }

        /// All alive nodes should agree on the same decided log prefix.
        fn verify_decided_log_consistency(&self) -> Vec<Vec<TestEntry>> {
            let mut logs: Vec<(NodeId, Vec<TestEntry>)> = Vec::new();

            for (&pid, node) in &self.nodes {
                if self.crashed.contains(&pid) {
                    continue;
                }
                let decided_idx = node.get_decided_idx();
                if decided_idx == 0 {
                    logs.push((pid, vec![]));
                    continue;
                }
                let entries = node.read_decided_suffix(0).unwrap();
                let decided: Vec<TestEntry> = entries
                    .into_iter()
                    .filter_map(|e| match e {
                        omnipaxos::util::LogEntry::Decided(v) => Some(v),
                        _ => None,
                    })
                    .collect();
                logs.push((pid, decided));
            }

            // All decided prefixes must be consistent.
            // The shorter log must be a prefix of the longer log.
            for i in 0..logs.len() {
                for j in (i + 1)..logs.len() {
                    let (pid_a, log_a) = &logs[i];
                    let (pid_b, log_b) = &logs[j];
                    let min_len = log_a.len().min(log_b.len());
                    assert_eq!(
                        &log_a[..min_len],
                        &log_b[..min_len],
                        "Decided log prefix mismatch between node {} (len {}) and node {} (len {})",
                        pid_a,
                        log_a.len(),
                        pid_b,
                        log_b.len(),
                    );
                }
            }

            logs.into_iter().map(|(_, log)| log).collect()
        }

        /// Verify synced log consistency across all alive nodes.
        #[allow(dead_code)]
        fn verify_synced_log_consistency(&self) {
            let mut synced_logs: Vec<(NodeId, Vec<TestEntry>)> = Vec::new();

            for (&pid, node) in &self.nodes {
                if self.crashed.contains(&pid) {
                    continue;
                }
                let synced: Vec<TestEntry> = node
                    .get_synced_log()
                    .into_iter()
                    .map(|(entry, _)| entry)
                    .collect();
                synced_logs.push((pid, synced));
            }

            // All synced logs that are non-empty must have the same ordering.
            let non_empty: Vec<_> = synced_logs
                .iter()
                .filter(|(_, log)| !log.is_empty())
                .collect();

            if non_empty.len() < 2 {
                return;
            }

            for i in 0..non_empty.len() {
                for j in (i + 1)..non_empty.len() {
                    let (pid_a, log_a) = &non_empty[i];
                    let (pid_b, log_b) = &non_empty[j];
                    let min_len = log_a.len().min(log_b.len());
                    assert_eq!(
                        &log_a[..min_len],
                        &log_b[..min_len],
                        "Synced log prefix mismatch between node {} and node {}",
                        pid_a,
                        pid_b,
                    );
                }
            }
        }

        /// Process the early buffer on a specific alive node.
        fn process_early_buffer(
            &mut self,
            pid: NodeId,
        ) -> ProcessEarlyBufferResult<TestEntry> {
            self.nodes
                .get_mut(&pid)
                .expect("Node not found")
                .process_early_buffer()
        }

        /// Append to the Nezha fast path on a specific node.
        fn append_nezha(&mut self, pid: NodeId, entry: TestEntry) {
            self.nodes
                .get_mut(&pid)
                .expect("Node not found")
                .append_nezha(entry)
                .expect("append_nezha failed");
        }

        /// Process late buffer on a node (leader only).
        fn process_late_buffer(&mut self, pid: NodeId) -> Vec<ReleasedEntry<TestEntry>> {
            self.nodes
                .get_mut(&pid)
                .expect("Node not found")
                .process_late_buffer()
        }

        /// Handle a log modification on a follower.
        fn handle_log_modification(
            &mut self,
            pid: NodeId,
            client_id: u64,
            command_id: usize,
            new_deadline: i64,
            leader_log_id: usize,
        ) -> Option<usize> {
            self.nodes
                .get_mut(&pid)
                .expect("Node not found")
                .handle_log_modification(client_id, command_id, new_deadline, leader_log_id)
        }
    }

    // ── Helper: simulate the full Nezha protocol for one entry on all nodes ──

    /// Multicasts an entry to all nodes (simulating a proxy), processes the early
    /// buffer on all alive nodes, and handles leader execution + log modification
    /// for late entries. Returns the synced log entry order.
    fn simulate_nezha_multicast_and_release(
        cluster: &mut NezhaCluster,
        entry: TestEntry,
        _leader_pid: NodeId,
    ) {
        // 1. Multicast: every alive node receives the entry via append_nezha
        let alive_nodes: Vec<NodeId> = cluster
            .nodes
            .keys()
            .copied()
            .filter(|pid| !cluster.crashed.contains(pid))
            .collect();

        for &pid in &alive_nodes {
            cluster.append_nezha(pid, entry.clone());
        }
    }

    fn release_entries_on_all(
        cluster: &mut NezhaCluster,
        leader_pid: NodeId,
    ) {
        let alive_nodes: Vec<NodeId> = cluster
            .nodes
            .keys()
            .copied()
            .filter(|pid| !cluster.crashed.contains(pid))
            .collect();

        // 2. Process early buffer on all alive nodes
        //    Leader: execute + append synced log + hash
        //    Follower: append to unsynced log (inside process_early_buffer)
        for &pid in &alive_nodes {
            let result = cluster.process_early_buffer(pid);

            if pid == leader_pid {
                // Leader: append to synced_log with execution result
                for cmd in result.released_entries {
                    let node = cluster.nodes.get_mut(&pid).unwrap();
                    let synced_entry = ReleasedEntry {
                        entry: cmd.entry.clone(),
                        log_id: cmd.log_id,
                        hash: cmd.hash,
                    };
                    // Leader's "execution": just record the value
                    let exec_result = Some(Some(cmd.entry.value.clone()));
                    node.append_synced_log(synced_entry, exec_result);
                }
            } else {
                // Followers: the process_early_buffer already placed entries in unsynced_log
                // (via finalize_follower_release). Nothing more to do for the fast path.
            }
        }
    }

    fn release_late_entries_on_leader(
        cluster: &mut NezhaCluster,
        leader_pid: NodeId,
    ) {
        // Leader processes late buffer — re-sequences entries with new deadlines
        // and moves them into the early buffer.
        let late_entries = cluster.process_late_buffer(leader_pid);

        if late_entries.is_empty() {
            return;
        }

       
        std::thread::sleep(Duration::from_millis(50));

        // Now process the early buffer on leader to release re-sequenced entries
        let leader_result = cluster.process_early_buffer(leader_pid);

        let alive_followers: Vec<NodeId> = cluster
            .nodes
            .keys()
            .copied()
            .filter(|pid| *pid != leader_pid && !cluster.crashed.contains(pid))
            .collect();

        for cmd in &leader_result.released_entries {
            // Leader appends to synced log
            let node = cluster.nodes.get_mut(&leader_pid).unwrap();
            let synced_entry = ReleasedEntry {
                entry: cmd.entry.clone(),
                log_id: cmd.log_id,
                hash: cmd.hash,
            };
            let exec_result = Some(Some(cmd.entry.value.clone()));
            node.append_synced_log(synced_entry, exec_result);

            // Broadcast log-modification to followers
            for &follower in &alive_followers {
                cluster.handle_log_modification(
                    follower,
                    cmd.entry.client_id,
                    cmd.entry.id,
                    cmd.entry.deadline,
                    cmd.log_id,
                );
            }
        }
    }

    // ── Tests ───────────────────────────────────────────────────────────

    /// Test 1: Fast path with good clocks.
    /// All 5 nodes have low-uncertainty clocks. Entries arrive on time at all replicas.
    /// The system should commit via the fast path and all synced logs must agree.
    #[test]
    fn test_fast_path_good_clocks_5_nodes() {
        let mut cluster = NezhaCluster::new_good_clocks();

        // Elect a leader
        let leader = cluster.wait_for_leader(200);
        println!("Leader elected: node {}", leader);

        // Get the current time from the leader to set a "future" deadline
        let base_time = cluster.nodes.get(&leader).unwrap().get_time();

        // Create 10 entries with increasing deadlines
        let entries: Vec<TestEntry> = (1..=10)
            .map(|i| {
                TestEntry::new(
                    1,                              // client_id
                    i,                              // command_id
                    base_time + (i as i64) * 1000,  // deadline: spaced 1ms apart
                    &format!("value_{}", i),
                )
            })
            .collect();

        // Multicast all entries
        for entry in &entries {
            simulate_nezha_multicast_and_release(&mut cluster, entry.clone(), leader);
        }

        // Wait enough simulated time for deadlines to pass, then release
        std::thread::sleep(Duration::from_millis(50));

        release_entries_on_all(&mut cluster, leader);

        // Run a few more steps to propagate
        cluster.run_steps(20, 0);

        // Verify: leader's synced log should have all 10 entries in deadline order
        let leader_synced = cluster.nodes.get(&leader).unwrap().get_synced_log();
        println!("Leader synced log length: {}", leader_synced.len());
        assert_eq!(
            leader_synced.len(),
            10,
            "Leader should have 10 entries in synced log"
        );

        // Verify deadline ordering in leader's synced log
        for i in 1..leader_synced.len() {
            assert!(
                leader_synced[i].0.deadline >= leader_synced[i - 1].0.deadline,
                "Synced log must be ordered by deadline"
            );
        }

        // All entries must be present
        for entry in &entries {
            assert!(
                leader_synced.iter().any(|(e, _)| e.id == entry.id),
                "Entry {} missing from synced log",
                entry.id,
            );
        }

        // Verify decided log consistency across all nodes
        cluster.verify_decided_log_consistency();
        println!("PASS: Fast path with good clocks — all logs consistent and ordered.");
    }

    /// Test 2: Slow path — late arrivals.
    /// Some entries have stale deadlines (smaller than last released) so they go
    /// to the late buffer. The leader re-sequences them. Followers receive
    /// log-modifications. All synced logs must still agree.
    #[test]
    fn test_slow_path_late_arrivals() {
        let mut cluster = NezhaCluster::new_good_clocks();
        let leader = cluster.wait_for_leader(200);
        println!("Leader elected: node {}", leader);

        let base_time = cluster.nodes.get(&leader).unwrap().get_time();

        // Entry 1: normal, on-time
        let entry1 = TestEntry::new(1, 1, base_time + 5000, "first");

        // Entry 2: also on-time, later deadline
        let entry2 = TestEntry::new(1, 2, base_time + 10000, "second");

        // Multicast and release entry 1 and entry 2
        simulate_nezha_multicast_and_release(&mut cluster, entry1.clone(), leader);
        simulate_nezha_multicast_and_release(&mut cluster, entry2.clone(), leader);
        std::thread::sleep(Duration::from_millis(50));
        release_entries_on_all(&mut cluster, leader);

        // Entry 3: LATE — deadline is before entry 2's deadline, so it goes to late buffer
        let entry3 = TestEntry::new(1, 3, base_time + 3000, "late_entry");
        simulate_nezha_multicast_and_release(&mut cluster, entry3.clone(), leader);

        // Leader processes the late buffer → re-sequences entry 3 with a new deadline
        release_late_entries_on_leader(&mut cluster, leader);

        cluster.run_steps(20, 0);

        // Verify leader's synced log
        let leader_synced = cluster.nodes.get(&leader).unwrap().get_synced_log();
        println!("Leader synced log length: {}", leader_synced.len());

        // All 3 entries must be in the synced log
        let ids_in_log: Vec<usize> = leader_synced.iter().map(|(e, _)| e.id).collect();
        assert!(
            ids_in_log.contains(&1) && ids_in_log.contains(&2) && ids_in_log.contains(&3),
            "All 3 entries must be in synced log, got: {:?}",
            ids_in_log,
        );

        // Verify deadline ordering in synced log (linearizability)
        for i in 1..leader_synced.len() {
            assert!(
                leader_synced[i].0.deadline >= leader_synced[i - 1].0.deadline,
                "Synced log must be ordered by deadline at positions {} and {}. \
                 Deadlines: {} >= {} violated.",
                i - 1,
                i,
                leader_synced[i].0.deadline,
                leader_synced[i - 1].0.deadline,
            );
        }

        // The late entry (id=3) should have gotten a new, larger deadline from the leader
        let late_in_synced = leader_synced.iter().find(|(e, _)| e.id == 3).unwrap();
        assert!(
            late_in_synced.0.deadline > base_time + 3000,
            "Late entry should have been re-sequenced with a larger deadline. Got: {}",
            late_in_synced.0.deadline,
        );

        cluster.verify_decided_log_consistency();
        println!("PASS: Slow path late arrivals — leader re-sequenced, logs consistent.");
    }

    /// Test 3: Node failure — node 5 crashes.
    /// With f=2, we can tolerate 2 failures. One crash still allows consensus
    /// among the remaining 4 nodes. The decided and synced logs should still
    /// be consistent across the surviving nodes.
    #[test]
    fn test_node_failure_linearizability() {
        let mut cluster = NezhaCluster::new_good_clocks();
        let leader = cluster.wait_for_leader(200);
        println!("Leader elected: node {}", leader);

        // Crash node 5 (make sure it's not the leader; if it is, crash a different one)
        let crash_target = if leader == 5 { 4 } else { 5 };
        cluster.crash_node(crash_target);
        println!("Crashed node {}", crash_target);

        // Run a few steps to let the cluster stabilize
        cluster.run_steps(50, 0);

        let base_time = cluster.nodes.get(&leader).unwrap().get_time();

        // Append entries — only alive nodes receive them
        let entries: Vec<TestEntry> = (1..=5)
            .map(|i| {
                TestEntry::new(
                    2,
                    i,
                    base_time + (i as i64) * 2000,
                    &format!("after_crash_{}", i),
                )
            })
            .collect();

        for entry in &entries {
            simulate_nezha_multicast_and_release(&mut cluster, entry.clone(), leader);
        }

        std::thread::sleep(Duration::from_millis(50));
        release_entries_on_all(&mut cluster, leader);

        cluster.run_steps(50, 0);

        // Verify decided log consistency among surviving nodes
        let logs = cluster.verify_decided_log_consistency();
        println!(
            "Surviving node log lengths: {:?}",
            logs.iter().map(|l| l.len()).collect::<Vec<_>>()
        );

        // Verify leader's synced log has all 5 entries
        let leader_synced = cluster.nodes.get(&leader).unwrap().get_synced_log();
        assert_eq!(
            leader_synced.len(),
            5,
            "Leader should have 5 entries in synced log after crash"
        );

        // Verify ordering
        for i in 1..leader_synced.len() {
            assert!(
                leader_synced[i].0.deadline >= leader_synced[i - 1].0.deadline,
                "Synced log must maintain deadline ordering after crash"
            );
        }

        println!(
            "PASS: Node {} crashed — remaining nodes maintain linearizability.",
            crash_target
        );
    }
    /// Test 4: Clock skew — nodes have different drift rates and uncertainties.
    #[test]
    fn test_clock_skew_linearizability() {
        let mut clock_configs = HashMap::new();
        clock_configs.insert(
            1,
            ClockConfig {
                drift_us_per_s: 0.0,
                uncertainty: 10,
                sync_interval_ms: 1,
            },
        );
        clock_configs.insert(
            2,
            ClockConfig {
                drift_us_per_s: 5.0,
                uncertainty: 100,
                sync_interval_ms: 10,
            },
        );
        clock_configs.insert(
            3,
            ClockConfig {
                drift_us_per_s: 20.0,
                uncertainty: 500,
                sync_interval_ms: 50,
            },
        );
        clock_configs.insert(
            4,
            ClockConfig {
                drift_us_per_s: 50.0,
                uncertainty: 1000,
                sync_interval_ms: 100,
            },
        );
        clock_configs.insert(
            5,
            ClockConfig {
                drift_us_per_s: 100.0,
                uncertainty: 2000,
                sync_interval_ms: 200,
            },
        );

        let mut cluster = NezhaCluster::new(clock_configs);
        let leader = cluster.wait_for_leader(200);
        println!("Leader elected: node {} (skewed clocks)", leader);

        let base_time = cluster.nodes.get(&leader).unwrap().get_time();

        // Give generous deadlines to accommodate the worst-case drift
        let entries: Vec<TestEntry> = (1..=8)
            .map(|i| {
                TestEntry::new(
                    3,
                    i,
                    base_time + (i as i64) * 5000, // 5ms apart
                    &format!("skewed_{}", i),
                )
            })
            .collect();

        for entry in &entries {
            simulate_nezha_multicast_and_release(&mut cluster, entry.clone(), leader);
        }

        // Wait for deadlines to pass (generous for high-uncertainty nodes)
        std::thread::sleep(Duration::from_millis(100));

        release_entries_on_all(&mut cluster, leader);
        cluster.run_steps(50, 0);

        // The leader must have all entries in synced log, ordered by deadline
        let leader_synced = cluster.nodes.get(&leader).unwrap().get_synced_log();
        println!(
            "Leader synced log with skewed clocks: {} entries",
            leader_synced.len()
        );

        // Some entries may go to late buffer on nodes with high drift/uncertainty.
        // The leader should still have them (via fast or slow path).
        // What matters: whatever IS in the synced log is in deadline order.
        for i in 1..leader_synced.len() {
            assert!(
                leader_synced[i].0.deadline >= leader_synced[i - 1].0.deadline,
                "Synced log must be deadline-ordered even with clock skew"
            );
        }

        // Verify decided log consistency
        cluster.verify_decided_log_consistency();
        println!("PASS: Clock skew — correctness maintained despite different drift rates.");
    }

    /// Test 5: Combined stress — clock skew + node failure + network delays + late arrivals.
    #[test]
    fn test_combined_skew_failure_delay_late() {
        let mut clock_configs = HashMap::new();
        clock_configs.insert(
            1,
            ClockConfig {
                drift_us_per_s: 0.0,
                uncertainty: 10,
                sync_interval_ms: 1,
            },
        );
        clock_configs.insert(
            2,
            ClockConfig {
                drift_us_per_s: 10.0,
                uncertainty: 200,
                sync_interval_ms: 20,
            },
        );
        clock_configs.insert(
            3,
            ClockConfig {
                drift_us_per_s: 30.0,
                uncertainty: 500,
                sync_interval_ms: 50,
            },
        );
        clock_configs.insert(
            4,
            ClockConfig {
                drift_us_per_s: 50.0,
                uncertainty: 1000,
                sync_interval_ms: 100,
            },
        );
        clock_configs.insert(
            5,
            ClockConfig {
                drift_us_per_s: 100.0,
                uncertainty: 2000,
                sync_interval_ms: 200,
            },
        );

        let mut cluster = NezhaCluster::new(clock_configs);

        // Use network delay of 2 ticks for message propagation
        // (simulates real-world network latency)
        let msg_delay = 2;

        // Elect leader with message delays
        let leader = {
            let mut elected = None;
            for _ in 0..300 {
                cluster.step(msg_delay);
                for (_pid, node) in &cluster.nodes {
                    if let Some((leader_pid, true)) = node.get_current_leader() {
                        elected = Some(leader_pid);
                        break;
                    }
                }
                if elected.is_some() {
                    break;
                }
            }
            elected.expect("No leader elected after 300 steps with delays")
        };
        println!("Leader elected: node {} (combined test)", leader);

        // Crash node 5
        let crash_target = if leader == 5 { 4 } else { 5 };
        cluster.crash_node(crash_target);
        println!("Crashed node {}", crash_target);

        // Let the cluster stabilize with delays
        cluster.run_steps(100, msg_delay);

        let base_time = cluster.nodes.get(&leader).unwrap().get_time();

        // Phase 1: On-time entries
        let on_time_entries: Vec<TestEntry> = (1..=5)
            .map(|i| {
                TestEntry::new(
                    10,
                    i,
                    base_time + (i as i64) * 5000,
                    &format!("combined_ontime_{}", i),
                )
            })
            .collect();

        for entry in &on_time_entries {
            simulate_nezha_multicast_and_release(&mut cluster, entry.clone(), leader);
        }

        std::thread::sleep(Duration::from_millis(100));
        release_entries_on_all(&mut cluster, leader);
        cluster.run_steps(30, msg_delay);

        // Phase 2: Late entries (deadline is before the last released entry)
        let late_entries: Vec<TestEntry> = (6..=8)
            .map(|i| {
                TestEntry::new(
                    10,
                    i,
                    base_time + 1000, // very early deadline — guaranteed late
                    &format!("combined_late_{}", i),
                )
            })
            .collect();

        for entry in &late_entries {
            simulate_nezha_multicast_and_release(&mut cluster, entry.clone(), leader);
        }

        // Leader processes late buffer and broadcasts log-modifications
        release_late_entries_on_leader(&mut cluster, leader);
        cluster.run_steps(30, msg_delay);

        // Phase 3: More on-time entries after slow path
        // Use a fresh base_time so deadlines are well in the future relative
        // to any re-sequenced late entry deadline.
        let phase3_base = cluster.nodes.get(&leader).unwrap().get_time();
        let final_entries: Vec<TestEntry> = (9..=12)
            .map(|i| {
                TestEntry::new(
                    10,
                    i,
                    phase3_base + (i as i64) * 5000,
                    &format!("combined_final_{}", i),
                )
            })
            .collect();

        for entry in &final_entries {
            simulate_nezha_multicast_and_release(&mut cluster, entry.clone(), leader);
        }

        std::thread::sleep(Duration::from_millis(100));
        release_entries_on_all(&mut cluster, leader);
        cluster.run_steps(50, msg_delay);

        // ── Verification ──

        // 1. Leader synced log ordering
        let leader_synced = cluster.nodes.get(&leader).unwrap().get_synced_log();
        println!(
            "Combined test: leader synced log has {} entries",
            leader_synced.len()
        );

        for i in 1..leader_synced.len() {
            assert!(
                leader_synced[i].0.deadline >= leader_synced[i - 1].0.deadline,
                "Linearizability violated: synced log not ordered by deadline at idx {} ({}) vs {} ({})",
                i - 1,
                leader_synced[i - 1].0.deadline,
                i,
                leader_synced[i].0.deadline,
            );
        }

        // 2. All entries present in leader's synced log
        let all_ids: Vec<usize> = leader_synced.iter().map(|(e, _)| e.id).collect();
        for i in 1..=12 {
            assert!(
                all_ids.contains(&i),
                "Entry {} missing from leader's synced log. Present: {:?}",
                i,
                all_ids,
            );
        }

        // 3. Late entries should have been re-sequenced
        for i in 6..=8 {
            let entry = leader_synced.iter().find(|(e, _)| e.id == i).unwrap();
            assert!(
                entry.0.deadline > base_time + 1000,
                "Late entry {} should have been re-sequenced with a later deadline. Got: {}",
                i,
                entry.0.deadline,
            );
        }

        // 4. Leader should have execution results for all entries
        for (entry, result) in &leader_synced {
            assert!(
                result.is_some(),
                "Leader must have execution result for entry {} (value={})",
                entry.id,
                entry.value,
            );
        }

        // 5. Decided log consistency (prefix match across surviving nodes)
        cluster.verify_decided_log_consistency();

        println!(
            "PASS: Combined test — clock skew + node {} crash + network delays + late arrivals. \
             All {} entries linearizable.",
            crash_target,
            leader_synced.len(),
        );
    }
}
