use crate::{
    ballot_leader_election::{Ballot, BallotLeaderElection},
    clock::{ClockConfig, ClockSimulator},
    errors::{valid_config, ConfigError},
    messages::Message,
    sequence_paxos::{Phase, SequencePaxos},
    storage::{Entry, StopSign, Storage},
    util::{
        defaults::{BUFFER_SIZE, ELECTION_TIMEOUT, FLUSH_BATCH_TIMEOUT, RESEND_MESSAGE_TIMEOUT},
        ConfigurationId, FlexibleQuorum, LogEntry, LogicalClock, NodeId,
    },
    utils::{ui, ui::ClusterState},
};
#[cfg(any(feature = "toml_config", feature = "serde"))]
use serde::Deserialize;
#[cfg(feature = "serde")]
use serde::Serialize;
#[cfg(feature = "toml_config")]
use std::fs;
use std::{
    error::Error,
    fmt::{Debug, Display},
    ops::RangeBounds,
};
#[cfg(feature = "toml_config")]
use toml;

/// Represents the hash (in SHA1) of a given log entry
pub type FastHash = [u8; 20];
/// Represents the Command ID
pub type CommandId = usize;
/// Represents the Client ID
pub type ClientId = u64;

/// Represents the released entry from the early buffer together with the index of its entry, and hash.
#[derive(Debug)]
pub struct ReleasedEntry<T> {
    /// The log entry
    pub entry: T,
    /// The log-id indicating the index of where this entry resides in the log
    pub log_id: usize,
    /// A hash of the follower's current Nezha state after inserting this entry
    /// into `unsynced_log`.
    ///
    /// For the follower, it is computed from the current `synced_log` and
    /// `unsynced_log` state after the insertion.
    ///
    /// This is `Some(...)` for followers, which can compute it immediately, and
    /// `None` for leaders, where it is populated later.
    pub hash: Option<FastHash>,
}

/// Result returned by `process_early_buffer()`.
#[derive(Debug)]
pub struct ProcessEarlyBufferResult<T> {
    /// Entries popped from the early buffer in this processing round.
    pub released_entries: Vec<ReleasedEntry<T>>,
    /// The leader ballot at processing time when this node is in `(Leader, Accept)`.
    pub leader_exec_epoch: Option<Ballot>,
    /// The ballot number
    pub reply_epoch: Ballot,
}

/// Configuration for `OmniPaxos`.
/// # Fields
/// * `cluster_config`: The configuration settings that are cluster-wide.
/// * `server_config`: The configuration settings that are specific to this OmniPaxos server.
#[allow(missing_docs)]
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "toml_config", derive(Deserialize), serde(default))]
pub struct OmniPaxosConfig {
    pub cluster_config: ClusterConfig,
    pub server_config: ServerConfig,
}

impl OmniPaxosConfig {
    /// Checks that all the fields of the cluster config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.cluster_config.validate()?;
        self.server_config.validate()?;
        valid_config!(
            self.cluster_config.nodes.contains(&self.server_config.pid),
            "Nodes must include own server pid"
        );
        Ok(())
    }

    /// Creates a new `OmniPaxosConfig` from a `toml` file.
    #[cfg(feature = "toml_config")]
    pub fn with_toml(file_path: &str) -> Result<Self, ConfigError> {
        let config_file = fs::read_to_string(file_path)?;
        let config: OmniPaxosConfig = toml::from_str(&config_file)?;
        config.validate()?;
        Ok(config)
    }

    /// Checks all configuration fields and returns the local OmniPaxos node if successful.
    /// The `clock` parameter is the single [`ClockSimulator`] instance that will be used
    /// by both the caller and the internal OmniPaxos algorithm for all time readings.
    pub fn build<T, B>(self, storage: B, clock: ClockSimulator) -> Result<OmniPaxos<T, B>, ConfigError>
    where
        T: Entry,
        B: Storage<T>,
    {
        self.validate()?;
        // Use stored ballot as initial BLE leader
        let recovered_leader = storage
            .get_promise()
            .expect("storage error while trying to read promise");
        Ok(OmniPaxos {
            ble: BallotLeaderElection::with(self.clone().into(), recovered_leader),
            election_clock: LogicalClock::with(self.server_config.election_tick_timeout),
            resend_message_clock: LogicalClock::with(
                self.server_config.resend_message_tick_timeout,
            ),
            flush_batch_clock: LogicalClock::with(self.server_config.flush_batch_tick_timeout),
            seq_paxos: SequencePaxos::with(self.into(), storage, clock),
        })
    }
}

/// Configuration for an `OmniPaxos` cluster.
/// # Fields
/// * `configuration_id`: The identifier for the cluster configuration that this OmniPaxos server is part of.
/// * `nodes`: The nodes in the cluster i.e. the `pid`s of the other servers in the configuration.
/// * `flexible_quorum` : Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
#[derive(Clone, Debug, PartialEq, Default)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "toml_config", serde(default))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ClusterConfig {
    /// The identifier for the cluster configuration that this OmniPaxos server is part of. Must
    /// not be 0 and be greater than the previous configuration's id.
    pub configuration_id: ConfigurationId,
    /// The nodes in the cluster i.e. the `pid`s of the servers in the configuration.
    pub nodes: Vec<NodeId>,
    /// Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
    pub flexible_quorum: Option<FlexibleQuorum>,
}

impl ClusterConfig {
    /// Checks that all the fields of the cluster config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let num_nodes = self.nodes.len();
        valid_config!(num_nodes > 1, "Need more than 1 node");
        valid_config!(self.configuration_id != 0, "Configuration ID cannot be 0");
        if let Some(FlexibleQuorum {
            read_quorum_size,
            write_quorum_size,
        }) = self.flexible_quorum
        {
            valid_config!(
                read_quorum_size + write_quorum_size > num_nodes,
                "The quorums must overlap i.e., the sum of their sizes must exceed the # of nodes"
            );
            valid_config!(
                read_quorum_size >= 2 && read_quorum_size <= num_nodes,
                "Read quorum must be in range 2 to # of nodes in the cluster"
            );
            valid_config!(
                write_quorum_size >= 2 && write_quorum_size <= num_nodes,
                "Write quorum must be in range 2 to # of nodes in the cluster"
            );
            valid_config!(
                read_quorum_size >= write_quorum_size,
                "Read quorum size must be >= the write quorum size."
            );
        }
        Ok(())
    }

    /// Checks all configuration fields and builds a local OmniPaxos node with settings for this
    /// node defined in `server_config` and using storage `with_storage`.
    pub fn build_for_server<T, B>(
        self,
        server_config: ServerConfig,
        with_storage: B,
    ) -> Result<OmniPaxos<T, B>, ConfigError>
    where
        T: Entry,
        B: Storage<T>,
    {
        let clock = server_config.clock.clone().build();
        let op_config = OmniPaxosConfig {
            cluster_config: self,
            server_config,
        };
        op_config.build(with_storage, clock)
    }
}

/// Configuration for a singular `OmniPaxos` instance in a cluster.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `election_tick_timeout`: The number of calls to `tick()` before leader election is updated. If this is set to 5 and `tick()` is called every 10ms, then the election timeout will be 50ms. Must not be 0.
/// * `resend_message_tick_timeout`: The number of calls to `tick()` before a message is considered dropped and thus resent. Must not be 0.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `batch_size`: The size of the buffer for log batching. The default is 1, which means no batching.
/// * `logger_file_path`: The path where the default logger logs events.
/// * `leader_priority` : Custom priority for this node to be elected as the leader.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "toml_config", derive(Deserialize), serde(default))]
pub struct ServerConfig {
    /// The unique identifier of this node. Must not be 0.
    pub pid: NodeId,
    /// The number of calls to `tick()` before leader election is updated. If this is set to 5 and `tick()` is called every 10ms, then the election timeout will be 50ms.
    pub election_tick_timeout: u64,
    /// The number of calls to `tick()` before a message is considered dropped and thus resent. Must not be 0.
    pub resend_message_tick_timeout: u64,
    /// The buffer size for outgoing messages.
    pub buffer_size: usize,
    /// The size of the buffer for log batching. The default is 1, which means no batching.
    pub batch_size: usize,
    /// The number of calls to `tick()` before the batched log entries are flushed.
    pub flush_batch_tick_timeout: u64,
    /// Custom priority for this node to be elected as the leader.
    pub leader_priority: u32,
    /// The path where the default logger logs events.
    #[cfg(feature = "logging")]
    pub logger_file_path: Option<String>,
    /// Custom logger, if provided, will be used instead of the default logger.
    #[cfg(feature = "logging")]
    #[cfg_attr(feature = "toml_config", serde(skip_deserializing))]
    pub custom_logger: Option<slog::Logger>,

    /// A clock config representing the different clock settings.
    pub clock: ClockConfig,
}


impl ServerConfig {
    /// Checks that all the fields of the server config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        valid_config!(self.pid != 0, "Server pid cannot be 0");
        valid_config!(self.buffer_size != 0, "Buffer size must be greater than 0");
        valid_config!(self.batch_size != 0, "Batch size must be greater than 0");
        valid_config!(
            self.election_tick_timeout != 0,
            "Election tick timeout must be greater than 0"
        );
        valid_config!(
            self.resend_message_tick_timeout != 0,
            "Resend message tick timeout must be greater than 0"
        );
        Ok(())
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            pid: 0,
            election_tick_timeout: ELECTION_TIMEOUT,
            resend_message_tick_timeout: RESEND_MESSAGE_TIMEOUT,
            buffer_size: BUFFER_SIZE,
            batch_size: 1,
            flush_batch_tick_timeout: FLUSH_BATCH_TIMEOUT,
            leader_priority: 0,
            #[cfg(feature = "logging")]
            logger_file_path: None,
            #[cfg(feature = "logging")]
            custom_logger: None,
            clock: ClockConfig::low(),
        }
    }
}

/// The `OmniPaxos` struct represents an OmniPaxos server. Maintains the replicated log that can be read from and appended to.
/// It also handles incoming messages and produces outgoing messages that you need to fetch and send periodically using your own network implementation.
pub struct OmniPaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    seq_paxos: SequencePaxos<T, B>,
    ble: BallotLeaderElection,
    election_clock: LogicalClock,
    resend_message_clock: LogicalClock,
    flush_batch_clock: LogicalClock,
}

impl<T, B> OmniPaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /// Returns the current simulated time from the shared clock, in microseconds since UNIX_EPOCH.
    pub fn get_time(&self) -> i64 {
        self.seq_paxos.get_time()
    }

    /// Returns the clock's uncertainty window in microseconds.
    pub fn get_uncertainty(&self) -> i64 {
        self.seq_paxos.get_uncertainty()
    }

    /// Resynchronizes the shared clock to the current system time, resetting accumulated drift.
    pub fn sync_clock(&mut self) {
        self.seq_paxos.sync_clock();
    }

    /// Recomputes and returns the current hash of the follower's synced Nezha log.
    #[cfg(feature = "serde")]
    pub fn hash_synced_log(&mut self) -> FastHash
    where
        T: serde::Serialize,
    {
        self.seq_paxos.hash_synced_log()
    }

    /// Appends an entry together with its result to the synced_log.
    pub fn append_synced_log(&mut self, entry: ReleasedEntry<T>, result: Option<Option<String>>) {
        self.seq_paxos.append_synced_log(
            crate::sequence_paxos::ReleasedEntry {
                entry: entry.entry,
                log_id: entry.log_id,
                hash: entry.hash,
            },
            result,
        );
    }

    /// Returns all entries in the committed (synced) Nezha log as (request, result) pairs.
    /// All replicas should converge to the same list if consensus is correct.
    #[cfg(feature = "serde")]
    pub fn get_synced_log(&self) -> Vec<(T, Option<Option<String>>)>
    where
        T: Clone,
    {
        self.seq_paxos.get_synced_log()
    }

}

impl<T, B> OmniPaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /// Initiates the trim process.
    /// # Arguments
    /// * `trim_index` - Deletes all entries up to [`trim_index`], if the [`trim_index`] is `None` then the minimum index accepted by **ALL** servers will be used as the [`trim_index`].
    pub fn trim(&mut self, trim_index: Option<usize>) -> Result<(), CompactionErr> {
        self.seq_paxos.trim(trim_index)
    }

    /// Trim the log and create a snapshot. ** Note: only up to the `decided_idx` can be snapshotted **
    /// # Arguments
    /// `compact_idx` - Snapshots all entries < [`compact_idx`], if the [`compact_idx`] is None then the decided index will be used.
    /// `local_only` - If `true`, only this server snapshots the log. If `false` all servers performs the snapshot.
    pub fn snapshot(
        &mut self,
        compact_idx: Option<usize>,
        local_only: bool,
    ) -> Result<(), CompactionErr> {
        self.seq_paxos.snapshot(compact_idx, local_only)
    }

    /// Return the decided index. 0 means that no entry has been decided.
    pub fn get_decided_idx(&self) -> usize {
        self.seq_paxos.get_decided_idx()
    }

    /// Return trim index from storage.
    pub fn get_compacted_idx(&self) -> usize {
        self.seq_paxos.get_compacted_idx()
    }

    /// Returns the ID of the current leader and whether the node's `Phase` is `Phase::Accepted`.
    ///
    /// If the node's phase is `Phase::Accepted`, this implies that the returned leader is also
    /// in the accepted phase. However, a `Phase::Prepare` or a `false` response does not
    /// necessarily imply that the leader is not in the accepted phase; it only reflects the current
    /// phase of this node.
    pub fn get_current_leader(&self) -> Option<(NodeId, bool)> {
        let promised_pid = self.seq_paxos.get_promise().pid;
        if promised_pid == 0 {
            None
        } else {
            let is_accepted = self.seq_paxos.get_state().1 == Phase::Accept;
            Some((promised_pid, is_accepted))
        }
    }

    /// Returns the promised ballot of this node.
    pub fn get_promise(&self) -> Ballot {
        self.seq_paxos.get_promise()
    }

    /// Moves outgoing messages from this server into the buffer. The messages should then be sent via the network implementation.
    pub fn take_outgoing_messages(&mut self, buffer: &mut Vec<Message<T>>) {
        self.seq_paxos.take_outgoing_msgs(buffer);
        buffer.extend(self.ble.outgoing_mut().drain(..).map(|b| Message::BLE(b)));
    }

    /// Read entry at index `idx` in the log. Returns `None` if `idx` is out of bounds.
    pub fn read(&self, idx: usize) -> Option<LogEntry<T>> {
        match self
            .seq_paxos
            .internal_storage
            .read(idx..idx + 1)
            .expect("storage error while trying to read log entries")
        {
            Some(mut v) => v.pop(),
            None => None,
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub fn read_entries<R>(&self, r: R) -> Option<Vec<LogEntry<T>>>
    where
        R: RangeBounds<usize>,
    {
        self.seq_paxos
            .internal_storage
            .read(r)
            .expect("storage error while trying to read log entries")
    }

    /// Read all decided entries starting at `from_idx` (inclusive) in the log. Returns `None` if `from_idx` is out of bounds.
    pub fn read_decided_suffix(&self, from_idx: usize) -> Option<Vec<LogEntry<T>>> {
        self.seq_paxos
            .internal_storage
            .read_decided_suffix(from_idx)
            .expect("storage error while trying to read decided log suffix")
    }

    /// Handle an incoming message
    pub fn handle_incoming(&mut self, m: Message<T>) {
        match m {
            Message::SequencePaxos(p) => self.seq_paxos.handle(p),
            Message::BLE(b) => self.ble.handle(b),
        }
    }

    /// Returns whether this Sequence Paxos has been reconfigured
    pub fn is_reconfigured(&self) -> Option<StopSign> {
        self.seq_paxos.is_reconfigured()
    }

    /// Checks the early_buffer and processes any messages whose deadlines have passed.
    /// This should be called periodically by your tick() function.
    #[cfg(feature = "serde")]
    pub fn process_early_buffer(&mut self) -> ProcessEarlyBufferResult<T>
    where
        T: serde::Serialize,
    {
        let internal = self.seq_paxos.process_early_buffer();
        ProcessEarlyBufferResult {
            released_entries: internal.released_entries.into_iter().map(|r| ReleasedEntry {
                entry: r.entry,
                log_id: r.log_id,
                hash: r.hash,
            }).collect(),
            leader_exec_epoch: internal.leader_exec_epoch,
            reply_epoch: internal.reply_epoch,
        }
    }

    /// Returns the number of microseconds until the next message in the early_buffer expires.
    /// Returns Some(0) if a message is already expired and ready to process.
    /// Returns None if the buffer is empty.
    pub fn time_until_next_early_buffer_deadline(&mut self) -> Option<i64> {
        self.seq_paxos.time_until_next_early_buffer_deadline()
    }

    /// Append an entry to the replicated log.
    pub fn append(&mut self, entry: T) -> Result<(), ProposeErr<T>> {
        self.seq_paxos.append(entry)
    }

    /// Tries to append an entry to the log via fast-path.
    pub fn append_nezha(&mut self, entry: T) -> Result<(), ProposeErr<T>> {
        self.seq_paxos.append_nezha(entry)
    }

    /// Nezha Slow Path: Leader re-sequences late requests
    pub fn process_late_buffer(&mut self) -> Vec<ReleasedEntry<T>> {
        let internal_entries = self.seq_paxos.process_late_buffer();
        internal_entries.into_iter().map(|r| ReleasedEntry {
            entry: r.entry,
            log_id: r.log_id,
            hash: r.hash,
        }).collect()
    }

    /// Nezha Slow Path on followers: repair log to match the leader's modification.
    #[cfg(feature = "serde")]
    pub fn handle_log_modification(
        &mut self,
        client_id: u64,
        command_id: usize,
        new_deadline: i64,
        leader_log_id: usize,
    ) -> Option<usize>
    where
        T: serde::Serialize,
    {
        self.seq_paxos.handle_log_modification(client_id, command_id, new_deadline, leader_log_id)
    }


    /// Propose a cluster reconfiguration. Returns an error if the current configuration has already been stopped
    /// by a previous reconfiguration request or if the `new_configuration` is invalid.
    /// `new_configuration` defines the cluster-wide configuration settings for the **next** cluster.
    /// `metadata` is optional data to commit alongside the reconfiguration.
    pub fn reconfigure(
        &mut self,
        new_configuration: ClusterConfig,
        metadata: Option<Vec<u8>>,
    ) -> Result<(), ProposeErr<T>> {
        if let Err(config_error) = new_configuration.validate() {
            return Err(ProposeErr::ConfigError(
                config_error,
                new_configuration,
                metadata,
            ));
        }
        self.seq_paxos.reconfigure(new_configuration, metadata)
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub fn reconnected(&mut self, pid: NodeId) {
        self.seq_paxos.reconnected(pid)
    }

    /// Increments the internal logical clock. This drives the processes for leader changes, resending dropped messages, and flushing batched log entries.
    /// Each of these is triggered every `election_tick_timeout`, `resend_message_tick_timeout`, and `flush_batch_tick_timeout` number of calls to this function
    /// (See how to configure these timeouts in `ServerConfig`).
    ///
    /// This function is called periodically to drive the Paxos algorithm's internal timers and timeouts.
    /// we want to add to it and make sure that when we get a nezha message it is handled appropriately.
    pub fn tick(&mut self) {
        if self.election_clock.tick_and_check_timeout() {
            self.election_timeout();
        }
        if self.resend_message_clock.tick_and_check_timeout() {
            self.seq_paxos.resend_message_timeout();
        }
        if self.flush_batch_clock.tick_and_check_timeout() {
            self.seq_paxos.flush_batch_timeout();
        }

    }

    /// Manually attempt to become the leader by incrementing this instance's Ballot. Calling this
    /// function may not result in gainig leadership if other instances are competing for
    /// leadership with higher Ballots.
    pub fn try_become_leader(&mut self) {
        let mut my_ballot = self.ble.get_current_ballot();
        let promise = self.seq_paxos.get_promise();
        my_ballot.n = promise.n + 1;
        self.seq_paxos.handle_leader(my_ballot);
    }

    /*** BLE calls ***/
    /// Update the custom priority used in the Ballot for this server. Note that changing the
    /// priority triggers a leader re-election.
    pub fn set_priority(&mut self, p: u32) {
        self.ble.set_priority(p)
    }

    /// If the heartbeat of a leader is not received when election_timeout() is called, the server might attempt to become the leader.
    /// It is also used for the election process, where the server checks if it can become the leader.
    /// For instance if `election_timeout()` is called every 100ms, then if the leader fails, the servers will detect it after 100ms and elect a new server after another 100ms if possible.
    fn election_timeout(&mut self) {
        if let Some(new_leader) = self
            .ble
            .hb_timeout(self.seq_paxos.get_state(), self.seq_paxos.get_promise())
        {
            self.seq_paxos.handle_leader(new_leader);
        }
    }

    /// Returns the current states of the OmniPaxos instance for OmniPaxos UI to display.
    pub fn get_ui_states(&self) -> ui::OmniPaxosStates {
        let mut cluster_state = ClusterState::from(self.seq_paxos.get_leader_state());
        cluster_state.heartbeats = self.ble.get_ballots();

        ui::OmniPaxosStates {
            current_ballot: self.ble.get_current_ballot(),
            current_leader: self.get_current_leader().map(|(leader, _)| leader),
            decided_idx: self.get_decided_idx(),
            heartbeats: self.ble.get_ballots(),
            cluster_state,
        }
    }
}

/// An error indicating a failed proposal due to the current cluster configuration being already stopped
/// or due to an invalid proposed configuration. Returns the failed proposal.
#[derive(Debug)]
pub enum ProposeErr<T>
where
    T: Entry,
{
    /// Couldn't propose entry because a reconfiguration is pending. Returns the failed, proposed entry.
    PendingReconfigEntry(T),
    /// Couldn't propose reconfiguration because a reconfiguration is already pending. Returns the failed, proposed `ClusterConfig` and the metadata.
    /// cluster config and metadata.
    PendingReconfigConfig(ClusterConfig, Option<Vec<u8>>),
    /// Couldn't propose reconfiguration because of an invalid cluster config. Contains the config
    /// error and the failed, proposed cluster config and metadata.
    ConfigError(ConfigError, ClusterConfig, Option<Vec<u8>>),
}

/// An error returning the proposal that was failed due to that the current configuration is stopped.
#[derive(Copy, Clone, Debug)]
pub enum CompactionErr {
    /// Snapshot was called with an index that is not decided yet. Returns the currently decided index.
    UndecidedIndex(usize),
    /// Snapshot was called with an index which is already trimmed. Returns the currently compacted index.
    TrimmedIndex(usize),
    /// Trim was called with an index that is not decided by all servers yet. Returns the index decided by ALL servers currently.
    NotAllDecided(usize),
    /// Trim was called at a follower node. Trim must be called by the leader, which is the returned NodeId.
    NotCurrentLeader(NodeId),
}

impl Error for CompactionErr {}
impl Display for CompactionErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}
