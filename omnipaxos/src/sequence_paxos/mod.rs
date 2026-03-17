use super::{ballot_leader_election::Ballot, messages::sequence_paxos::*, util::LeaderState};
#[cfg(feature = "logging")]
use crate::utils::logger::create_logger;
use crate::{
    messages::Message,
    storage::{
        internal_storage::{InternalStorage, InternalStorageConfig},
        Entry, Snapshot, StopSign, Storage,
    },
    util::{
        FlexibleQuorum, LogSync, NodeId, Quorum, SequenceNumber, READ_ERROR_MSG, WRITE_ERROR_MSG,
    },
    ClusterConfig, CompactionErr, OmniPaxosConfig, ProposeErr,
};
#[cfg(feature = "logging")]
use slog::{debug, info, trace, warn, Logger};
use std::{fmt::Debug, vec};
use crate::clock::ClockSimulator;

pub mod follower;
pub mod leader;

type FastHash = [u8; 20];

#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
struct SyncedLogEntry<T> {
    request: T,
    result: Option<Option<String>>,
}

/// Nezha state for fast-path hash verification.
/// Uses incremental XOR hashing: log_hash = XOR of SHA1(entry) for all released entries.
#[derive(Debug, Clone)]
struct NezhaState<T> {
    /// Leader's synced log with execution results (for slow path / recovery)
    synced_log: Vec<SyncedLogEntry<T>>,
    /// Incremental XOR hash of all released entries - same computation for leader and follower
    log_hash: FastHash,
}

impl<T> NezhaState<T> {
    /// Incrementally update log_hash by XORing with the hash of a new entry.
    /// This is O(1) per entry and produces identical results on all replicas
    /// that release the same entries.
    #[cfg(feature = "serde")]
    fn update_log_hash(&mut self, entry: &T)
    where
        T: serde::Serialize,
    {
        let entry_bytes = bincode::serialize(entry)
            .expect("Failed to serialize entry for hashing");
        let mut hasher = Sha1::new();
        hasher.update(&entry_bytes);
        let entry_hash: [u8; 20] = hasher.finalize().into();
        
        // XOR the entry hash into the running log hash
        xor_hash(&mut self.log_hash, &entry_hash);
    }

    fn get_log_hash(&self) -> FastHash {
        self.log_hash
    }

    fn append_synced_log(&mut self, entry: ReleasedEntry<T>, result: Option<Option<String>>) {
        self.synced_log.push(SyncedLogEntry {
            request: entry.entry,
            result,
        });
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ReleasedEntry<T> {
    pub entry: T,
    /// The index at which we modified the log.
    pub log_id: usize,
    /// A hash of the follower's current Nezha state after inserting this entry
    /// into `unsynced_log` or `synced_log`.
    ///
    /// For the follower, it is computed from the current `synced_log` and
    /// `unsynced_log` state after the insertion.
    ///
    /// This is `Some(...)` for followers, which can compute it immediately, and
    /// `None` for leaders, where it is populated later.
    pub hash: Option<FastHash>,
}


#[derive(Debug)]
pub(crate) struct ProcessEarlyBufferResult<T> {
    pub released_entries: Vec<ReleasedEntry<T>>,
    pub leader_exec_epoch: Option<Ballot>,
    pub reply_epoch: Ballot,
}
// start of slow path
/// a Sequence Paxos replica. Maintains local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
/// If snapshots are not desired to be used, use `()` for the type parameter `S`.
pub(crate) struct SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    pub(crate) internal_storage: InternalStorage<B, T>,
    pid: NodeId,
    peers: Vec<NodeId>, // excluding self pid
    state: (Role, Phase),
    buffered_proposals: Vec<T>,
    buffered_stopsign: Option<StopSign>,
    outgoing: Vec<Message<T>>,
    leader_state: LeaderState<T>,
    latest_accepted_meta: Option<(Ballot, usize)>,
    // Keeps track of sequence of accepts from leader where AcceptSync = 1
    current_seq_num: SequenceNumber,
    cached_promise_message: Option<Promise<T>>,
    #[cfg(feature = "logging")]
    logger: Logger,

    // nezha implementation
    clock: ClockSimulator,
    early_buffer: Vec<T>,
    late_buffer: Vec<T>,

    last_popped_deadline: i64,
    nezha: NezhaState<T>,

}

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    pub(crate) fn append_nezha(&mut self, entry: T) -> Result<(), ProposeErr<T>> {
        if self.accepted_reconfiguration() {
            return Err(ProposeErr::PendingReconfigEntry(entry));
        }
        self.handle_deadlined_request(entry);
        Ok(())
    }
}
// the logic for creating the ordering logic for the deadline requests of the binary heap

use crate::storage::StorageResult;

use sha1::{Digest, Sha1};
impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /*** User functions ***/
    /// Creates a Sequence Paxos replica.
    pub(crate) fn with(config: SequencePaxosConfig, storage: B, clock: ClockSimulator) -> Self {
        let pid = config.pid;
        let peers = config.peers;
        let num_nodes = &peers.len() + 1;
        let quorum = Quorum::with(config.flexible_quorum, num_nodes);
        let max_peer_pid = peers.iter().max().unwrap();
        let max_pid = *std::cmp::max(max_peer_pid, &pid) as usize;
        let mut outgoing = Vec::with_capacity(config.buffer_size);
        let (state, leader) = match storage
            .get_promise()
            .expect("storage error while trying to read promise")
        {
            // if we recover a promise from storage then we must do failure recovery
            Some(b) => {
                let state = (Role::Follower, Phase::Recover);
                for peer_pid in &peers {
                    let prepreq = PrepareReq { n: b };
                    outgoing.push(Message::SequencePaxos(PaxosMessage {
                        from: pid,
                        to: *peer_pid,
                        msg: PaxosMsg::PrepareReq(prepreq),
                    }));
                }
                (state, b)
            }
            None => ((Role::Follower, Phase::None), Ballot::default()),
        };
        let internal_storage_config = InternalStorageConfig {
            batch_size: config.batch_size,
        };
        let mut paxos = SequencePaxos {
            internal_storage: InternalStorage::with(
                storage,
                internal_storage_config,

                #[cfg(feature = "unicache")]
                pid,
            ),
            pid,
            peers,
            state,
            buffered_proposals: vec![],
            buffered_stopsign: None,
            outgoing,
            leader_state: LeaderState::<T>::with(leader, max_pid, quorum),
            latest_accepted_meta: None,
            current_seq_num: SequenceNumber::default(),
            cached_promise_message: None,
            #[cfg(feature = "logging")]
            logger: {
                if let Some(logger) = config.custom_logger {
                    logger
                } else {
                    let s = config
                        .logger_file_path
                        .unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                    create_logger(s.as_str())
                }
            },
            early_buffer: Vec::new(),
            late_buffer: Vec::new(),
            last_popped_deadline: 0,
            clock,
            nezha: NezhaState {
                synced_log: Vec::new(),
                log_hash: [0u8; 20],
            },
        };
        paxos
            .internal_storage
            .set_promise(leader)
            .expect(WRITE_ERROR_MSG);
        #[cfg(feature = "logging")]
        {
            info!(paxos.logger, "Paxos component pid: {} created!", pid);
            if let Quorum::Flexible(flex_quorum) = quorum {
                if flex_quorum.read_quorum_size > num_nodes - flex_quorum.write_quorum_size + 1 {
                    warn!(
                        paxos.logger,
                        "Unnecessary overlaps in read and write quorums. Read and Write quorums only need to be overlapping by one node i.e., read_quorum_size + write_quorum_size = num_nodes + 1");
                }
            }
        }
        paxos
    }

    pub(crate) fn get_state(&self) -> &(Role, Phase) {
        &self.state
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.internal_storage.get_promise()
    }

    /// Initiates the trim process.
    /// # Arguments
    /// * `trim_idx` - Deletes all entries up to [`trim_idx`], if the [`trim_idx`] is `None` then the minimum index accepted by **ALL** servers will be used as the [`trim_idx`].
    pub(crate) fn trim(&mut self, trim_idx: Option<usize>) -> Result<(), CompactionErr> {
        match self.state {
            (Role::Leader, _) => {
                let min_all_accepted_idx = self.leader_state.get_min_all_accepted_idx();
                let trimmed_idx = match trim_idx {
                    Some(idx) if idx <= *min_all_accepted_idx => idx,
                    None => {
                        #[cfg(feature = "logging")]
                        trace!(
                            self.logger,
                            "No trim index provided, using min_las_idx: {:?}",
                            min_all_accepted_idx
                        );
                        *min_all_accepted_idx
                    }
                    _ => {
                        return Err(CompactionErr::NotAllDecided(*min_all_accepted_idx));
                    }
                };
                let result = self.internal_storage.try_trim(trimmed_idx);
                if result.is_ok() {
                    for pid in &self.peers {
                        let msg = PaxosMsg::Compaction(Compaction::Trim(trimmed_idx));
                        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                            from: self.pid,
                            to: *pid,
                            msg,
                        }));
                    }
                }
                result.map_err(|e| {
                    *e.downcast()
                        .expect("storage error while trying to trim log")
                })
            }
            _ => Err(CompactionErr::NotCurrentLeader(self.get_current_leader())),
        }
    }

    /// Trim the log and create a snapshot. ** Note: only up to the `decided_idx` can be snapshotted **
    /// # Arguments
    /// `idx` - Snapshots all entries with index < [`idx`], if the [`idx`] is None then the decided index will be used.
    /// `local_only` - If `true`, only this server snapshots the log. If `false` all servers performs the snapshot.
    pub(crate) fn snapshot(
        &mut self,
        idx: Option<usize>,
        local_only: bool,
    ) -> Result<(), CompactionErr> {
        let result = self.internal_storage.try_snapshot(idx);
        if !local_only && result.is_ok() {
            // since it is decided, it is ok even for a follower to send this
            for pid in &self.peers {
                let msg = PaxosMsg::Compaction(Compaction::Snapshot(idx));
                self.outgoing.push(Message::SequencePaxos(PaxosMessage {
                    from: self.pid,
                    to: *pid,
                    msg,
                }));
            }
        }
        result.map_err(|e| {
            *e.downcast()
                .expect("storage error while trying to snapshot log")
        })
    }

    /// Return the decided index.
    pub(crate) fn get_decided_idx(&self) -> usize {
        self.internal_storage.get_decided_idx()
    }

    /// Return trim index from storage.
    pub(crate) fn get_compacted_idx(&self) -> usize {
        self.internal_storage.get_compacted_idx()
    }

    fn handle_compaction(&mut self, c: Compaction) {
        // try trimming and snapshotting forwarded compaction. Errors are ignored as that the data will still be kept.
        match c {
            Compaction::Trim(idx) => {
                let _ = self.internal_storage.try_trim(idx);
            }
            Compaction::Snapshot(idx) => {
                let _ = self.snapshot(idx, true);
            }
        }
    }

    /// Detects if a Prepare, Promise, AcceptStopSign, Decide of a Stopsign, or PrepareReq message
    /// has been sent but not been received. If so resends them. Note: We can't detect if a
    /// StopSign's Decide message has been received so we always resend to be safe.
    pub(crate) fn resend_message_timeout(&mut self) {
        match self.state.0 {
            Role::Leader => self.resend_messages_leader(),
            Role::Follower => self.resend_messages_follower(),
        }
    }

    /// Flushes any batched log entries and sends their corresponding Accept or Accepted messages.
    pub(crate) fn flush_batch_timeout(&mut self) {
        match self.state {
            (Role::Leader, Phase::Accept) => self.flush_batch_leader(),
            (Role::Follower, Phase::Accept) => self.flush_batch_follower(),
            _ => (),
        }
    }

    /// Moves the outgoing messages from this replica into the buffer. The messages should then be sent via the network implementation.
    /// If `buffer` is empty, it gets swapped with the internal message buffer. Otherwise, messages are appended to the buffer. This prevents messages from getting discarded.
    /// the buffer.
    pub(crate) fn take_outgoing_msgs(&mut self, buffer: &mut Vec<Message<T>>) {
        if buffer.is_empty() {
            std::mem::swap(buffer, &mut self.outgoing);
        } else {
            // User has unsent messages in their buffer, must extend their buffer.
            buffer.append(&mut self.outgoing);
        }
        self.leader_state.reset_latest_accept_meta();
        self.latest_accepted_meta = None;
    }

    /// Handle an incoming message.
    pub(crate) fn handle(&mut self, m: PaxosMessage<T>) {
        match m.msg {
            PaxosMsg::PrepareReq(prepreq) => self.handle_preparereq(prepreq, m.from),
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => match &self.state {
                (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                _ => {}
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_acceptsync(acc_sync, m.from),
            PaxosMsg::AcceptDecide(acc) => self.handle_acceptdecide(acc),
            PaxosMsg::NotAccepted(not_acc) => self.handle_notaccepted(not_acc, m.from),
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::Decide(d) => self.handle_decide(d),
            PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
            PaxosMsg::Compaction(c) => self.handle_compaction(c),
            PaxosMsg::AcceptStopSign(acc_ss) => self.handle_accept_stopsign(acc_ss),
            PaxosMsg::ForwardStopSign(f_ss) => self.handle_forwarded_stopsign(f_ss),
        }
    }

    /// Returns whether this Sequence Paxos has been reconfigured
    pub(crate) fn is_reconfigured(&self) -> Option<StopSign> {
        match self.internal_storage.get_stopsign() {
            Some(ss) if self.internal_storage.stopsign_is_decided() => Some(ss),
            _ => None,
        }
    }

    /// Returns whether this Sequence Paxos instance is stopped, i.e. if it has been reconfigured.
    fn accepted_reconfiguration(&self) -> bool {
        self.internal_storage.get_stopsign().is_some()
    }

    /// Append an entry to the replicated log.
    pub(crate) fn append(&mut self, entry: T) -> Result<(), ProposeErr<T>> {
        if self.accepted_reconfiguration() {
            Err(ProposeErr::PendingReconfigEntry(entry))
        } else {
           // self.handle(entry);
            self.propose_entry(entry);
            Ok(())
        }
    }

    /// Propose a reconfiguration. Returns an error if already stopped or `new_config` is invalid.
    /// `new_config` defines the cluster-wide configuration settings for the next cluster.
    /// `metadata` is optional data to commit alongside the reconfiguration.
    pub(crate) fn reconfigure(
        &mut self,
        new_config: ClusterConfig,
        metadata: Option<Vec<u8>>,
    ) -> Result<(), ProposeErr<T>> {
        if self.accepted_reconfiguration() {
            return Err(ProposeErr::PendingReconfigConfig(new_config, metadata));
        }
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "Accepting reconfiguration {:?}", new_config.nodes
        );
        let ss = StopSign::with(new_config, metadata);
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_stopsign = Some(ss),
            (Role::Leader, Phase::Accept) => self.accept_stopsign_leader(ss),
            _ => self.forward_stopsign(ss),
        }
        Ok(())
    }

    fn get_current_leader(&self) -> NodeId {
        self.get_promise().pid
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub(crate) fn reconnected(&mut self, pid: NodeId) {
        if pid == self.pid {
            return;
        } else if pid == self.get_current_leader() {
            self.state = (Role::Follower, Phase::Recover);
        }
        let prepreq = PrepareReq {
            n: self.get_promise(),
        };
        self.outgoing.push(Message::SequencePaxos(PaxosMessage {
            from: self.pid,
            to: pid,
            msg: PaxosMsg::PrepareReq(prepreq),
        }));
    }

    fn propose_entry(&mut self, entry: T) {
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_proposals.push(entry),
            (Role::Leader, Phase::Accept) => self.accept_entry_leader(entry),
            _ => self.forward_proposals(vec![entry]),
        }
    }

    pub(crate) fn get_leader_state(&self) -> &LeaderState<T> {
        &self.leader_state
    }

    pub(crate) fn forward_proposals(&mut self, mut entries: Vec<T>) {
        let leader = self.get_current_leader();
        if leader > 0 && self.pid != leader {
            let pf = PaxosMsg::ProposalForward(entries);
            let msg = Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: leader,
                msg: pf,
            });
            self.outgoing.push(msg);
        } else {
            self.buffered_proposals.append(&mut entries);
        }
    }

    pub(crate) fn forward_stopsign(&mut self, ss: StopSign) {
        let leader = self.get_current_leader();
        if leader > 0 && self.pid != leader {
            #[cfg(feature = "logging")]
            trace!(self.logger, "Forwarding StopSign to Leader {:?}", leader);
            let fs = PaxosMsg::ForwardStopSign(ss);
            let msg = Message::SequencePaxos(PaxosMessage {
                from: self.pid,
                to: leader,
                msg: fs,
            });
            self.outgoing.push(msg);
        } else if self.buffered_stopsign.as_mut().is_none() {
            self.buffered_stopsign = Some(ss);
        }
    }
    /// Returns `LogSync`, a struct to help other servers synchronize their log to correspond to the
    /// current state of our own log. The `common_prefix_idx` marks where in the log the other server
    /// needs to be sync from.
    fn create_log_sync(
        &self,
        common_prefix_idx: usize,
        other_logs_decided_idx: usize,
    ) -> LogSync<T> {
        let decided_idx = self.internal_storage.get_decided_idx();
        let (decided_snapshot, suffix, sync_idx) =
            if T::Snapshot::use_snapshots() && decided_idx > common_prefix_idx {
                // Note: We snapshot from the other log's decided index and not the common prefix because
                // snapshots currently only work on decided entries.
                let (delta_snapshot, compacted_idx) = self
                    .internal_storage
                    .create_diff_snapshot(other_logs_decided_idx)
                    .expect(READ_ERROR_MSG);
                let suffix = self
                    .internal_storage
                    .get_suffix(decided_idx)
                    .expect(READ_ERROR_MSG);
                (delta_snapshot, suffix, compacted_idx)
            } else {
                let suffix = self
                    .internal_storage
                    .get_suffix(common_prefix_idx)
                    .expect(READ_ERROR_MSG);
                (None, suffix, common_prefix_idx)
            };
        LogSync {
            decided_snapshot,
            suffix,
            sync_idx,
            stopsign: self.internal_storage.get_stopsign(),
        }
    }

    fn handle_deadlined_request(&mut self, d_req: T) {
        let uncertainty = self.clock.get_uncertainty();
        let d_time = d_req.deadline();

        // 1. Paper rule: "the incoming request can enter early-buffer only
        //    if its deadline is larger than the last released one from early-buffer"
        if d_time <= self.last_popped_deadline {
            self.late_buffer.push(d_req);
            return;
        }

        // 2. Linear check for conflicts within the early_buffer (uncertainty used here for ordering ambiguity)
        let has_conflict = self.early_buffer.iter().any(|buffered_msg| {
            (d_time - buffered_msg.deadline()).abs() <= uncertainty
        });

        if has_conflict {
            // Move all conflicting entries to late_buffer
            let mut i = 0;
            while i < self.early_buffer.len() {
                if (d_time - self.early_buffer[i].deadline()).abs() <= uncertainty {
                    let removed = self.early_buffer.remove(i);
                    self.late_buffer.push(removed);
                } else {
                    i += 1;
                }
            }
            self.late_buffer.push(d_req);
        } else {
            // 3. No conflict: Push and sort
            self.early_buffer.push(d_req);
            self.early_buffer.sort_by(|a, b| {
                a.deadline().cmp(&b.deadline())
                    .then_with(|| a.client_id().cmp(&b.client_id()))
                    .then_with(|| a.id().cmp(&b.id()))
            });
        }
    }


    /// Nezha Slow Path: Leader re-sequences late requests.
    /// Assigns unique deadlines larger than last released, appends to log, returns entries for LogModification broadcast.
    #[cfg(feature = "serde")]
    pub(crate) fn process_late_buffer(&mut self) -> Vec<ReleasedEntry<T>>
    where
        T: serde::Serialize,
    {
        if self.late_buffer.is_empty() {
            return Vec::new();
        }

        let current_time = self.clock.get_time();
        
        // 1. Establish baseline safe deadline (must be > last_popped_deadline)
        let mut next_ddl = std::cmp::max(current_time, self.last_popped_deadline + 1);

        // 2. Assign unique deadlines to each late entry
        let mut entries_to_append: Vec<T> = Vec::new();
        for mut entry in self.late_buffer.drain(..) {
            entry.set_deadline(next_ddl);
            entries_to_append.push(entry);
            next_ddl += 1; // Ensure each entry gets a unique deadline
        }

        // 3. Sort by deadline with paper's tie-breaker before appending
        entries_to_append.sort_by(|a, b| {
            a.deadline().cmp(&b.deadline())
                .then_with(|| a.client_id().cmp(&b.client_id()))
                .then_with(|| a.id().cmp(&b.id()))
        });

        let num_entries = entries_to_append.len();

        // 4. Append directly to log (leader is sequencing, bypass early_buffer)
        let last_log_id = self
            .append_local_only(entries_to_append.clone())
            .expect("Appending late entries to log failed!");

        let first_log_id = last_log_id - num_entries + 1;

        // 5. Update last_popped_deadline to the highest deadline we assigned
        self.last_popped_deadline = next_ddl - 1;

        // 6. Build ReleasedEntry with correct log_ids and hashes for LogModification broadcast
        let mut released_info = Vec::with_capacity(num_entries);
        for (offset, entry) in entries_to_append.into_iter().enumerate() {
            let log_id = first_log_id + offset;
            let hash = self.update_and_get_log_hash(&entry);

            released_info.push(ReleasedEntry {
                entry,
                log_id,
                hash: Some(hash),
            });
        }

        released_info
    }

    /// Follower applies a LogModification from the leader.
    /// Finds the request in late_buffer or early_buffer, updates its deadline,
    /// appends to log, and returns the hash for the slow reply.
    #[cfg(feature = "serde")]
    pub(crate) fn apply_log_modification(
        &mut self,
        client_id: u64,
        command_id: usize,
        new_deadline: i64,
        log_id: usize,
    ) -> Option<FastHash>
    where
        T: serde::Serialize,
    {
        let current_log_len = self.internal_storage.get_accepted_idx();
        
        // Check if we already have an entry at log_id position
        if log_id < current_log_len {
            // Log already has entry at this position - check for consistency
            if let Ok(entries) = self.internal_storage.get_entries(log_id, log_id + 1) {
                if let Some(existing) = entries.first() {
                    if existing.client_id() == client_id && existing.id() == command_id {
                        // Same entry already at position - idempotent, just return current hash
                        return Some(self.get_log_hash());
                    } else {
                        // Different entry at position - log divergence, skip this modification
                        #[cfg(feature = "logging")]
                        warn!(
                            self.logger,
                            "Log divergence at position {}: expected ({}, {}), found ({}, {})",
                            log_id, client_id, command_id, existing.client_id(), existing.id()
                        );
                        return None;
                    }
                }
            }
        }
        
        // Normal case: log shorter than log_id, find entry in buffers
        // 1. Try to find in late_buffer first
        let mut found_entry: Option<T> = None;
        let mut found_idx: Option<usize> = None;
        
        for (i, entry) in self.late_buffer.iter().enumerate() {
            if entry.client_id() == client_id && entry.id() == command_id {
                found_idx = Some(i);
                break;
            }
        }
        
        if let Some(idx) = found_idx {
            found_entry = Some(self.late_buffer.remove(idx));
        }
        
        // 2. If not in late_buffer, try early_buffer
        if found_entry.is_none() {
            for (i, entry) in self.early_buffer.iter().enumerate() {
                if entry.client_id() == client_id && entry.id() == command_id {
                    found_idx = Some(i);
                    break;
                }
            }
            if let Some(idx) = found_idx {
                found_entry = Some(self.early_buffer.remove(idx));
            }
        }
        
        // 3. If found, update deadline and append to log
        if let Some(mut entry) = found_entry {
            entry.set_deadline(new_deadline);
            
            // Update last_popped_deadline if this deadline is larger
            if new_deadline > self.last_popped_deadline {
                self.last_popped_deadline = new_deadline;
            }
            
            // Append to log
            let _ = self.append_local_only(vec![entry.clone()]);
            
            // Update hash and return
            let hash = self.update_and_get_log_hash(&entry);
            return Some(hash);
        }
        
        // Entry not found - may have already been processed or not yet received
        None
    }


    /// Checks the early_buffer and processes any messages whose deadlines have passed.
    /// This should be called periodically by your tick() function.
    #[cfg(feature = "serde")]
    pub(crate) fn process_early_buffer(&mut self) -> ProcessEarlyBufferResult<T>
    where
        T: serde::Serialize,
    {
        let current_time = self.clock.get_time();
        let mut entries_to_fast_append: Vec<T> = Vec::new(); // append to log
        let mut released_entries : Vec<ReleasedEntry<T>> = Vec::new();

        let promise = self.get_promise();
        let leader_exec_epoch = match self.state {
            (Role::Leader, Phase::Accept) => Some(promise),
            _ => None,
        };
        let reply_epoch = promise;

        // Use while loop to check the front of the Vec
        while !self.early_buffer.is_empty() {
            // Access the first element (the one with the earliest deadline)
            let first_deadline = self.early_buffer[0].deadline();

            // Paper rule: "It will be released at the deadline"
            if first_deadline <= current_time {
                // Remove the element from the front (index 0)
                let popped_entry = self.early_buffer.remove(0);

                // Update the last popped deadline
                self.last_popped_deadline = popped_entry.deadline();

                // Prepare for log append and return
                entries_to_fast_append.push(popped_entry.clone());

            } else {
                // Deadline not yet reached
                break;
            }
        }

        // Nothing to do
        if entries_to_fast_append.is_empty() {
            return ProcessEarlyBufferResult {
                released_entries,
                leader_exec_epoch,
                reply_epoch,
            };
        }
        let num_entries_to_fast_append = entries_to_fast_append.len();

        let last_log_id_in_full_log = self
            .append_local_only(entries_to_fast_append.clone())
            .expect("Appending to fast-path replica failed!");

        let first_log_id_from_fast_append = last_log_id_in_full_log - num_entries_to_fast_append + 1;

        // Build a ReleaseEntry for every appended element.
        for (offset, entry) in entries_to_fast_append.into_iter().enumerate() {
            let log_id = first_log_id_from_fast_append + offset; // Offset initially 0

            // Update incremental log hash for this entry (same computation for leader and follower)
            // Both store the progressive hash to ensure per-entry hash matching with proxies
            let current_hash = self.update_and_get_log_hash(&entry);

            released_entries.push(ReleasedEntry {
                entry,
                log_id,
                hash: Some(current_hash),
            });
        }

        ProcessEarlyBufferResult {
            released_entries,
            leader_exec_epoch,
            reply_epoch,
        }
    }

    
    pub(crate) fn append_local_only(&mut self, entries: Vec<T>) -> StorageResult<usize>{
        self.internal_storage.append_entries_without_batching(entries)
    }

    /// Update the incremental log hash with a new entry and return the current hash.
    /// Used by both leader and follower when releasing entries - produces identical
    /// results if they release the same entries in the same order.
    #[cfg(feature = "serde")]
    pub(crate) fn update_and_get_log_hash(&mut self, entry: &T) -> FastHash
    where
        T: serde::Serialize,
    {
        self.nezha.update_log_hash(entry);
        self.nezha.get_log_hash()
    }

    /// Get the current log hash without updating it.
    pub(crate) fn get_log_hash(&self) -> FastHash {
        self.nezha.get_log_hash()
    }

    pub(crate) fn append_synced_log(&mut self, entry: ReleasedEntry<T>, result: Option<Option<String>>) {
        self.nezha.append_synced_log(entry, result)
    }

    /// Returns the current simulated time from the shared clock, in microseconds since UNIX_EPOCH.
    pub(crate) fn get_time(&self) -> i64 {
        self.clock.get_time()
    }

    /// Returns the clock's uncertainty window in microseconds.
    pub(crate) fn get_uncertainty(&self) -> i64 {
        self.clock.get_uncertainty()
    }

    /// Resynchronizes the shared clock to the current system time.
    pub(crate) fn sync_clock(&mut self) {
        self.clock.sync_clock();
    }



    /// Returns the number of microseconds until the next message in the early_buffer expires.
    /// Returns Some(0) if a message is already expired and ready to process.
    /// Returns None if the buffer is empty.
    pub(crate) fn time_until_next_early_buffer_deadline(&self) -> Option<i64> {
        if let Some(top) = self.early_buffer.first() {
            let current_time = self.clock.get_time();

            // Paper: release at the deadline
            let wait_time = top.deadline() - current_time;

            // Use .max(0) to ensure we never return a negative sleep duration
            Some(wait_time.max(0))
        } else {
            None
        }
    }
}

fn xor_hash(a: &mut FastHash, b: &FastHash) {
    for i in 0..20 {
        a[i] ^= b[i];
    }
}

#[derive(PartialEq, Debug)]
pub(crate) enum Phase {
    Prepare,
    Accept,
    Recover,
    None,
}

#[derive(PartialEq, Debug)]
pub(crate) enum Role {
    Follower,
    Leader,
}

/// Configuration for `SequencePaxos`.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `flexible_quorum` : Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `batch_size`: The size of the buffer for log batching. The default is 1, which means no batching.
/// * `logger_file_path`: The path where the default logger logs events.
#[derive(Clone, Debug)]
pub(crate) struct SequencePaxosConfig {
    pid: NodeId,
    peers: Vec<NodeId>,
    buffer_size: usize,
    pub(crate) batch_size: usize,
    flexible_quorum: Option<FlexibleQuorum>,
    #[cfg(feature = "logging")]
    logger_file_path: Option<String>,
    #[cfg(feature = "logging")]
    custom_logger: Option<Logger>,

}

impl From<OmniPaxosConfig> for SequencePaxosConfig {
    fn from(config: OmniPaxosConfig) -> Self {
        let pid = config.server_config.pid;
        let peers = config
            .cluster_config
            .nodes
            .into_iter()
            .filter(|x| *x != pid)
            .collect();
        SequencePaxosConfig {
            pid,
            peers,
            flexible_quorum: config.cluster_config.flexible_quorum,
            buffer_size: config.server_config.buffer_size,
            batch_size: config.server_config.batch_size,
            #[cfg(feature = "logging")]
            logger_file_path: config.server_config.logger_file_path,
            #[cfg(feature = "logging")]
            custom_logger: config.server_config.custom_logger,

        }
    }
}
