use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Errors that can occur when creating a [`ClockSimulator`].
#[derive(Debug)]
pub enum ClockSimError {
    /// The drift rate was negative.
    ///
    /// Contains the invalid drift rate.
    NegativeDriftRate(f64),
    /// The uncertainty value was negative.
    ///
    /// Contains the invalid uncertainty value in microseconds.
    NegativeUncertainty(i64),
    /// The sync interval was zero.
    ///
    /// A clock must resynchronize at a strictly positive interval.
    ZeroSyncInterval,

}

/// A clock simulator that models a synchronized clock with configurable drift and uncertainty.
pub struct ClockSimulator {
    drift_rate: f64,           // microseconds per second
    uncertainty: i64,          // microseconds
    #[allow(dead_code)]
    sync_interval: Duration,   // the configured sync interval (informational)
    last_sync_system: Instant, // monotonic reference point at last sync
    last_sync_simulated: i64,  // simulated clock reading at last sync (µs since UNIX_EPOCH)
}

impl ClockSimulator {
    /// Creates a new [`ClockSimulator`].
    pub fn new(
        drift_rate: f64,
        uncertainty: i64,
        sync_interval: Duration,
    ) -> Result<Self, ClockSimError> {
        if uncertainty < 0 {
            return Err(ClockSimError::NegativeUncertainty(uncertainty));
        }
        if sync_interval.is_zero() {
            return Err(ClockSimError::ZeroSyncInterval);
        }
        if drift_rate < 0.0 {
            return Err(ClockSimError::NegativeDriftRate(drift_rate));
        }

        // Current C_i(t) reading
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        Ok(Self {
            drift_rate,
            uncertainty,
            sync_interval,
            last_sync_system: Instant::now(),
            last_sync_simulated: now,
        })
    }

    /// Returns the current simulated time in microseconds since UNIX_EPOCH.
    pub fn get_time(&self) -> i64 {
        let elapsed_real_secs = self.last_sync_system.elapsed().as_secs_f64(); // True deltaT, real time passed (not C_i(t))
        let drift_offset = (elapsed_real_secs * self.drift_rate) as i64;
        let elapsed_real_micros = (elapsed_real_secs * 1_000_000.0) as i64;

        // Simulated time = Last Synced Time + Real Elapsed (in micros) + Drift
        self.last_sync_simulated + elapsed_real_micros + drift_offset
    }

    /// Returns the configured uncertainty window in microseconds.
    pub fn get_uncertainty(&self) -> i64 {
        self.uncertainty
    }

    /// Resynchronizes the simulated clock to current system time, resetting accumulated drift.
    /// Resets the [`ClockSimulator::last_sync_simulated`] to time since UNIX_EPOCH in microseconds.
    /// Resets the [`ClockSimulator::last_sync_system`] to Instant::now().
    pub fn sync_clock(&mut self) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        self.last_sync_simulated = now;
        self.last_sync_system = Instant::now();
    }
}

/// Clock configuration used to construct a [`ClockSimulator`].
#[derive(Clone, Debug)]
#[cfg_attr(
    any(feature = "toml_config", feature = "serde"),
    derive(serde::Deserialize)
)]
#[cfg_attr(feature = "serde", derive(serde::Serialize))]
pub struct ClockConfig {
    /// Clock drift in microseconds per second.
    pub drift_us_per_s: f64,
    /// Clock uncertainty window in microseconds.
    pub uncertainty: i64,
    /// How often the clock is synchronized, in milliseconds.
    pub sync_interval_ms: u64,
}

impl ClockConfig {
    /// Builds a [`ClockSimulator`] from this configuration.
    pub fn build(self) -> ClockSimulator {
        ClockSimulator::new(
            self.drift_us_per_s,
            self.uncertainty,
            Duration::from_millis(self.sync_interval_ms),
        )
        .expect("Invalid clock configuration")
    }

    /// Low-drift, low-uncertainty profile for well-synchronized networks.
    pub fn low() -> Self {
        Self {
            drift_us_per_s: 0.0,
            uncertainty: 100,
            sync_interval_ms: 1000,
        }
    }

    /// Medium-drift profile.
    pub fn medium() -> Self {
        Self {
            drift_us_per_s: 10.0,
            uncertainty: 1000,
            sync_interval_ms: 500,
        }
    }

    /// High-drift, high-uncertainty profile for poorly synchronized clocks.
    pub fn high() -> Self {
        Self {
            drift_us_per_s: 100.0,
            uncertainty: 10000,
            sync_interval_ms: 100,
        }
    }
}
