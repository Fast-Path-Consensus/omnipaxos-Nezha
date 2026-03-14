#[derive(Debug)]
pub enum ClockSimError {
    NegativeUncertainty(i64),
    ZeroSyncInterval,
    NegativeDriftRate(f64),
}

use std::time::{Duration,Instant, SystemTime, UNIX_EPOCH};

/// A clock simulator that models a synchronized clock with configurable parameters.
pub struct ClockSimulator {
    drift_rate: f64,            // microseconds per second
    uncertainty: i64,           // microseconds
    sync_interval: Duration, // the sync interval
    last_sync_system: Instant, // Monotonic reference point captured at the last sync (used to measure elapsed time).
    last_sync_simulated: i64,   // Simulated clock reading at the last sync (µs since UNIX_EPOCH).
}

impl ClockSimulator {

    pub fn new(drift_rate: f64,uncertainty: i64,sync_interval: Duration)-> Result<Self, ClockSimError>{

        if uncertainty < 0 {
            return Err(ClockSimError::NegativeUncertainty(uncertainty));
        }
        if sync_interval.is_zero() {
            return Err(ClockSimError::ZeroSyncInterval);
        }
        if drift_rate < 0.0 {
            return Err(ClockSimError::NegativeDriftRate(drift_rate));
        }

        let now = SystemTime::now() // Current C_i(t) reading
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
    /// A function C_i(t) estimating time at real time t.
    pub fn get_time(&self) -> i64{

        let elapsed_real_secs = self.last_sync_system.elapsed().as_secs_f64(); // True deltaT, real time passed (not C_i(t))

        // Calculate the drift in microseconds: (seconds elapsed * drift rate)
        let drift_offset = (elapsed_real_secs * self.drift_rate) as i64;
        let elapsed_real_micros = (elapsed_real_secs * 1_000_000.0) as i64;

        // Simulated time = Last Synced Time + Real Elapsed (in micros) + Drift
        self.last_sync_simulated + elapsed_real_micros + drift_offset
    }

    /// Returns the uncertainty of the simulated clock.
    /// Given as positive integer in microseconds.
    pub fn get_uncertainty(&self) -> i64{
        self.uncertainty
    }


    /// Resynchronizes the simulated clock to current system time.
    /// Resets the [`ClockSimulator::last_sync_simulated`] to time since UNIX_EPOCH in microseconds.
    /// Resets the [`ClockSimulator::last_sync_system`] to Instant::now().
    pub fn sync_clock(&mut self){
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        self.last_sync_simulated = now;
        self.last_sync_system = Instant::now();

    }
}

