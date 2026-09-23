use core::num::NonZero;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::thread;

pub const SPIN_LIMIT: u16 = 6;

#[cfg(target_arch = "x86_64")]
pub const DEFAULT_LIMIT: u16 = 6;
#[cfg(not(target_arch = "x86_64"))]
pub const DEFAULT_LIMIT: u16 = 10;
pub const MAX_LIMIT: u16 = 10;

pub const DEFAULT_CONFIG: u32 =
    BackoffConfig { spin_limit: SPIN_LIMIT, limit: DEFAULT_LIMIT }.to_u32();

static DETECT_CONFIG: AtomicU32 = AtomicU32::new(DEFAULT_CONFIG);

static _INIT: AtomicBool = AtomicBool::new(false);

/// Detect cpu number and auto setting backoff config.
///
/// On one core system, it will be more effective (as much as 2x faster) to use yield than spinning.
///
/// The function need to be invoke manually in your initialization code, which does not interrupt
/// channel operation on other thread. By saving the result to global atomic, the effect will apply after execution.
///
/// The result we choose not to include this in default channel initialization code, because
/// Cpu detection process is somehow slow for benchmark standard,
/// and `thread::available_parallelism()` might require I/O on system files, you may not
/// like it in sandbox scenario.
pub fn detect_backoff_cfg() {
    if _INIT.compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed).is_err() {
        return;
    }
    if thread::available_parallelism().unwrap_or(NonZero::new(1).unwrap())
        == NonZero::new(1).unwrap()
    {
        // For one core (like VM machine), better use yield_now instead of spin_loop.
        DETECT_CONFIG.store(
            BackoffConfig { spin_limit: 0, limit: DEFAULT_LIMIT }.to_u32(),
            Ordering::Release,
        );
    }
}

#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BackoffConfig {
    pub spin_limit: u16,
    pub limit: u16,
}

impl Default for BackoffConfig {
    #[inline(always)]
    fn default() -> Self {
        Self::from_u32(DETECT_CONFIG.load(Ordering::Relaxed))
    }
}

impl BackoffConfig {
    #[inline(always)]
    pub fn detect() -> Self {
        Self::from_u32(DETECT_CONFIG.load(Ordering::Relaxed))
    }

    #[inline(always)]
    pub const fn to_u32(self) -> u32 {
        (self.spin_limit as u32) | ((self.limit as u32) << 16)
    }

    #[inline(always)]
    pub const fn from_u32(config: u32) -> Self {
        Self { spin_limit: config as u16, limit: (config >> 16) as u16 }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub const fn async_limit(mut self, limit: u16) -> Self {
        if limit < self.limit {
            self.limit = limit;
        }
        self.spin_limit = limit;
        self
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub const fn limit(mut self, limit: u16) -> Self {
        self.limit = limit;
        self
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub const fn spin(mut self, spin_limit: u16) -> Self {
        if spin_limit < self.spin_limit {
            self.spin_limit = spin_limit;
        }
        self
    }
}

/// Adapts the async spin-before-park to whether spinning pays off on this channel.
///
/// Spinning pays off when the counterpart runs concurrently and refills (or drains) the channel
/// within the spin, as in streaming. It is pure delay for request/response, or on a
/// single-threaded runtime, where the counterpart cannot progress until this task yields.
///
/// A miss costs one spin (about 2us), a hit saves a park and wake-up (tens of us), so spinning is
/// worth it even when only a few percent of spins hit. The high half holds a score: a hit adds
/// `HIT_SCORE`, a miss subtracts 1, and spinning stops at 0, i.e. once the hit rate stays below
/// about 1 / (HIT_SCORE + 1). While stopped, the low half counts attempts and every
/// `PROBE_INTERVAL`-th attempt spins anyway, so spinning resumes once it pays off again.
pub struct AdaptiveSpin(AtomicU32);

const HIT_SCORE: u32 = 32;
const INIT_SCORE: u32 = 64;
const MAX_SCORE: u32 = 256;
const PROBE_INTERVAL: u32 = 32;

impl AdaptiveSpin {
    #[inline]
    pub const fn new() -> Self {
        Self(AtomicU32::new(INIT_SCORE << 16))
    }

    #[inline(always)]
    pub fn should_spin(&self) -> bool {
        let v = self.0.load(Ordering::Relaxed);
        if v >> 16 > 0 {
            return true;
        }
        let count = (v + 1) & 0xffff;
        self.0.store(count, Ordering::Relaxed);
        count % PROBE_INTERVAL == 0
    }

    #[inline(always)]
    pub fn hit(&self) {
        let score = self.0.load(Ordering::Relaxed) >> 16;
        if score < MAX_SCORE {
            self.0.store((score + HIT_SCORE).min(MAX_SCORE) << 16, Ordering::Relaxed);
        }
    }

    #[inline(always)]
    pub fn miss(&self) {
        let score = self.0.load(Ordering::Relaxed) >> 16;
        if score > 0 {
            self.0.store((score - 1) << 16, Ordering::Relaxed);
        }
    }
}

pub struct Backoff {
    step: u16,
    pub config: BackoffConfig,
}

impl Backoff {
    #[inline(always)]
    pub fn new() -> Self {
        Self { step: 0, config: BackoffConfig::default() }
    }

    #[inline(always)]
    pub fn from(config: BackoffConfig) -> Self {
        Self { step: 0, config }
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn spin(&mut self) -> bool {
        for _ in 0..1 << self.step.min(SPIN_LIMIT) {
            std::hint::spin_loop();
        }
        if self.step < MAX_LIMIT {
            self.step += 1;
            self.step > self.config.limit
        } else {
            true
        }
    }

    #[inline(always)]
    pub fn set_step(&mut self, step: u16) {
        self.step = step;
    }

    #[inline(always)]
    pub fn snooze(&mut self) -> bool {
        if self.step >= self.config.limit {
            return true;
        }
        if self.step < self.config.spin_limit {
            for _ in 0..1 << self.step {
                std::hint::spin_loop();
            }
        } else {
            std::thread::yield_now();
        }
        self.step += 1;
        false
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn yield_now(&mut self) -> bool {
        if self.step >= self.config.limit {
            return true;
        }
        std::thread::yield_now();
        self.step += 1;
        false
    }

    #[inline(always)]
    pub fn is_completed(&self) -> bool {
        self.step >= self.config.limit
    }

    #[allow(dead_code)]
    #[inline(always)]
    pub fn step(&self) -> usize {
        self.step as usize
    }

    #[inline(always)]
    pub fn reset(&mut self) {
        self.step = 0;
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_adaptive_spin() {
        let spin = AdaptiveSpin::new();
        for _ in 0..INIT_SCORE {
            assert!(spin.should_spin());
            spin.miss();
        }
        // Stopped, but probes every PROBE_INTERVAL attempts
        let probes = (0..PROBE_INTERVAL * 4).filter(|_| spin.should_spin()).count();
        assert_eq!(probes, 4);
        spin.miss();
        assert!(!spin.should_spin());
        // A hit rate above 1 / (HIT_SCORE + 1) keeps spinning, and raises the score to the cap
        spin.hit();
        for _ in 0..MAX_SCORE {
            for _ in 0..HIT_SCORE - 1 {
                assert!(spin.should_spin());
                spin.miss();
            }
            spin.hit();
        }
        // The score is capped, so a switch to request/response stops spinning in bounded time
        for _ in 0..MAX_SCORE {
            assert!(spin.should_spin());
            spin.miss();
        }
        assert!(!spin.should_spin());
    }

    #[test]
    fn test_backoff() {
        let backoff = Backoff::from(BackoffConfig { spin_limit: 1, limit: 0 });
        assert!(backoff.is_completed());
        println!("Option<backoff> size {}", size_of::<Option<Backoff>>());
        println!("backoff size {}", size_of::<Backoff>());
        println!("BackoffConfig size {}", size_of::<BackoffConfig>());
        assert_eq!(size_of::<BackoffConfig>(), size_of::<u32>());
        let config = BackoffConfig { spin_limit: 6, limit: 7 };
        let config_i = config.to_u32();
        let _config = BackoffConfig::from_u32(config_i);
        assert_eq!(config.spin_limit, _config.spin_limit);
        assert_eq!(config.limit, _config.limit);

        let mut backoff = Backoff::from(BackoffConfig { spin_limit: 2, limit: 4 });
        assert_eq!(backoff.step, 0);
        backoff.spin();
        assert_eq!(backoff.step, 1);
        backoff.snooze();
        assert_eq!(backoff.step, 2);
        backoff.snooze();
        backoff.snooze();
        backoff.snooze();
        backoff.snooze();
        assert_eq!(backoff.step, 4);
        backoff.spin();
        assert_eq!(backoff.step, 5);
    }
}
