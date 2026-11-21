use std::time::Instant;

#[derive(Debug)]
pub struct FenceGuard {
    deadline: Instant,
    generation: u64,
    fenced: bool,
}

impl FenceGuard {
    pub fn new(deadline: Instant, generation: u64) -> Self {
        Self {
            deadline,
            generation,
            fenced: false,
        }
    }

    pub fn allow_dispatch(&mut self, now: Instant, generation: u64) -> bool {
        if self.fenced {
            return false;
        }

        if now >= self.deadline || generation != self.generation {
            self.fenced = true;
            return false;
        }

        true
    }

    pub fn is_fenced(&self) -> bool {
        self.fenced
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use super::FenceGuard;

    #[test]
    fn allows_dispatch_before_deadline_with_current_generation() {
        let now = Instant::now();
        let mut guard = FenceGuard::new(now + Duration::from_secs(10), 41);

        assert!(guard.allow_dispatch(now, 41));
        assert!(!guard.is_fenced());
    }

    #[test]
    fn stale_generation_fences_permanently() {
        let now = Instant::now();
        let mut guard = FenceGuard::new(now + Duration::from_secs(10), 41);

        assert!(!guard.allow_dispatch(now, 40));
        assert!(guard.is_fenced());
        assert!(!guard.allow_dispatch(now, 41));
    }

    #[test]
    fn deadline_fences_dispatch_even_with_current_generation() {
        let now = Instant::now();
        let deadline = now + Duration::from_secs(10);
        let mut guard = FenceGuard::new(deadline, 41);

        assert!(!guard.allow_dispatch(deadline, 41));
        assert!(guard.is_fenced());
    }
}
