use rcaster_core::protocol::{
    AdmissionPolicy, AdmissionProgress, AdmissionViolation, ProtocolDemux,
};
use std::time::Duration;

#[derive(Clone, Debug)]
pub struct SourceAdmissionConfig {
    pub policy: AdmissionPolicy,
    pub max_bytes: u64,
    pub max_frames: u64,
    pub timeout: Duration,
    pub read_timeout: Duration,
}

pub struct SourceAdmissionWindow {
    demux: ProtocolDemux,
    policy: AdmissionPolicy,
    max_bytes: u64,
    max_frames: u64,
    accepted_bytes: Vec<u8>,
    outcome: Option<WindowOutcome>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WindowOutcome {
    Pending,
    Accepted {
        bytes: Vec<u8>,
        warnings: Vec<AdmissionViolation>,
    },
    Rejected {
        violations: Vec<AdmissionViolation>,
    },
}

impl SourceAdmissionWindow {
    pub fn new(policy: AdmissionPolicy, max_bytes: u64, max_frames: u64) -> Self {
        Self {
            demux: ProtocolDemux::new(),
            policy,
            max_bytes,
            max_frames,
            accepted_bytes: Vec::new(),
            outcome: None,
        }
    }

    pub fn push(&mut self, input: &[u8]) -> WindowOutcome {
        if let Some(outcome) = &self.outcome {
            return outcome.clone();
        }
        let frames = self.demux.push(input);
        for frame in frames {
            self.accepted_bytes.extend_from_slice(&frame.bytes);
        }
        let profile = self.demux.profile();
        match self.policy.evaluate(profile) {
            AdmissionProgress::Pending => {
                if profile.total_bytes >= self.max_bytes || profile.valid_frames >= self.max_frames
                {
                    self.finish()
                } else {
                    WindowOutcome::Pending
                }
            }
            AdmissionProgress::Accepted { warnings } => {
                let outcome = WindowOutcome::Accepted {
                    bytes: std::mem::take(&mut self.accepted_bytes),
                    warnings,
                };
                self.outcome = Some(outcome.clone());
                outcome
            }
            AdmissionProgress::Rejected { violations } => {
                let waiting_for_correction = self.policy.require_correction_data
                    && !profile.has_correction_data
                    && violations.iter().all(|violation| {
                        matches!(
                            violation,
                            AdmissionViolation::MissingCorrectionData
                                | AdmissionViolation::NoObservationOrSsr
                                | AdmissionViolation::ExpectedFormatMismatch(_)
                        )
                    });
                if waiting_for_correction
                    && profile.total_bytes < self.max_bytes
                    && profile.valid_frames < self.max_frames
                {
                    return WindowOutcome::Pending;
                }
                let outcome = WindowOutcome::Rejected { violations };
                self.outcome = Some(outcome.clone());
                outcome
            }
        }
    }

    pub fn finish(&mut self) -> WindowOutcome {
        if let Some(outcome) = &self.outcome {
            return outcome.clone();
        }
        let profile = self.demux.profile();
        let outcome = match self.policy.evaluate(profile) {
            AdmissionProgress::Accepted { warnings } => WindowOutcome::Accepted {
                bytes: std::mem::take(&mut self.accepted_bytes),
                warnings,
            },
            AdmissionProgress::Rejected { violations } => WindowOutcome::Rejected { violations },
            AdmissionProgress::Pending => WindowOutcome::Rejected {
                violations: vec![AdmissionViolation::NoValidFrames],
            },
        };
        self.outcome = Some(outcome.clone());
        outcome
    }

    pub fn profile(&self) -> &rcaster_core::protocol::DataProfile {
        self.demux.profile()
    }

    pub fn take_demux(&mut self) -> ProtocolDemux {
        std::mem::take(&mut self.demux)
    }
}
