#![doc = include_str!("../README.md")]

mod artifacts;
mod campaign;
mod db;
mod invariants;
mod minimizer;
mod retention;
mod vfs;

pub mod application {
    pub use crate::artifacts::{
        ArtifactError, decode_json_artifact, encode_json_artifact, load_json_artifact,
        save_json_artifact,
    };
    pub use crate::campaign::{
        GeneratedScenarioHarness, ReplayCapture, SeedCampaign, assert_seed_replays,
        assert_seed_variation, replay_seed, run_campaign,
    };
    pub use crate::minimizer::{MinimizationReport, ScenarioOperations, minimize_by_removal};
}

pub mod projection {
    pub use crate::application::*;
}

pub mod workflow {
    pub use crate::application::*;
}

pub mod relay {
    pub use crate::application::*;
}

pub use artifacts::*;
pub use campaign::*;
pub use db::*;
pub use invariants::*;
pub use minimizer::*;
pub use retention::*;
pub use terracedb_simulation::*;
pub use vfs::*;
