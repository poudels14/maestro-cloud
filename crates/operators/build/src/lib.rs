//! Artifact build reconciliation for Maestro Build resources.
//!
//! The operator persists source resolution before invoking a potentially long
//! artifact build. Both external boundaries are injected so controller state
//! transitions remain deterministic and independently testable.

mod archive;
mod git_process;
mod local_fs;
mod local_source;
mod reconciler;
mod source;
mod watch;
mod watch_writer;
mod writer;

pub use archive::{ArtifactArchiveStore, ArtifactArchiveWrite};
pub use local_source::LocalBuildSourceProvider;
pub use reconciler::BuildReconciler;
pub use source::{
    BuildRevisionResolver, BuildSourceError, BuildSourceProvider, PreparedBuildSource,
};
pub use watch::{BuildWatchError, BuildWatchReconciler, BuildWatchSettings};

#[cfg(test)]
mod tests;
