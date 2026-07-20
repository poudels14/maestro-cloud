use async_trait::async_trait;

use crate::{DaemonError, DaemonPlan, DaemonRole, RoleError, RoleFailure, RoleSpec};

/// Owned lifetime of one started daemon role.
#[async_trait]
pub trait RoleRuntime: Send {
    /// Stops all role-owned work and waits for completion.
    ///
    /// Dropping a runtime without calling this method must still cancel or
    /// terminate every task, process, listener, and lease it owns.
    async fn shutdown(self: Box<Self>) -> Result<(), RoleError>;
}

/// Side-effect boundary that starts concrete controller and agent roles.
#[async_trait]
pub trait RoleFactory: Send + Sync {
    /// Starts one exact role specification and transfers lifetime ownership.
    async fn start(
        &self,
        plan: &DaemonPlan,
        spec: &RoleSpec,
    ) -> Result<Box<dyn RoleRuntime>, RoleError>;
}

/// Composition root that realizes one validated daemon plan.
pub struct Daemon<Factory> {
    plan: DaemonPlan,
    factory: Factory,
}

impl<Factory> Daemon<Factory>
where
    Factory: RoleFactory,
{
    /// Binds a validated plan to its production or test role factory.
    pub fn new(plan: DaemonPlan, factory: Factory) -> Self {
        Self { plan, factory }
    }

    /// Starts roles in plan order and rolls earlier roles back on failure.
    pub async fn start(self) -> Result<RunningDaemon, DaemonError> {
        let mut roles = Vec::with_capacity(self.plan.roles().len());
        for spec in self.plan.roles() {
            match self.factory.start(&self.plan, spec).await {
                Ok(runtime) => roles.push(ActiveRole {
                    role: spec.role,
                    runtime,
                }),
                Err(error) => {
                    let rollback_failures = shutdown_roles(&mut roles).await;
                    return Err(DaemonError::Startup {
                        role: spec.role,
                        error,
                        rollback_failures,
                    });
                }
            }
        }
        Ok(RunningDaemon {
            plan: self.plan,
            roles,
        })
    }
}

/// Fully started daemon whose value owns every role lifetime.
pub struct RunningDaemon {
    plan: DaemonPlan,
    roles: Vec<ActiveRole>,
}

impl RunningDaemon {
    /// Returns the immutable plan used to start this instance.
    pub fn plan(&self) -> &DaemonPlan {
        &self.plan
    }

    /// Returns active roles in their original startup order.
    pub fn active_roles(&self) -> Vec<DaemonRole> {
        self.roles.iter().map(|active| active.role).collect()
    }

    /// Stops roles in reverse order and attempts every shutdown after errors.
    pub async fn shutdown(mut self) -> Result<(), DaemonError> {
        let failures = shutdown_roles(&mut self.roles).await;
        if failures.is_empty() {
            Ok(())
        } else {
            Err(DaemonError::Shutdown { failures })
        }
    }
}

struct ActiveRole {
    role: DaemonRole,
    runtime: Box<dyn RoleRuntime>,
}

async fn shutdown_roles(roles: &mut Vec<ActiveRole>) -> Vec<RoleFailure> {
    let mut failures = Vec::new();
    while let Some(active) = roles.pop() {
        if let Err(error) = active.runtime.shutdown().await {
            failures.push(RoleFailure {
                role: active.role,
                error,
            });
        }
    }
    failures
}
