use crate::deployment::types::ServiceDeployment;

pub trait ServiceCommandPlanner: Send + Sync {
    fn build(&self, deployment: &ServiceDeployment) -> Option<String>;
    fn deploy(&self, deployment: &ServiceDeployment) -> Option<String>;
}

pub struct DockerDeploymentProvider;
pub struct ShellDeploymentProvider;

impl ServiceCommandPlanner for DockerDeploymentProvider {
    fn build(&self, deployment: &ServiceDeployment) -> Option<String> {
        let build = deployment.config.build.as_ref()?;
        Some(format!(
            "git clone {} && docker build -f {} .",
            build.repo, build.dockerfile_path
        ))
    }

    fn deploy(&self, deployment: &ServiceDeployment) -> Option<String> {
        if let Some(image) = deployment
            .config
            .image
            .as_deref()
            .map(str::trim)
            .filter(|image| !image.is_empty())
        {
            Some(docker_run_command(
                &deployment.config.id,
                &deployment.id,
                image,
                &deployment.config.deploy.ports,
                &deployment.config.deploy.flags,
            ))
        } else if let Some(deploy_command) = deployment.config.deploy.command.as_ref() {
            Some(to_shell_command(
                &deploy_command.command,
                &deploy_command.args,
            ))
        } else {
            None
        }
    }
}

impl ServiceCommandPlanner for ShellDeploymentProvider {
    fn build(&self, _deployment: &ServiceDeployment) -> Option<String> {
        None
    }

    fn deploy(&self, deployment: &ServiceDeployment) -> Option<String> {
        let deploy_command = deployment.config.deploy.command.as_ref()?;
        let command = deploy_command.command.trim();
        if command.is_empty() {
            return None;
        }
        Some(to_shell_command(command, &deploy_command.args))
    }
}

fn to_shell_command(command: &str, args: &[String]) -> String {
    if args.is_empty() {
        command.to_string()
    } else {
        format!("{} {}", command, args.join(" "))
    }
}

fn docker_run_command(
    service_id: &str,
    deployment_id: &str,
    image: &str,
    ports: &[String],
    flags: &[String],
) -> String {
    let short_deployment_id = deployment_id.chars().take(6).collect::<String>();
    let container_name = format!("{service_id}-{short_deployment_id}");
    let docker_port_flags = ports
        .iter()
        .map(|port| format!("-p {port}"))
        .collect::<Vec<_>>();

    if docker_port_flags.is_empty() && flags.is_empty() {
        format!("exec docker run --rm --name {container_name} {image}")
    } else if flags.is_empty() {
        format!(
            "exec docker run --rm --name {container_name} {} {image}",
            docker_port_flags.join(" ")
        )
    } else if docker_port_flags.is_empty() {
        format!(
            "exec docker run --rm --name {container_name} {image} {}",
            flags.join(" ")
        )
    } else {
        format!(
            "exec docker run --rm --name {container_name} {} {image} {}",
            docker_port_flags.join(" "),
            flags.join(" "),
        )
    }
}
