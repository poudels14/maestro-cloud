use kernel_api::{ArtifactTemplate, Build, SecretValue, Service, ServiceSpec, Webhook};

pub(crate) fn build(mut build: Build) -> Build {
    values(build.spec.template.secrets.values_mut());
    build
}

pub(crate) fn service(mut service: Service) -> Service {
    service_spec(&mut service.spec);
    service
}

pub(crate) fn webhook(mut webhook: Webhook) -> Webhook {
    webhook.spec.endpoint = SecretValue::new(webhook.spec.endpoint.masked().as_str());
    webhook.spec.signing_secret = webhook
        .spec
        .signing_secret
        .map(|secret| SecretValue::new(secret.masked().as_str()));
    webhook
}

pub(crate) fn service_spec(spec: &mut ServiceSpec) {
    if let ArtifactTemplate::Build { template } = &mut spec.artifact {
        values(template.secrets.values_mut());
    }
    if let Some(secrets) = &mut spec.secrets {
        values(secrets.values_mut().values_mut());
    }
}

fn values<'a>(values: impl Iterator<Item = &'a mut SecretValue>) {
    for value in values {
        *value = SecretValue::new(value.masked().as_str());
    }
}
