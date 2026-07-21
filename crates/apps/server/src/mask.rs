use kernel_api::{ArtifactTemplate, SecretValue, Service, ServiceSpec};

pub(crate) fn service(mut service: Service) -> Service {
    service_spec(&mut service.spec);
    service
}

pub(crate) fn service_spec(spec: &mut ServiceSpec) {
    if let ArtifactTemplate::Build { template } = &mut spec.artifact {
        values(template.secrets.values_mut());
    }
    if let Some(secrets) = &mut spec.secrets {
        values(secrets.items.values_mut());
    }
}

fn values<'a>(values: impl Iterator<Item = &'a mut SecretValue>) {
    for value in values {
        *value = SecretValue::new(value.masked().as_str());
    }
}
