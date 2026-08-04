/// A borrowed environment-variable name that has passed Maestro's portable name policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct EnvironmentName<'a>(&'a str);

impl<'a> EnvironmentName<'a> {
    pub fn parse(value: &'a str) -> Result<Self, InvalidEnvironmentName> {
        let mut bytes = value.bytes();
        if bytes
            .next()
            .is_some_and(|byte| byte == b'_' || byte.is_ascii_alphabetic())
            && bytes.all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
        {
            Ok(Self(value))
        } else {
            Err(InvalidEnvironmentName)
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'a str {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("environment name must match [A-Za-z_][A-Za-z0-9_]*")]
pub struct InvalidEnvironmentName;

#[cfg(test)]
mod tests {
    use super::EnvironmentName;

    #[test]
    fn validates_portable_environment_names() {
        for valid in ["A", "_", "MAESTRO_TOKEN", "value2"] {
            assert!(EnvironmentName::parse(valid).is_ok());
        }
        for invalid in ["", "2VALUE", "WITH-DASH", "NON_ASCII_é"] {
            assert!(EnvironmentName::parse(invalid).is_err());
        }
    }
}
