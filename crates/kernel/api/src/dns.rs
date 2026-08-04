/// A borrowed lowercase DNS label accepted by Maestro's canonical name policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DnsLabel<'a>(&'a str);

impl<'a> DnsLabel<'a> {
    pub fn parse(value: &'a str) -> Result<Self, DnsNameError> {
        validate_label(value, LetterCase::Lowercase)?;
        Ok(Self(value))
    }

    #[must_use]
    pub const fn as_str(self) -> &'a str {
        self.0
    }
}

/// A borrowed DNS name accepted by a declared Maestro name policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DnsName<'a>(&'a str);

impl<'a> DnsName<'a> {
    /// Parses a canonical lowercase DNS name without a trailing root dot.
    pub fn parse(value: &'a str) -> Result<Self, DnsNameError> {
        validate_name(value, LetterCase::Lowercase)?;
        Ok(Self(value))
    }

    /// Parses a DNS name case-insensitively, for compatibility with legacy state.
    pub fn parse_case_insensitive(value: &'a str) -> Result<Self, DnsNameError> {
        validate_name(value, LetterCase::Insensitive)?;
        Ok(Self(value))
    }

    #[must_use]
    pub const fn as_str(self) -> &'a str {
        self.0
    }
}

/// A borrowed canonical DNS host, optionally prefixed by one wildcard label.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WildcardDnsName<'a>(&'a str);

impl<'a> WildcardDnsName<'a> {
    pub fn parse(value: &'a str) -> Result<Self, DnsNameError> {
        if value.is_empty() || value.len() > 253 || value.ends_with('.') {
            return Err(DnsNameError::InvalidName);
        }
        let name = value.strip_prefix("*.").unwrap_or(value);
        validate_name(name, LetterCase::Lowercase)?;
        Ok(Self(value))
    }

    #[must_use]
    pub const fn as_str(self) -> &'a str {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DnsNameError {
    #[error("name must be non-empty, no longer than 253 bytes, and have no trailing dot")]
    InvalidName,
    #[error("contains an empty label")]
    EmptyLabel,
    #[error("contains a label longer than 63 bytes")]
    LabelTooLong,
    #[error("labels may contain only ASCII letters, digits, and hyphens")]
    InvalidLabelCharacter,
    #[error("labels must start and end with an ASCII letter or digit")]
    InvalidLabelBoundary,
    #[error("canonical labels must use lowercase ASCII letters")]
    UppercaseLabel,
}

#[derive(Clone, Copy)]
enum LetterCase {
    Lowercase,
    Insensitive,
}

fn validate_name(value: &str, letter_case: LetterCase) -> Result<(), DnsNameError> {
    if value.is_empty() || value.len() > 253 || value.ends_with('.') {
        return Err(DnsNameError::InvalidName);
    }
    for label in value.split('.') {
        validate_label(label, letter_case)?;
    }
    Ok(())
}

fn validate_label(value: &str, letter_case: LetterCase) -> Result<(), DnsNameError> {
    if value.is_empty() {
        return Err(DnsNameError::EmptyLabel);
    }
    if value.len() > 63 {
        return Err(DnsNameError::LabelTooLong);
    }
    if value
        .bytes()
        .any(|byte| !byte.is_ascii_alphabetic() && !byte.is_ascii_digit() && byte != b'-')
    {
        return Err(DnsNameError::InvalidLabelCharacter);
    }
    if matches!(letter_case, LetterCase::Lowercase)
        && value.bytes().any(|byte| byte.is_ascii_uppercase())
    {
        return Err(DnsNameError::UppercaseLabel);
    }
    if !value
        .as_bytes()
        .first()
        .is_some_and(u8::is_ascii_alphanumeric)
        || !value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
    {
        return Err(DnsNameError::InvalidLabelBoundary);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{DnsLabel, DnsName, WildcardDnsName};

    #[test]
    fn name_policies_are_explicit() {
        assert!(DnsLabel::parse("maestro-1").is_ok());
        assert!(DnsLabel::parse("Maestro").is_err());
        assert!(DnsName::parse("node.maestro.internal").is_ok());
        assert!(DnsName::parse("Node.maestro.internal").is_err());
        assert!(DnsName::parse_case_insensitive("Node.example").is_ok());
        assert!(WildcardDnsName::parse("*.example.com").is_ok());
        assert!(WildcardDnsName::parse("foo.*.example.com").is_err());
    }
}
