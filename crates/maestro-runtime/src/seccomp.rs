use serde_json::{Value, json};

// Docker's default seccomp profile (moby/profiles, Apache-2.0).
const DEFAULT_PROFILE: &str = include_str!("seccomp/default.json");

pub fn default_profile(capabilities: &[&str]) -> Value {
    let profile: Value = serde_json::from_str(DEFAULT_PROFILE)
        .unwrap_or_else(|_| unreachable!("embedded profile is valid JSON"));
    let docker_arch = docker_arch();
    let syscalls = profile["syscalls"]
        .as_array()
        .into_iter()
        .flatten()
        .filter(|rule| applies(rule, docker_arch, capabilities))
        .map(|rule| {
            let mut syscall = json!({
                "names": rule["names"],
                "action": rule["action"],
            });
            if let Some(args) = rule.get("args").filter(|args| !args.is_null()) {
                syscall["args"] = args.clone();
            }
            if let Some(errno) = rule.get("errnoRet") {
                syscall["errnoRet"] = errno.clone();
            }
            syscall
        })
        .collect::<Vec<_>>();
    json!({
        "defaultAction": profile["defaultAction"],
        "defaultErrnoRet": profile["defaultErrnoRet"],
        "architectures": architectures(&profile),
        "syscalls": syscalls,
    })
}

fn applies(rule: &Value, arch: &str, capabilities: &[&str]) -> bool {
    let listed = |section: &str, key: &str| {
        rule[section][key]
            .as_array()
            .map(|values| values.iter().filter_map(Value::as_str).collect::<Vec<_>>())
            .unwrap_or_default()
    };
    let include_arches = listed("includes", "arches");
    let include_caps = listed("includes", "caps");
    let exclude_arches = listed("excludes", "arches");
    let exclude_caps = listed("excludes", "caps");
    (include_arches.is_empty() || include_arches.contains(&arch))
        && include_caps.iter().all(|cap| capabilities.contains(cap))
        && !exclude_arches.contains(&arch)
        && !exclude_caps.iter().any(|cap| capabilities.contains(cap))
}

fn architectures(profile: &Value) -> Vec<Value> {
    let native = native_seccomp_arch();
    profile["archMap"]
        .as_array()
        .into_iter()
        .flatten()
        .find(|entry| entry["architecture"] == native)
        .map(|entry| {
            std::iter::once(entry["architecture"].clone())
                .chain(
                    entry["subArchitectures"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default(),
                )
                .collect()
        })
        .unwrap_or_default()
}

fn docker_arch() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    }
}

fn native_seccomp_arch() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "SCMP_ARCH_AARCH64",
        _ => "SCMP_ARCH_X86_64",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn allowed(profile: &Value, name: &str) -> bool {
        profile["syscalls"].as_array().unwrap().iter().any(|rule| {
            rule["action"] == "SCMP_ACT_ALLOW"
                && rule["args"].is_null()
                && rule["names"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|candidate| candidate == name)
        })
    }

    #[test]
    fn resolves_docker_rules_for_the_granted_capabilities() {
        let profile = default_profile(&["CAP_CHOWN", "CAP_NET_RAW"]);
        assert_eq!(profile["defaultAction"], "SCMP_ACT_ERRNO");
        assert!(!profile["architectures"].as_array().unwrap().is_empty());
        assert!(allowed(&profile, "read"));
        assert!(!allowed(&profile, "unshare"));
        assert!(!allowed(&profile, "mount"));
        assert!(!allowed(&profile, "bpf"));
        assert!(!allowed(&profile, "keyctl"));

        let admin = default_profile(&["CAP_SYS_ADMIN"]);
        assert!(allowed(&admin, "mount"));
    }
}
