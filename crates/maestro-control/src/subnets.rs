use std::net::Ipv4Addr;

const NODE_PREFIX: u8 = 24;

pub fn allocate_subnet(cluster_cidr: &str, used: &[String]) -> Option<String> {
    let (network, prefix) = parse_cidr(cluster_cidr)?;
    let base = u32::from(network) & mask(prefix);
    let available = 1u32 << (NODE_PREFIX - prefix);
    (1..available)
        .map(|index| Ipv4Addr::from(base + (index << (32 - NODE_PREFIX))))
        .map(|address| format!("{address}/{NODE_PREFIX}"))
        .find(|candidate| !used.contains(candidate))
}

fn parse_cidr(cidr: &str) -> Option<(Ipv4Addr, u8)> {
    let (address, prefix) = cidr.split_once('/')?;
    let prefix = prefix
        .parse::<u8>()
        .ok()
        .filter(|prefix| *prefix < NODE_PREFIX)?;
    Some((address.parse().ok()?, prefix))
}

fn mask(prefix: u8) -> u32 {
    u32::MAX.checked_shl(32 - u32::from(prefix)).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_the_first_unused_node_subnet() {
        assert_eq!(
            allocate_subnet("10.200.0.0/16", &[]),
            Some("10.200.1.0/24".to_string())
        );
        let used = ["10.200.1.0/24".to_string(), "10.200.2.0/24".to_string()];
        assert_eq!(
            allocate_subnet("10.200.0.0/16", &used),
            Some("10.200.3.0/24".to_string())
        );
    }

    #[test]
    fn respects_smaller_cluster_ranges() {
        let used = (1..4)
            .map(|index| format!("10.9.{index}.0/24"))
            .collect::<Vec<_>>();
        assert_eq!(allocate_subnet("10.9.0.0/22", &used), None);
        assert_eq!(
            allocate_subnet("10.9.0.0/22", &used[..2]),
            Some("10.9.3.0/24".to_string())
        );
        assert_eq!(allocate_subnet("10.9.0.0/24", &[]), None);
        assert_eq!(allocate_subnet("garbage", &[]), None);
    }
}
