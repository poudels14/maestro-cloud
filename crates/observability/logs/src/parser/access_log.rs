use std::collections::BTreeMap;
use std::net::IpAddr;

const LOG_TYPE_ATTRIBUTE: &str = "maestro.log_type";
const ACCESS_LOG_TYPE: &str = "ingress_access";
const CLIENT_IP_ATTRIBUTE: &str = "maestro.client_ip";

pub(super) fn is_traefik_access_log(object: &serde_json::Map<String, serde_json::Value>) -> bool {
    ["RequestMethod", "RequestPath", "DownstreamStatus"]
        .into_iter()
        .all(|field| find_json_field(object, field).is_some())
}

pub(super) fn sanitize_request_path(object: &mut serde_json::Map<String, serde_json::Value>) {
    let Some((_, value)) = object
        .iter_mut()
        .find(|(field, _)| field.eq_ignore_ascii_case("RequestPath"))
    else {
        return;
    };
    let Some(path) = value.as_str() else {
        return;
    };
    let path = path
        .split('?')
        .next()
        .unwrap_or("/")
        .chars()
        .take(2_048)
        .collect::<String>();
    *value = serde_json::Value::String(if path.is_empty() {
        "/".to_owned()
    } else {
        path
    });
}

pub(super) fn normalize_traefik_attributes(attributes: &mut BTreeMap<String, String>) {
    attributes.insert(LOG_TYPE_ATTRIBUTE.to_owned(), ACCESS_LOG_TYPE.to_owned());

    let direct = find_attribute(attributes, "ClientHost").and_then(parse_ip);
    let forwarded = direct
        .filter(|address| is_internal_proxy_address(*address))
        .and_then(|_| forwarded_address(attributes));
    if let Some(address) = forwarded.or(direct) {
        attributes.insert(CLIENT_IP_ATTRIBUTE.to_owned(), address.to_string());
    }
}

fn forwarded_address(attributes: &BTreeMap<String, String>) -> Option<IpAddr> {
    ["request_CF-Connecting-IP", "request_X-Real-IP"]
        .into_iter()
        .find_map(|field| find_attribute(attributes, field).and_then(parse_ip))
        .or_else(|| {
            find_attribute(attributes, "request_X-Forwarded-For")
                .and_then(|value| value.split(',').find_map(parse_ip))
        })
}

fn find_json_field<'a>(
    object: &'a serde_json::Map<String, serde_json::Value>,
    name: &str,
) -> Option<&'a serde_json::Value> {
    object
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value)
}

fn find_attribute<'a>(attributes: &'a BTreeMap<String, String>, name: &str) -> Option<&'a str> {
    attributes
        .iter()
        .find(|(candidate, _)| candidate.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn parse_ip(value: &str) -> Option<IpAddr> {
    value.trim().parse().ok()
}

fn is_internal_proxy_address(address: IpAddr) -> bool {
    match address {
        IpAddr::V4(address) => {
            const TAILSCALE_CGNAT: ipnet::Ipv4Net =
                ipnet::Ipv4Net::new_assert(std::net::Ipv4Addr::new(100, 64, 0, 0), 10);
            address.is_private()
                || address.is_loopback()
                || address.is_link_local()
                || TAILSCALE_CGNAT.contains(&address)
        }
        IpAddr::V6(address) => {
            let [first, _, _, _, _, _, _, _] = address.segments();
            address.is_loopback() || first & 0xfe00 == 0xfc00 || first & 0xffc0 == 0xfe80
        }
    }
}
