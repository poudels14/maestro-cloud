use std::collections::HashMap;
use std::sync::Mutex;

use tempfile::TempDir;

use super::*;

const PRIVATE_KEY: &str = "cGxhY2Vob2xkZXItcHJpdmF0ZS1rZXktZm9yLXRlc3Q=";
const PEER_TWO: &str = "cGVlci10d28tcHVibGljLWtleS1wbGFjZWhvbGRlcjI=";
const PEER_THREE: &str = "cGVlci10aHJlZS1wdWJsaWMta2V5LXBsYWNlaG9sZGU=";
const PEER_FOUR: &str = "cGVlci1mb3VyLXB1YmxpYy1rZXktcGxhY2Vob2xkZXI=";
const LINK_QUERY: &str = "ip -j addr show dev maestro-wg";
const DUMP_QUERY: &str = "wg show maestro-wg dump";
const ROUTE_QUERY: &str = "ip -j route show dev maestro-wg";
const NAT_QUERY: &str = "nft list table ip maestro";
const CONVERGED_LINK: &str = r#"[{"ifindex":7,"ifname":"maestro-wg","flags":["POINTOPOINT","NOARP","UP","LOWER_UP"],"mtu":1420,"operstate":"UNKNOWN","addr_info":[{"family":"inet","local":"10.200.1.1","prefixlen":32,"scope":"global","label":"maestro-wg"}]}]"#;
const CONVERGED_ROUTES: &str = r#"[{"dst":"10.200.2.0/24","scope":"link","flags":[]},{"dst":"10.200.3.0/24","scope":"link","flags":[]}]"#;

struct FakeRunner {
    responses: HashMap<String, CommandOutput>,
    invocations: Mutex<Vec<(String, Option<String>)>>,
}

impl CommandRunner for FakeRunner {
    async fn run(
        &self,
        program: &str,
        args: &[String],
        stdin: Option<&str>,
    ) -> Result<CommandOutput, AgentError> {
        let command = format!("{program} {}", args.join(" "));
        self.invocations
            .lock()
            .unwrap()
            .push((command.clone(), stdin.map(str::to_string)));
        Ok(self
            .responses
            .get(&command)
            .cloned()
            .unwrap_or_else(|| succeeded("")))
    }
}

impl FakeRunner {
    fn new(responses: &[(&str, CommandOutput)]) -> Self {
        Self {
            responses: responses
                .iter()
                .map(|(command, output)| (command.to_string(), output.clone()))
                .collect(),
            invocations: Mutex::new(Vec::new()),
        }
    }

    fn mutations(&self) -> Vec<String> {
        self.invocations
            .lock()
            .unwrap()
            .iter()
            .map(|(command, _)| command.clone())
            .filter(|command| {
                ![LINK_QUERY, DUMP_QUERY, ROUTE_QUERY, NAT_QUERY].contains(&command.as_str())
            })
            .collect()
    }

    fn stdin_of(&self, command: &str) -> Option<String> {
        self.invocations
            .lock()
            .unwrap()
            .iter()
            .find(|(invoked, _)| invoked == command)
            .and_then(|(_, stdin)| stdin.clone())
    }
}

#[tokio::test]
async fn creates_mesh_from_scratch() {
    let (directory, config) = fixture(vec![
        peer(2, PEER_TWO, "203.0.113.2:51820"),
        peer(3, PEER_THREE, "203.0.113.3:51820"),
    ]);
    let mesh = WireGuardMesh::new(FakeRunner::new(&[
        (LINK_QUERY, failed("Device \"maestro-wg\" does not exist.")),
        (DUMP_QUERY, succeeded("(none)\t(none)\t0\toff\n")),
        (ROUTE_QUERY, succeeded("[]")),
        (NAT_QUERY, failed("No such file or directory")),
    ]));

    mesh.apply(&config).await.unwrap();

    let key_path = key_path(&directory);
    assert_eq!(
        mesh.runner.mutations(),
        vec![
            "ip link add maestro-wg type wireguard".to_string(),
            "ip addr add 10.200.1.1/32 dev maestro-wg".to_string(),
            format!("wg set maestro-wg listen-port 51820 private-key {key_path}"),
            format!(
                "wg set maestro-wg peer {PEER_TWO} endpoint 203.0.113.2:51820 allowed-ips 10.200.2.0/24 persistent-keepalive 25"
            ),
            format!(
                "wg set maestro-wg peer {PEER_THREE} endpoint 203.0.113.3:51820 allowed-ips 10.200.3.0/24 persistent-keepalive 25"
            ),
            "ip link set maestro-wg up".to_string(),
            "ip route replace 10.200.2.0/24 dev maestro-wg".to_string(),
            "ip route replace 10.200.3.0/24 dev maestro-wg".to_string(),
            "nft -f -".to_string(),
        ]
    );
    assert_eq!(mesh.runner.stdin_of("nft -f -"), Some(nat_ruleset(&config)));
}

#[tokio::test]
async fn reapplying_converged_mesh_changes_nothing() {
    let (_directory, config) = fixture(vec![
        peer(2, PEER_TWO, "203.0.113.2:51820"),
        peer(3, PEER_THREE, "203.0.113.3:51820"),
    ]);
    let mesh = WireGuardMesh::new(FakeRunner::new(&[
        (LINK_QUERY, succeeded(CONVERGED_LINK)),
        (
            DUMP_QUERY,
            succeeded(&dump(&[
                peer_line(PEER_TWO, "203.0.113.2:51820", "10.200.2.0/24", "25"),
                peer_line(PEER_THREE, "203.0.113.3:51820", "10.200.3.0/24", "25"),
            ])),
        ),
        (ROUTE_QUERY, succeeded(CONVERGED_ROUTES)),
        (NAT_QUERY, succeeded(&listed_nat_table(&config))),
    ]));

    mesh.apply(&config).await.unwrap();

    assert_eq!(mesh.runner.mutations(), Vec::<String>::new());
}

#[tokio::test]
async fn reconciles_added_removed_and_moved_peers() {
    let (_directory, config) = fixture(vec![
        peer(2, PEER_TWO, "198.51.100.2:51820"),
        peer(3, PEER_THREE, "203.0.113.3:51820"),
    ]);
    let mesh = WireGuardMesh::new(FakeRunner::new(&[
        (LINK_QUERY, succeeded(CONVERGED_LINK)),
        (
            DUMP_QUERY,
            succeeded(&dump(&[
                peer_line(PEER_TWO, "203.0.113.2:51820", "10.200.2.0/24", "25"),
                peer_line(PEER_FOUR, "203.0.113.4:51820", "10.200.4.0/24", "25"),
            ])),
        ),
        (
            ROUTE_QUERY,
            succeeded(
                r#"[{"dst":"10.200.2.0/24","scope":"link"},{"dst":"10.200.4.0/24","scope":"link"}]"#,
            ),
        ),
        (NAT_QUERY, succeeded(&listed_nat_table(&config))),
    ]));

    mesh.apply(&config).await.unwrap();

    assert_eq!(
        mesh.runner.mutations(),
        vec![
            format!("wg set maestro-wg peer {PEER_FOUR} remove"),
            format!(
                "wg set maestro-wg peer {PEER_TWO} endpoint 198.51.100.2:51820 allowed-ips 10.200.2.0/24 persistent-keepalive 25"
            ),
            format!(
                "wg set maestro-wg peer {PEER_THREE} endpoint 203.0.113.3:51820 allowed-ips 10.200.3.0/24 persistent-keepalive 25"
            ),
            "ip route del 10.200.4.0/24 dev maestro-wg".to_string(),
            "ip route replace 10.200.3.0/24 dev maestro-wg".to_string(),
        ]
    );
}

#[tokio::test]
async fn resets_peer_with_drifted_allowed_ips_or_keepalive() {
    let (_directory, config) = fixture(vec![
        peer(2, PEER_TWO, "203.0.113.2:51820"),
        peer(3, PEER_THREE, "203.0.113.3:51820"),
    ]);
    let mesh = WireGuardMesh::new(FakeRunner::new(&[
        (LINK_QUERY, succeeded(CONVERGED_LINK)),
        (
            DUMP_QUERY,
            succeeded(&dump(&[
                peer_line(
                    PEER_TWO,
                    "203.0.113.2:51820",
                    "10.200.2.0/24,10.200.9.0/24",
                    "25",
                ),
                peer_line(PEER_THREE, "203.0.113.3:51820", "10.200.3.0/24", "off"),
            ])),
        ),
        (ROUTE_QUERY, succeeded(CONVERGED_ROUTES)),
        (NAT_QUERY, succeeded(&listed_nat_table(&config))),
    ]));

    mesh.apply(&config).await.unwrap();

    assert_eq!(
        mesh.runner.mutations(),
        vec![
            format!(
                "wg set maestro-wg peer {PEER_TWO} endpoint 203.0.113.2:51820 allowed-ips 10.200.2.0/24 persistent-keepalive 25"
            ),
            format!(
                "wg set maestro-wg peer {PEER_THREE} endpoint 203.0.113.3:51820 allowed-ips 10.200.3.0/24 persistent-keepalive 25"
            ),
        ]
    );
}

#[tokio::test]
async fn removes_stale_routes_and_addresses() {
    let (_directory, config) = fixture(vec![peer(2, PEER_TWO, "203.0.113.2:51820")]);
    let mesh = WireGuardMesh::new(FakeRunner::new(&[
        (
            LINK_QUERY,
            succeeded(
                r#"[{"ifname":"maestro-wg","flags":["POINTOPOINT","NOARP"],"addr_info":[{"family":"inet","local":"10.200.7.1","prefixlen":32}]}]"#,
            ),
        ),
        (
            DUMP_QUERY,
            succeeded(&dump(&[peer_line(
                PEER_TWO,
                "203.0.113.2:51820",
                "10.200.2.0/24",
                "25",
            )])),
        ),
        (
            ROUTE_QUERY,
            succeeded(
                r#"[{"dst":"10.200.2.0/24","scope":"link"},{"dst":"10.200.8.0/24","scope":"link"},{"dst":"10.200.9.5","scope":"link"}]"#,
            ),
        ),
        (NAT_QUERY, succeeded(&listed_nat_table(&config))),
    ]));

    mesh.apply(&config).await.unwrap();

    assert_eq!(
        mesh.runner.mutations(),
        vec![
            "ip addr del 10.200.7.1/32 dev maestro-wg".to_string(),
            "ip addr add 10.200.1.1/32 dev maestro-wg".to_string(),
            "ip link set maestro-wg up".to_string(),
            "ip route del 10.200.8.0/24 dev maestro-wg".to_string(),
            "ip route del 10.200.9.5/32 dev maestro-wg".to_string(),
        ]
    );
}

#[tokio::test]
async fn rewrites_nat_table_when_cluster_layout_changes() {
    let (_directory, config) = fixture(Vec::new());
    let previous = MeshConfig {
        cluster_cidr: "10.100.0.0/16".to_string(),
        ..config.clone()
    };
    let mesh = WireGuardMesh::new(FakeRunner::new(&[
        (LINK_QUERY, succeeded(CONVERGED_LINK)),
        (DUMP_QUERY, succeeded(&dump(&[]))),
        (ROUTE_QUERY, succeeded("")),
        (NAT_QUERY, succeeded(&listed_nat_table(&previous))),
    ]));

    mesh.apply(&config).await.unwrap();

    assert_eq!(mesh.runner.mutations(), vec!["nft -f -".to_string()]);
    assert_eq!(mesh.runner.stdin_of("nft -f -"), Some(nat_ruleset(&config)));
}

#[tokio::test]
async fn surfaces_failed_mutations() {
    let (_directory, config) = fixture(Vec::new());
    let mesh = WireGuardMesh::new(FakeRunner::new(&[
        (LINK_QUERY, failed("does not exist")),
        (
            "ip link add maestro-wg type wireguard",
            failed("Error: Unknown device type."),
        ),
    ]));

    let error = mesh.apply(&config).await.unwrap_err();

    assert!(error.to_string().contains("Unknown device type"));
}

#[tokio::test]
async fn resets_device_when_key_or_port_drifts() {
    let (directory, config) = fixture(Vec::new());
    let mesh = WireGuardMesh::new(FakeRunner::new(&[
        (LINK_QUERY, succeeded(CONVERGED_LINK)),
        (
            DUMP_QUERY,
            succeeded(&format!("(none)\t{PEER_TWO}\t51821\toff\n")),
        ),
        (ROUTE_QUERY, succeeded("[]")),
        (NAT_QUERY, succeeded(&listed_nat_table(&config))),
    ]));

    mesh.apply(&config).await.unwrap();

    assert_eq!(
        mesh.runner.mutations(),
        vec![format!(
            "wg set maestro-wg listen-port 51820 private-key {}",
            key_path(&directory)
        )]
    );
}

#[test]
fn nat_ruleset_only_masquerades_traffic_leaving_the_cluster() {
    let (_directory, config) = fixture(Vec::new());

    assert_eq!(
        nat_ruleset(&config),
        "table ip maestro {\n}\n\
         delete table ip maestro\n\
         table ip maestro {\n\
         \tchain postrouting {\n\
         \t\ttype nat hook postrouting priority srcnat; policy accept;\n\
         \t\tip saddr 10.200.1.0/24 ip daddr != 10.200.0.0/16 oifname != \"maestro0\" masquerade comment \"maestro-mesh 10.200.1.0/24 10.200.0.0/16 maestro0\"\n\
         \t}\n\
         }\n"
    );
}

#[test]
fn parses_wg_dump() {
    let output = format!(
        "{PRIVATE_KEY}\tcHVibGlj\t51820\toff\n\
         {PEER_TWO}\t(none)\t[2001:db8::2]:51820\t10.200.2.0/24,10.200.20.5/32\t1700000000\t10\t20\t25\n\
         {PEER_THREE}\t(none)\t(none)\t(none)\t0\t0\t0\toff\n"
    );

    let state = parse_dump(&output);

    assert_eq!(state.public_key.as_deref(), Some("cHVibGlj"));
    assert_eq!(state.listen_port, 51820);
    assert_eq!(
        state.peers.get(PEER_TWO),
        Some(&PeerState {
            endpoint: Some("[2001:db8::2]:51820".parse().unwrap()),
            allowed_ips: vec!["10.200.2.0/24".to_string(), "10.200.20.5/32".to_string()],
            persistent_keepalive: Some(25),
        })
    );
    assert_eq!(state.peers.get(PEER_THREE), Some(&PeerState::default()));
}

#[test]
fn mesh_config_round_trips_through_json() {
    let (_directory, config) = fixture(vec![peer(2, PEER_TWO, "203.0.113.2:51820")]);
    let encoded = serde_json::to_string(&config).unwrap();
    assert_eq!(
        serde_json::from_str::<MeshConfig>(&encoded).unwrap(),
        config
    );
}

fn fixture(peers: Vec<MeshPeer>) -> (TempDir, MeshConfig) {
    let directory = tempfile::tempdir().unwrap();
    let private_key_path = directory.path().join("wireguard.key");
    std::fs::write(&private_key_path, format!("{PRIVATE_KEY}\n")).unwrap();
    let config = MeshConfig {
        interface: DEFAULT_INTERFACE.to_string(),
        listen_port: DEFAULT_LISTEN_PORT,
        private_key_path,
        local_subnet: "10.200.1.0/24".to_string(),
        cluster_cidr: "10.200.0.0/16".to_string(),
        bridge: "maestro0".to_string(),
        peers,
    };
    (directory, config)
}

fn peer(node_id: u64, public_key: &str, endpoint: &str) -> MeshPeer {
    MeshPeer {
        node_id,
        public_key: public_key.to_string(),
        endpoint: endpoint.parse().unwrap(),
        subnet: format!("10.200.{node_id}.0/24"),
    }
}

fn key_path(directory: &TempDir) -> String {
    directory.path().join("wireguard.key").display().to_string()
}

fn dump(peer_lines: &[String]) -> String {
    let interface_line = format!(
        "{PRIVATE_KEY}\t{}\t51820\toff",
        public_key(PRIVATE_KEY).unwrap()
    );
    std::iter::once(interface_line)
        .chain(peer_lines.iter().cloned())
        .collect::<Vec<_>>()
        .join("\n")
}

fn peer_line(public_key: &str, endpoint: &str, allowed_ips: &str, keepalive: &str) -> String {
    format!("{public_key}\t(none)\t{endpoint}\t{allowed_ips}\t1700000000\t1024\t2048\t{keepalive}")
}

fn listed_nat_table(config: &MeshConfig) -> String {
    format!(
        "table ip maestro {{\n\tchain postrouting {{\n\t\ttype nat hook postrouting priority srcnat; policy accept;\n\t\tip saddr {} ip daddr != {} oifname != \"{}\" masquerade comment \"{}\"\n\t}}\n}}\n",
        config.local_subnet,
        config.cluster_cidr,
        config.bridge,
        nat_marker(config)
    )
}

fn succeeded(stdout: &str) -> CommandOutput {
    CommandOutput {
        success: true,
        stdout: stdout.to_string(),
        stderr: String::new(),
    }
}

fn failed(stderr: &str) -> CommandOutput {
    CommandOutput {
        success: false,
        stdout: String::new(),
        stderr: stderr.to_string(),
    }
}
