use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, Ipv6Addr};

use kernel_api::{
    DnsRecord, DnsRecordId, DnsRecordSpec, DnsRecordStatus, DnsRecordValue, Generation, ObjectMeta,
    ResourceRevision,
};

use crate::{
    AuthoritativeDnsResolver, DnsQueryType, DnsResolverError, DnsResponseCode, DnsZoneSummary,
};

#[tokio::test]
async fn authoritative_zone_record_sets_match_the_reviewed_contract()
-> Result<(), Box<dyn std::error::Error>> {
    let resolver = AuthoritativeDnsResolver::new()?;
    let resources = vec![
        record(
            "api-v4",
            "api.maestro.internal.",
            vec![
                DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 12)),
                DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 11)),
            ],
        ),
        record(
            "api-v6",
            "API.maestro.internal.",
            vec![DnsRecordValue::Aaaa("fd00:42::11".parse::<Ipv6Addr>()?)],
        ),
        record(
            "alias",
            "alias.maestro.internal.",
            vec![DnsRecordValue::Cname("API.Maestro.Internal.".to_owned())],
        ),
        record(
            "metadata",
            "metadata.maestro.internal.",
            vec![DnsRecordValue::Txt("environment=production".to_owned())],
        ),
        record(
            "http",
            "_http._tcp.maestro.internal.",
            vec![
                DnsRecordValue::Srv {
                    priority: 10,
                    weight: 20,
                    port: 8081,
                    target: "api.maestro.internal.".to_owned(),
                },
                DnsRecordValue::Srv {
                    priority: 10,
                    weight: 10,
                    port: 8080,
                    target: "API.maestro.internal.".to_owned(),
                },
            ],
        ),
    ];

    assert_eq!(
        resolver.replace(&resources).await?,
        DnsZoneSummary {
            record_sets: 5,
            records: 7,
        }
    );
    let lookups = BTreeMap::from([
        (
            "aliasA",
            resolver
                .lookup("alias.maestro.internal.", DnsQueryType::A)
                .await?,
        ),
        (
            "apiA",
            resolver
                .lookup("api.maestro.internal.", DnsQueryType::A)
                .await?,
        ),
        (
            "apiAaaa",
            resolver
                .lookup("api.maestro.internal.", DnsQueryType::Aaaa)
                .await?,
        ),
        (
            "apiAny",
            resolver
                .lookup("api.maestro.internal.", DnsQueryType::Any)
                .await?,
        ),
        (
            "metadataTxt",
            resolver
                .lookup("metadata.maestro.internal.", DnsQueryType::Txt)
                .await?,
        ),
        (
            "serviceSrv",
            resolver
                .lookup("_http._tcp.maestro.internal.", DnsQueryType::Srv)
                .await?,
        ),
    ]);

    insta::assert_json_snapshot!(lookups);
    Ok(())
}

#[tokio::test]
async fn authoritative_zone_distinguishes_refused_name_error_and_no_data()
-> Result<(), Box<dyn std::error::Error>> {
    let resolver = AuthoritativeDnsResolver::new()?;
    resolver
        .replace(&[record(
            "api",
            "api.maestro.internal.",
            vec![DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 11))],
        )])
        .await?;

    let missing = resolver
        .lookup("missing.maestro.internal.", DnsQueryType::A)
        .await?;
    assert!(missing.authoritative);
    assert_eq!(missing.response_code, DnsResponseCode::NameError);
    let apex = resolver
        .lookup("maestro.internal.", DnsQueryType::A)
        .await?;
    assert!(apex.authoritative);
    assert_eq!(apex.response_code, DnsResponseCode::NoError);
    let no_data = resolver
        .lookup("api.maestro.internal.", DnsQueryType::Txt)
        .await?;
    assert_eq!(no_data.response_code, DnsResponseCode::NoError);
    assert!(no_data.answers.is_empty());
    let outside = resolver.lookup("example.com.", DnsQueryType::A).await?;
    assert!(!outside.authoritative);
    assert_eq!(outside.response_code, DnsResponseCode::Refused);
    assert!(resolver.lookup("relative", DnsQueryType::A).await.is_err());
    Ok(())
}

#[tokio::test]
async fn invalid_snapshots_do_not_replace_the_last_valid_zone()
-> Result<(), Box<dyn std::error::Error>> {
    let resolver = AuthoritativeDnsResolver::new()?;
    resolver
        .replace(&[record(
            "api",
            "api.maestro.internal.",
            vec![DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 11))],
        )])
        .await?;
    let invalid = record(
        "mixed",
        "mixed.maestro.internal.",
        vec![
            DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 12)),
            DnsRecordValue::Txt("mixed".to_owned()),
        ],
    );
    assert!(matches!(
        resolver.replace(&[invalid]).await,
        Err(DnsResolverError::MixedRecordSet { .. })
    ));

    let retained = resolver
        .lookup("api.maestro.internal.", DnsQueryType::A)
        .await?;
    assert_eq!(retained.answers.len(), 1);
    assert_eq!(
        retained.answers.first().expect("retained answer").value,
        DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 11))
    );
    Ok(())
}

#[tokio::test]
async fn authoritative_zone_rejects_duplicates_conflicts_cycles_and_unsafe_values()
-> Result<(), Box<dyn std::error::Error>> {
    let resolver = AuthoritativeDnsResolver::new()?;
    let duplicate = vec![
        record(
            "first",
            "api.maestro.internal.",
            vec![DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 11))],
        ),
        record(
            "second",
            "API.maestro.internal.",
            vec![DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 12))],
        ),
    ];
    assert!(matches!(
        resolver.replace(&duplicate).await,
        Err(DnsResolverError::DuplicateRecordSet { .. })
    ));

    let conflict = vec![
        record(
            "address",
            "api.maestro.internal.",
            vec![DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 11))],
        ),
        record(
            "alias",
            "api.maestro.internal.",
            vec![DnsRecordValue::Cname("target.maestro.internal.".to_owned())],
        ),
    ];
    assert!(matches!(
        resolver.replace(&conflict).await,
        Err(DnsResolverError::AliasDataConflict { .. })
    ));

    let cycle = vec![
        record(
            "alias-a",
            "a.maestro.internal.",
            vec![DnsRecordValue::Cname("b.maestro.internal.".to_owned())],
        ),
        record(
            "alias-b",
            "b.maestro.internal.",
            vec![DnsRecordValue::Cname("a.maestro.internal.".to_owned())],
        ),
    ];
    assert!(matches!(
        resolver.replace(&cycle).await,
        Err(DnsResolverError::AliasCycle { .. })
    ));
    assert!(matches!(
        resolver
            .replace(&[record(
                "outside",
                "example.com.",
                vec![DnsRecordValue::A(Ipv4Addr::LOCALHOST)],
            )])
            .await,
        Err(DnsResolverError::OutsideZone { .. })
    ));
    assert!(matches!(
        resolver
            .replace(&[record(
                "long-text",
                "text.maestro.internal.",
                vec![DnsRecordValue::Txt("x".repeat(256))],
            )])
            .await,
        Err(DnsResolverError::TextTooLong { .. })
    ));
    Ok(())
}

fn record(id: &str, name: &str, values: Vec<DnsRecordValue>) -> DnsRecord {
    DnsRecord {
        meta: ObjectMeta {
            id: DnsRecordId::new(id).expect("dns record id"),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision(1),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: DnsRecordSpec {
            name: name.to_owned(),
            values,
            ttl_secs: 30,
        },
        status: DnsRecordStatus {
            applied_generation: Generation(0),
            published_nodes: Vec::new(),
            conditions: Vec::new(),
        },
    }
}
