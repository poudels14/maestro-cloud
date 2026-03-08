use super::*;

#[test]
fn cancel_endpoint_adds_http_when_missing() {
    let endpoint =
        cancel_endpoint("127.0.0.1:3000", "svc-1", "dep-1").expect("should build endpoint");
    assert_eq!(
        endpoint,
        "http://127.0.0.1:3000/api/services/svc-1/deployments/dep-1/cancel"
    );
}
