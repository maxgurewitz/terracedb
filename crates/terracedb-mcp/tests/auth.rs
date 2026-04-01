use std::collections::BTreeMap;

use terracedb_capabilities::PolicySubject;
use terracedb_mcp::{
    DeterministicMcpAuthenticator, McpAuthContext, McpAuthenticationError,
    McpAuthenticationRequest, McpAuthenticator,
};

fn sample_context() -> McpAuthContext {
    McpAuthContext {
        subject: Some(PolicySubject {
            subject_id: "user:alice".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            groups: vec!["support".to_string()],
            attributes: BTreeMap::from([("role".to_string(), "operator".to_string())]),
        }),
        authentication_kind: Some("bearer".to_string()),
        trusted_draft: true,
        metadata: BTreeMap::new(),
    }
}

#[tokio::test]
async fn deterministic_authenticator_accepts_known_bearer_token() {
    let authenticator = DeterministicMcpAuthenticator::default()
        .with_bearer_token("secret-token", sample_context());

    let context = authenticator
        .authenticate(&McpAuthenticationRequest {
            session_id: "session-1".to_string(),
            stream_id: Some("stream-1".to_string()),
            headers: BTreeMap::from([(
                "authorization".to_string(),
                "Bearer secret-token".to_string(),
            )]),
            peer_addr: Some("127.0.0.1:9000".to_string()),
            metadata: BTreeMap::new(),
        })
        .await
        .expect("authenticate");

    assert_eq!(context, sample_context());
}

#[tokio::test]
async fn deterministic_authenticator_rejects_unknown_bearer_token() {
    let authenticator = DeterministicMcpAuthenticator::default()
        .with_bearer_token("secret-token", sample_context());

    let error = authenticator
        .authenticate(&McpAuthenticationRequest {
            session_id: "session-1".to_string(),
            stream_id: Some("stream-1".to_string()),
            headers: BTreeMap::from([(
                "authorization".to_string(),
                "Bearer wrong-token".to_string(),
            )]),
            peer_addr: None,
            metadata: BTreeMap::new(),
        })
        .await
        .expect_err("unknown token should be rejected");

    assert_eq!(error, McpAuthenticationError::Forbidden);
}
