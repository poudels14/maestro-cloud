use std::collections::BTreeSet;
use std::time::Duration;

use etcd_client::{Compare as EtcdCompare, CompareOp, DeleteOptions, KeyValue, PutOptions, TxnOp};

use crate::etcd_value::ValueProtector;
use crate::{
    Compare, ExpectedVersion, Mutation, SessionBinding, SessionId, StoreError, StoreKey,
    StoredValue, Transaction, Version,
};

pub(crate) fn etcd_compare(compare: Compare) -> Result<EtcdCompare, StoreError> {
    let key = compare.key.as_str();
    match compare.expected {
        ExpectedVersion::Missing => Ok(EtcdCompare::version(key, CompareOp::Equal, 0)),
        ExpectedVersion::Exact(version) => Ok(EtcdCompare::mod_revision(
            key,
            CompareOp::Equal,
            revision_i64(version.0)?,
        )),
    }
}

pub(crate) fn etcd_operation(
    mutation: &Mutation,
    values: &ValueProtector,
) -> Result<TxnOp, StoreError> {
    match mutation {
        Mutation::Put {
            key,
            value,
            session,
        } => {
            let options = session
                .map(|binding| session_i64(binding.session_id))
                .transpose()?
                .map(|lease| PutOptions::new().with_lease(lease));
            let value = values.protect(key, value)?;
            Ok(TxnOp::put(key.as_str(), value, options))
        }
        Mutation::Delete { key } => Ok(TxnOp::delete(
            key.as_str(),
            Some(DeleteOptions::new().with_prev_key()),
        )),
    }
}

pub(crate) fn stored_value(
    value: &KeyValue,
    values: &ValueProtector,
) -> Result<StoredValue, StoreError> {
    let key = StoreKey::from_backend(value.key())?;
    Ok(StoredValue {
        value: values.unprotect(&key, value.value())?,
        key,
        version: version(value.mod_revision())?,
    })
}

pub(crate) fn validate_transaction(transaction: &Transaction) -> Result<(), StoreError> {
    let unique_keys = transaction
        .mutations
        .iter()
        .map(|mutation| match mutation {
            Mutation::Put { key, .. } | Mutation::Delete { key } => key,
        })
        .collect::<BTreeSet<_>>();
    if unique_keys.len() == transaction.mutations.len() {
        Ok(())
    } else {
        Err(StoreError::Contract {
            message: "a transaction cannot mutate the same key more than once".to_string(),
        })
    }
}

pub(crate) fn response_revision(
    header: Option<&etcd_client::ResponseHeader>,
) -> Result<u64, StoreError> {
    let header = header.ok_or_else(|| StoreError::Contract {
        message: "etcd response omitted its linearizable revision header".to_string(),
    })?;
    revision(header.revision())
}

pub(crate) fn revision(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::Contract {
        message: format!("etcd returned an invalid negative revision: {value}"),
    })
}

pub(crate) fn version(value: i64) -> Result<Version, StoreError> {
    revision(value).map(Version)
}

pub(crate) fn revision_i64(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| StoreError::Contract {
        message: format!("store revision does not fit etcd's signed revision space: {value}"),
    })
}

pub(crate) fn session_id(value: i64) -> Result<SessionId, StoreError> {
    u64::try_from(value)
        .map(SessionId)
        .map_err(|_| StoreError::Contract {
            message: format!("etcd returned an invalid negative lease identity: {value}"),
        })
}

pub(crate) fn session_i64(value: SessionId) -> Result<i64, StoreError> {
    i64::try_from(value.0).map_err(|_| StoreError::Contract {
        message: "store session identity does not fit etcd's lease space".to_string(),
    })
}

pub(crate) fn duration_to_ttl(ttl: Duration) -> Result<i64, StoreError> {
    if ttl.is_zero() {
        return Err(StoreError::Contract {
            message: "store session TTL must be greater than zero".to_string(),
        });
    }
    let seconds = ttl
        .as_secs()
        .saturating_add(u64::from(ttl.subsec_nanos() > 0));
    i64::try_from(seconds).map_err(|_| StoreError::Contract {
        message: "store session TTL exceeds etcd's supported range".to_string(),
    })
}

pub(crate) fn operation_error(
    error: etcd_client::Error,
    session: Option<SessionBinding>,
) -> StoreError {
    let message = error.to_string();
    if message.contains("requested lease not found")
        && let Some(binding) = session
    {
        StoreError::SessionExpired {
            session_id: binding.session_id,
        }
    } else {
        StoreError::Unavailable { message }
    }
}

pub(crate) fn unavailable(error: etcd_client::Error) -> StoreError {
    StoreError::Unavailable {
        message: error.to_string(),
    }
}
