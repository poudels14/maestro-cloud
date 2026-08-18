use anyhow::{Result, anyhow, bail};
use etcd_client::{Client, GetOptions, KeyValue};

pub const MAX_DECODING_MESSAGE_SIZE: usize = 32 * 1024 * 1024;

const RANGE_PAGE_SIZE: usize = 256;

pub async fn get_prefix(
    client: &Client,
    prefix: impl Into<Vec<u8>>,
    keys_only: bool,
    max_results: Option<usize>,
) -> Result<Vec<KeyValue>> {
    let prefix = prefix.into();
    let range_end = prefix_range_end(&prefix)
        .ok_or_else(|| anyhow!("failed to compute etcd range end for prefix"))?;
    get_range(client, prefix, range_end, keys_only, max_results).await
}

pub async fn get_range(
    client: &Client,
    mut start_key: Vec<u8>,
    range_end: Vec<u8>,
    keys_only: bool,
    max_results: Option<usize>,
) -> Result<Vec<KeyValue>> {
    let mut entries = Vec::new();
    let mut revision = None;

    loop {
        let remaining = max_results
            .map(|limit| limit.saturating_sub(entries.len()))
            .unwrap_or(RANGE_PAGE_SIZE);
        if remaining == 0 {
            break;
        }
        let page_size = remaining.min(RANGE_PAGE_SIZE);
        let mut options = GetOptions::new()
            .with_range(range_end.clone())
            .with_limit(page_size as i64);
        if keys_only {
            options = options.with_keys_only();
        }
        if let Some(revision) = revision {
            options = options.with_revision(revision);
        }

        let mut kv_client = client
            .kv_client()
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE);
        let mut response = kv_client.get(start_key, Some(options)).await?;
        if revision.is_none() {
            revision = response.header().map(|header| header.revision());
        }
        let more = response.more();
        let page = response.take_kvs();
        if page.is_empty() {
            if more {
                bail!("etcd range response reported more entries without returning a page");
            }
            break;
        }

        start_key = page.last().expect("non-empty etcd page").key().to_vec();
        start_key.push(0);
        entries.extend(page);
        if !more {
            break;
        }
    }

    Ok(entries)
}

fn prefix_range_end(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut end = prefix.to_vec();
    for index in (0..end.len()).rev() {
        if end[index] != 0xff {
            end[index] += 1;
            end.truncate(index + 1);
            return Some(end);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_exclusive_prefix_range_end() {
        assert_eq!(
            prefix_range_end(b"/maetro/services/"),
            Some(b"/maetro/services0".to_vec())
        );
        assert_eq!(prefix_range_end(&[0x01, 0xff]), Some(vec![0x02]));
        assert_eq!(prefix_range_end(&[0xff]), None);
    }
}
