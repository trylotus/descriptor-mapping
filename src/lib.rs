mod utils;

use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::anyhow;
use once_cell::sync::Lazy;
use protobuf::{
    descriptor::FileDescriptorProto,
    reflect::{FileDescriptor, MessageDescriptor},
    Message, MessageDyn,
};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::utils::base64a;

static PROTO_DEPENDENCIES: Lazy<Vec<FileDescriptor>> = Lazy::new(|| {
    vec![
        protobuf::well_known_types::timestamp::file_descriptor().clone(),
        lotus_proto::lotus::file_descriptor().clone(),
    ]
});

/// A mapping from string key to protobuf descriptor.
pub struct DescriptorMapping {
    mapping: Arc<RwLock<HashMap<String, Arc<MessageDescriptor>>>>,
}

impl Clone for DescriptorMapping {
    fn clone(&self) -> Self {
        Self {
            mapping: self.mapping.clone(),
        }
    }
}

impl DescriptorMapping {
    /// Create a descriptor mapping.
    pub fn new() -> Self {
        DescriptorMapping {
            mapping: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Fetch descriptor mapping from remote registry.
    pub async fn fetch_from_registry(&self, url: &str, ts: Option<i64>) -> anyhow::Result<()> {
        if let Some(result) = fetch_from_registry(url, ts).await? {
            self.add_many(result.mapping);
        }

        Ok(())
    }

    /// Periodically synchronize mapping from remote registry.
    ///
    /// This function unblocks after the first successful synchronization.
    pub async fn sync_from_registry(&self, url: &str, sync_interval: Duration) {
        let url = url.to_string();
        let this = self.clone();

        let (tx, mut rx) = mpsc::channel(1);

        tokio::spawn(async move {
            let mut results = sync_from_registry(&url, sync_interval);

            while let Some(result) = results.recv().await {
                this.add_many(result);

                if !tx.is_closed() {
                    let _ = tx.send(true).await;
                    tx.closed().await;
                }
            }
        });

        rx.recv().await;
    }

    /// Return all keys.
    pub fn keys<T>(&self) -> T
    where
        T: FromIterator<String>,
    {
        let mapping = self.mapping.read().unwrap();
        mapping.keys().cloned().collect()
    }

    /// Get the descriptor for a given key.
    pub fn get(&self, key: &str) -> Option<Arc<MessageDescriptor>> {
        let mapping = self.mapping.read().unwrap();
        mapping.get(key).cloned()
    }

    /// Add the descriptor for a given key.
    pub fn add(&self, key: String, md: MessageDescriptor) {
        let mut mapping = self.mapping.write().unwrap();
        mapping.insert(key, Arc::new(md));
    }

    /// Add a list of key and descriptor pairs.
    pub fn add_many(&self, mds: Vec<(String, MessageDescriptor)>) {
        let mut mapping = self.mapping.write().unwrap();
        for (key, md) in mds {
            info!("Add mapping: {} -> {}", key, md.full_name());
            mapping.insert(key, Arc::new(md));
        }
    }

    /// Decode a message given the descriptor key.
    pub fn decode(&self, key: &str, bytes: &[u8]) -> anyhow::Result<Box<dyn MessageDyn>> {
        if let Some(md) = self.get(key) {
            let msg = md.parse_from_bytes(bytes)?;
            Ok(msg)
        } else {
            Err(anyhow!("Key not found: {}", key))
        }
    }
}

#[derive(Deserialize)]
struct FetchResponseBody {
    last_updated: i64,
    data: Vec<Descriptor>,
}

#[derive(Deserialize)]
struct Descriptor {
    author: String,
    connector: String,
    version: String,
    #[serde(with = "base64a")]
    files: Vec<Vec<u8>>,
}

pub struct FetchResult {
    pub last_updated: i64,
    pub mapping: Vec<(String, MessageDescriptor)>,
}

/// Fetch descriptor mapping from remote registry.
pub async fn fetch_from_registry(
    url: &str,
    ts: Option<i64>,
) -> anyhow::Result<Option<FetchResult>> {
    let url = if let Some(ts) = ts {
        format!("{}/api/v1/descriptors?ts={}", url.trim_end_matches("/"), ts)
    } else {
        format!("{}/api/v1/descriptors", url.trim_end_matches("/"))
    };

    let resp = reqwest::get(url).await?;

    match resp.status() {
        StatusCode::NOT_FOUND => Ok(None),
        StatusCode::OK => {
            let resp_bytes = resp.bytes().await?.to_vec();
            let resp_body: FetchResponseBody = serde_json::from_slice(&resp_bytes)?;

            let mut mapping = Vec::<(String, MessageDescriptor)>::new();

            for d in resp_body.data {
                for file in d.files {
                    let fdp = FileDescriptorProto::parse_from_bytes(&file)?;
                    let fd = FileDescriptor::new_dynamic(fdp, &PROTO_DEPENDENCIES)?;
                    for md in fd.messages() {
                        let key = format!(
                            "{}.{}.{}.{}",
                            d.author,
                            d.connector,
                            d.version.replace(".", "_"),
                            md.full_name().replace(".", "_")
                        );
                        mapping.push((key, md));
                    }
                }
            }

            let result = FetchResult {
                last_updated: resp_body.last_updated,
                mapping,
            };

            Ok(Some(result))
        }
        _ => {
            let status = resp.status();
            let resp_bytes = resp.bytes().await?.to_vec();
            let err_msg = String::from_utf8(resp_bytes)?;

            Err(anyhow!("({}) {}", status, err_msg))
        }
    }
}

/// Periodically synchronize mapping from remote registry.
pub fn sync_from_registry(
    url: &str,
    sync_interval: Duration,
) -> mpsc::Receiver<Vec<(String, MessageDescriptor)>> {
    let url = url.to_string();

    let (tx, rx) = mpsc::channel(1);

    tokio::spawn(async move {
        let mut ts: Option<i64> = None;
        let mut interval = tokio::time::interval(sync_interval);

        loop {
            match fetch_from_registry(&url, ts).await {
                Err(err) => {
                    error!("Failed to fetch descriptors from registry: {}", err)
                }
                Ok(result) => {
                    match result {
                        None => {
                            debug!("Local descriptors are up-to-date with registry")
                        }
                        Some(result) => {
                            debug!("Successfully fetched descriptors from registry");

                            ts = Some(result.last_updated);

                            if let Err(_) = tx.send(result.mapping).await {
                                break;
                            }
                        }
                    };
                }
            };

            interval.tick().await;
        }
    });

    rx
}
