mod lotus;
mod proto;
mod utils;

use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::Duration,
};

use anyhow::anyhow;
use bytes::Buf;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use prost_reflect_build::Builder;
use proto::decode_file_descriptor_protos;
use reqwest::StatusCode;
use serde::Deserialize;

use crate::utils::base64a;

/// Compile protobuf files that will be loaded into the mapping later.
pub fn compile_protos(
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> anyhow::Result<()> {
    let file_descriptor_set_path = env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("local_file_descriptor_set.bin");

    Builder::new()
        .descriptor_pool("crate::DESCRIPTOR_POOL")
        .file_descriptor_set_path(file_descriptor_set_path)
        .compile_protos(protos, includes)?;

    Ok(())
}

/// A mapping from string key to protobuf descriptor.
pub struct DescriptorMapping {
    mapping: Arc<RwLock<HashMap<String, MessageDescriptor>>>,
}

impl Clone for DescriptorMapping {
    fn clone(&self) -> Self {
        Self {
            mapping: self.mapping.clone(),
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

impl DescriptorMapping {
    /// Create a descriptor mapping.
    pub fn new() -> Self {
        DescriptorMapping {
            mapping: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Load compiled protobuf into the mapping.
    ///
    /// Provide the path to a json file mapping from descriptor key to message name.
    pub fn load_from_local<P>(&self, path: P) -> anyhow::Result<()>
    where
        P: AsRef<Path>,
    {
        let pool_bin = std::fs::read(env::var("OUT_DIR")? + "/local_file_descriptor_set.bin")?;
        let pool = DescriptorPool::decode(pool_bin.as_ref())?;

        for md in pool.all_messages() {
            tracing::debug!("Load descriptor: {}", md.full_name());
        }

        let key_mapping_json = std::fs::read(path)?;
        let key_mapping: HashMap<String, String> = serde_json::from_slice(&key_mapping_json)?;

        for (key, msg_name) in key_mapping {
            if let Some(md) = pool.get_message_by_name(&msg_name) {
                self.add(key, md);
            } else {
                tracing::error!("Unknown message: {}", msg_name);
            }
        }

        Ok(())
    }

    /// Fetch descriptor mapping from remote registry.
    pub async fn fetch_from_registry(
        &self,
        url: &str,
        ts: Option<i64>,
    ) -> anyhow::Result<Option<i64>> {
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

                for d in resp_body.data {
                    let fds = decode_file_descriptor_protos(d.files)?;
                    let mut mapping = Vec::<(String, MessageDescriptor)>::new();

                    for fd in fds {
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

                    self.add_many(mapping);
                }

                Ok(Some(resp_body.last_updated))
            }
            _ => {
                let status = resp.status();
                let resp_bytes = resp.bytes().await?.to_vec();
                let err_msg = String::from_utf8(resp_bytes)?;
                return Err(anyhow!("({}) {}", status, err_msg));
            }
        }
    }

    /// Periodically synchronize local mapping from remote registry.
    pub fn sync_from_registry(&self, url: &str, sync_interval: Duration) {
        let url = url.to_string();
        let this = self.clone();

        tokio::spawn(async move {
            let mut ts: Option<i64> = None;
            let mut interval = tokio::time::interval(sync_interval);

            loop {
                match this.fetch_from_registry(&url, ts).await {
                    Err(err) => {
                        tracing::error!("Failed to fetch descriptors from registry: {}", err)
                    }
                    Ok(last_updated) => {
                        match last_updated {
                            None => {
                                tracing::debug!("Local descriptors are up-to-date with registry")
                            }
                            Some(last_updated) => {
                                tracing::debug!("Successfully fetched descriptors from registry");
                                ts = Some(last_updated);
                            }
                        };
                    }
                };

                interval.tick().await;
            }
        });
    }

    /// Get the descriptor for a given key.
    pub fn get(&self, key: &str) -> Option<MessageDescriptor> {
        let mapping = self.mapping.read().unwrap();
        mapping.get(key).cloned()
    }

    /// Add the descriptor for a given key.
    pub fn add(&self, key: String, md: MessageDescriptor) {
        let mut mapping = self.mapping.write().unwrap();
        mapping.insert(key, md);
    }

    /// Add a list of key and descriptor pairs.
    pub fn add_many(&self, mds: Vec<(String, MessageDescriptor)>) {
        let mut mapping = self.mapping.write().unwrap();
        for (key, md) in mds {
            tracing::info!("Add mapping: {} -> {}", key, md.full_name());
            mapping.insert(key, md);
        }
    }

    /// Decode a message given the descriptor key.
    pub fn decode<B>(&self, key: &str, buf: B) -> anyhow::Result<DynamicMessage>
    where
        B: Buf,
    {
        if let Some(md) = self.get(key) {
            let msg = DynamicMessage::decode(md, buf)?;
            Ok(msg)
        } else {
            Err(anyhow!("Key not found: {}", key))
        }
    }
}
