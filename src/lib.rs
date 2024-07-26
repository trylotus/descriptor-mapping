mod utils;

#[cfg(feature = "search")]
use std::{collections::BTreeSet, ops::Bound};

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};

use anyhow::anyhow;
use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use protobuf::{
    descriptor::FileDescriptorProto,
    reflect::{FileDescriptor, MessageDescriptor},
    Message, MessageDyn,
};
use serde::Deserialize;
use tokio::{select, sync::mpsc};
use tracing::{error, info};

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
    #[cfg(feature = "search")]
    keys: Arc<RwLock<BTreeSet<String>>>,
}

impl Clone for DescriptorMapping {
    fn clone(&self) -> Self {
        Self {
            mapping: self.mapping.clone(),
            #[cfg(feature = "search")]
            keys: self.keys.clone(),
        }
    }
}

impl DescriptorMapping {
    /// Create a descriptor mapping.
    pub fn new() -> Self {
        Self {
            mapping: Arc::new(RwLock::new(HashMap::new())),
            #[cfg(feature = "search")]
            keys: Arc::new(RwLock::new(BTreeSet::new())),
        }
    }

    /// Synchronize descriptor mapping from remote registry.
    pub fn sync_from_registry(&self, url: &str) {
        let this = self.clone();

        let mut rx = sync_from_registry(url);

        tokio::spawn(async move {
            while let Some((key, md)) = rx.recv().await {
                this.add(key, md);
            }
        });
    }

    /// Return all keys.
    pub fn keys<T>(&self) -> T
    where
        T: FromIterator<String>,
    {
        let mapping = self.mapping.read().unwrap();
        mapping.keys().cloned().collect()
    }

    /// Check if the given key exists.
    pub fn contains_key(&self, key: &str) -> bool {
        let mapping = self.mapping.read().unwrap();
        mapping.contains_key(key)
    }

    /// Get the descriptor for a given key.
    pub fn get(&self, key: &str) -> Option<Arc<MessageDescriptor>> {
        let mapping = self.mapping.read().unwrap();
        mapping.get(key).cloned()
    }

    /// Add the descriptor for a given key.
    pub fn add(&self, key: String, md: MessageDescriptor) {
        #[cfg(feature = "search")]
        {
            let mut keys = self.keys.write().unwrap();
            keys.insert(key.clone());
        }
        let mut mapping = self.mapping.write().unwrap();
        info!("Add mapping: {} -> {}", key, md.full_name());
        mapping.insert(key, Arc::new(md));
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

    #[cfg(feature = "search")]
    /// Find all keys that have the given prefix.
    pub fn search_prefix<T>(&self, prefix: &str) -> T
    where
        T: FromIterator<String>,
    {
        let keys = self.keys.read().unwrap();
        keys.range((Bound::Included(prefix.to_string()), Bound::Unbounded))
            .take_while(|key| key.starts_with(prefix))
            .cloned()
            .collect()
    }
}

#[derive(Deserialize)]
struct Descriptor {
    ts: DateTime<Utc>,
    author: String,
    connector: String,
    version: String,
    #[serde(with = "base64a")]
    files: Vec<Vec<u8>>,
}

/// Fetch descriptor mapping from remote registry.
///
/// This function will exit if there is an error from server.
async fn fetch_from_registry(
    url: &str,
    ts: Option<DateTime<Utc>>,
    tx: mpsc::Sender<Descriptor>,
) -> anyhow::Result<()> {
    let url = if let Some(ts) = ts {
        format!(
            "{}/sse/v1/descriptors?ts={}",
            url.trim_end_matches("/"),
            ts.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
        )
    } else {
        format!("{}/sse/v1/descriptors", url.trim_end_matches("/"))
    };

    let es = sse_client::EventSource::new(&url)?;

    let (tx_err, mut rx_err) = mpsc::channel(1);
    let tx_err_1 = tx_err.clone();

    es.on_message(
        move |message| match serde_json::from_str::<Descriptor>(&message.data) {
            Err(err) => {
                error!(error = err.to_string(), "Failed to decode sse message");
            }
            Ok(desc) => {
                if let Err(err) = tx.blocking_send(desc) {
                    let _ = tx_err_1.blocking_send(err.to_string());
                }
            }
        },
    );

    es.add_event_listener("error", move |event| {
        let _ = tx_err.blocking_send(event.data);
    });

    let mut health_check_interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        select! {
            err = rx_err.recv() => {
                return Err(anyhow!(err.unwrap()));
            },
            _ = health_check_interval.tick() => {
                if es.state() == sse_client::State::Closed {
                    return Err(anyhow!("See connection closed"));
                }
            }
        }
    }
}

/// Synchronize descriptor mapping from remote registry.
///
/// This function will automatically retry uppon failure.
pub fn sync_from_registry(url: &str) -> mpsc::Receiver<(String, MessageDescriptor)> {
    let url = url.to_string();

    let (tx, rx) = mpsc::channel(100);
    let tx_1 = tx.clone();

    let last_updated = Arc::new(AtomicI64::new(0));
    let last_updated_1 = last_updated.clone();

    let (tx_desc, mut rx_desc) = mpsc::channel::<Descriptor>(100);

    tokio::spawn(async move {
        'process: while let Some(desc) = rx_desc.recv().await {
            if let Some(ts) = desc.ts.timestamp_nanos_opt() {
                last_updated_1.store(ts, Ordering::Relaxed);
            }
            match decode_descriptor(desc) {
                Err(err) => error!(error = err.to_string(), "Failed to decode descriptor"),
                Ok(mds) => {
                    for (key, md) in mds {
                        if let Err(_) = tx_1.send((key, md)).await {
                            break 'process;
                        }
                    }
                }
            }
        }
    });

    tokio::spawn(async move {
        let mut retry_interval = tokio::time::interval(Duration::from_secs(10));

        loop {
            select! {
                _ = retry_interval.tick() => {
                    let last_updated = last_updated.load(Ordering::Relaxed);

                    let ts = match last_updated > 0 {
                        true => Some(DateTime::from_timestamp_nanos(last_updated)),
                        false => None,
                    };

                    if let Err(err) = fetch_from_registry(&url, ts, tx_desc.clone()).await {
                        error!(error = err.to_string(), "Failed to fetch from registry");
                    }
                },
                _ = tx.closed() => {
                    break;
                }
            }
        }
    });

    rx
}

fn decode_descriptor(desc: Descriptor) -> anyhow::Result<Vec<(String, MessageDescriptor)>> {
    let mut mapping = Vec::<(String, MessageDescriptor)>::new();

    for file in desc.files {
        let fdp = FileDescriptorProto::parse_from_bytes(&file)?;
        let fd = FileDescriptor::new_dynamic(fdp, &PROTO_DEPENDENCIES)?;
        for md in fd.messages() {
            let key = format!(
                "{}.{}.{}.{}",
                desc.author,
                desc.connector,
                desc.version.replace(".", "_"),
                md.full_name().replace(".", "_")
            );
            mapping.push((key, md));
        }
    }

    Ok(mapping)
}

#[tokio::test]
async fn test_sync_from_registry() {
    let mapping = DescriptorMapping::new();

    mapping.sync_from_registry("http://localhost:8081");

    tokio::time::sleep(Duration::from_secs(3)).await;

    let keys: Vec<String> = mapping.keys();

    println!("{:?}", keys);
}
