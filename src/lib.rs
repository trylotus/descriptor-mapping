use std::{collections::HashMap, env, path::Path};

use anyhow::{anyhow, Ok};
use bytes::Buf;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor};
use prost_reflect_build::Builder;

/// Compile protobuf files that will be loaded into the mapping later.
pub fn compile_protos(
    protos: &[impl AsRef<Path>],
    includes: &[impl AsRef<Path>],
) -> anyhow::Result<()> {
    Builder::new()
        .descriptor_pool("crate::DESCRIPTOR_POOL")
        .compile_protos(protos, includes)?;

    Ok(())
}

/// A mapping from string key to protobuf descriptor.
pub struct DescriptorMapping {
    mapping: HashMap<String, MessageDescriptor>,
}

impl DescriptorMapping {
    /// Create a descriptor mapping.
    pub fn new() -> Self {
        DescriptorMapping {
            mapping: HashMap::new(),
        }
    }

    /// Load compiled protobuf into the mapping.
    ///
    /// Provide the path to a json file mapping from descriptor key to message name.
    pub fn load_from_local<P>(&mut self, path: P) -> anyhow::Result<()>
    where
        P: AsRef<Path>,
    {
        let pool_bin = std::fs::read(env::var("OUT_DIR")? + "/file_descriptor_set.bin")?;
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

    /// Get the descriptor for a given key.
    pub fn get(&self, key: &str) -> Option<MessageDescriptor> {
        self.mapping.get(key).cloned()
    }

    /// Add the descriptor for a given key.
    pub fn add(&mut self, key: String, md: MessageDescriptor) {
        tracing::info!("Add mapping: {} -> {}", key, md.full_name());
        self.mapping.insert(key, md);
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
