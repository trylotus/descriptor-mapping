use std::env;

use once_cell::sync::Lazy;
use prost_reflect::DescriptorPool;

pub static DESCRIPTOR_POOL: Lazy<DescriptorPool> = Lazy::new(|| {
    DescriptorPool::decode(
        include_bytes!(concat!(env!("OUT_DIR"), "/lotus_file_descriptor_set.bin")).as_ref(),
    )
    .unwrap()
});

include!(concat!(env!("OUT_DIR"), "/lotus.v1alpha1.rs"));
