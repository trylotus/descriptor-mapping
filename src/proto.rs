use prost_reflect::{
    prost::Message,
    prost_types::{FileDescriptorProto, Timestamp},
    DescriptorPool, FileDescriptor, ReflectMessage,
};

use crate::lotus;

fn well_known_types() -> Vec<FileDescriptorProto> {
    Vec::from([Timestamp::default()
        .descriptor()
        .parent_file_descriptor_proto()
        .clone()])
}

pub fn decode_file_descriptor_protos(files: Vec<Vec<u8>>) -> anyhow::Result<Vec<FileDescriptor>> {
    let mut fdps = Vec::<FileDescriptorProto>::new();
    for file in files {
        let fdp = FileDescriptorProto::decode(file.as_ref())?;
        fdps.push(fdp);
    }

    let file_names: Vec<String> = fdps.iter().map(|fdp| fdp.name().to_string()).collect();

    let mut pool = DescriptorPool::new();
    pool.add_file_descriptor_protos(well_known_types())?;
    for file in lotus::DESCRIPTOR_POOL.files() {
        pool.add_file_descriptor_proto(file.file_descriptor_proto().clone())?;
    }
    pool.add_file_descriptor_protos(fdps)?;

    let mut fds = Vec::<FileDescriptor>::new();
    for file_name in file_names {
        if let Some(fd) = pool.get_file_by_name(&file_name) {
            fds.push(fd);
        }
    }

    Ok(fds)
}
