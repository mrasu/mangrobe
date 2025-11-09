// Write "./../" explicitly because IntelliJ doesn't recognize the path when using other macros like `concat!(env!("CARGO_MANIFEST_DIR")`
include!("./../generated/mangrobe.api.rs");

pub(crate) const FILE_DESCRIPTOR_SET2: &[u8] = include_bytes!("./../generated/api_descriptor.bin");
