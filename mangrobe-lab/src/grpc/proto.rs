// Write "./../" explicitly because IntelliJ doesn't recognize the path when using other macros like `concat!(env!("CARGO_MANIFEST_DIR")`
include!("./../generated/mangrobe.api.rs");
