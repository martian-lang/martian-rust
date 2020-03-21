//! Re-export commonly used symbols in this crate
//!
//! It is a common practice in Rust crates to include a `prelude` module
//! to help you export commonly used symbols in a crate. If you are
//! using `martian` crate, it might be convenient to use:
//! ```rust
//! use martian::prelude::*;
//! ```
pub use crate::stage::{
    MartianFileType, MartianMain, MartianMakePath, MartianRover, MartianStage, MartianVoid,
    RawMartianStage, Resource, StageDef,
};
pub use crate::{MartianAdapter, martian_make_mro};
pub use failure::Error;
pub use log::LevelFilter;
pub use martian_stages;
