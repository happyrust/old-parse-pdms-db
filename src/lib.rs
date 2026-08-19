#![feature(type_ascription)]
#![feature(array_methods)]
#![feature(slice_pattern)]
#![feature(core_intrinsics)]
#![feature(associated_type_bounds)]
#![feature(let_chains)]
#![feature(async_closure)]
#![feature(generic_const_exprs)]
#![feature(exclusive_range_pattern)]
#[macro_use]
extern crate approx;
#[allow(
    dead_code,
    unused_imports,
    unused_variables,
    unused,
    missing_docs,
    unused_results,
    unused_must_use
)]
#[allow(unused_mut)]
#[macro_use]
extern crate bitflags;
extern crate core;
#[macro_use]
extern crate derivative;
extern crate hash32;
#[macro_use]
extern crate hash32_derive;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate serde;

use aios_core::tool::db_tool::db1_hash;
use futures::stream::TryStreamExt;
use std::collections::HashSet;
use std::error::Error;
use std::time::Instant;

pub use parse::parse_pdms_dir;
pub use parse::{parse_db, parse_file};
pub use refno_index::{find_refno_entry, gen_ref_type_pos_table_from_index};

pub mod consts;
pub mod dict;
pub mod error_types;
pub mod paged;
pub mod parse;
pub mod parse_explict_tools;
pub mod refno_index;
pub mod test_cases;

pub type BHashMap<K, V> = std::collections::HashMap<K, V>;
