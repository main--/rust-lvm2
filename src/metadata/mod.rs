use core::ops::Range;

use alloc::collections::BTreeMap;
use alloc::string::String;
use alloc::vec::Vec;
use serde::Deserialize;

use crate::force_de_typed_map::ForceDeTypedMap;

pub(crate) mod deserialize;

#[derive(Deserialize, Clone, Debug)]
pub struct MetadataRoot {
    pub id: String,
    pub extent_size: u64,
    pub physical_volumes: BTreeMap<String, PVDesc>,
    pub logical_volumes: BTreeMap<String, LVDesc>,
}
#[derive(Deserialize, Clone, Debug)]
pub struct PVDesc {
    pub id: String,
    pub device: String,
    pub pe_start: u64,
    pub pe_count: u64,
}
#[derive(Deserialize, Clone, Debug)]
pub struct LVDesc {
    pub id: String,
    pub status: Vec<String>,
    pub flags: Vec<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    pub creation_time: u64,
    pub creation_host: String,
    pub segment_count: usize,
    #[serde(flatten)]
    pub segments: ForceDeTypedMap<String, LVSegmentDesc>,
}
#[derive(Deserialize, Clone, Debug)]
pub struct LVSegmentDesc {
    pub start_extent: u64,
    pub extent_count: u64,
    pub r#type: String,
    pub stripe_count: Option<usize>,
    pub stripe_size: Option<usize>,
    pub stripes: Option<(String, u64)>,
    pub raid0_lvs: Option<Vec<String>>,
}
impl LVSegmentDesc {
    pub fn extents(&self) -> Range<u64> {
        self.start_extent .. (self.start_extent + self.extent_count)
    }
}
