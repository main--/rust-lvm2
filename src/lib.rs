#![no_std]
extern crate alloc;

use acid_io::{Read, Seek, SeekFrom};
use alloc::string::{String, ToString};
use header::PhysicalVolumeHeader;
use serde::Deserialize;
use snafu::{Snafu, ResultExt, ensure, OptionExt};

use crate::header::{PhysicalVolumeLabelHeader, MetadataAreaHeader};
use crate::metadata::{deserialize::MetadataElements, MetadataRoot};

// Vocabulary: in this crate we use the term "sheet" to describe a block of exactly 512 bytes
// (to avoid confusion around the word "sector")

// spec: https://github.com/libyal/libvslvm/blob/ab09a380072448d9c84c886d487d8c3dfa2d1527/documentation/Logical%20Volume%20Manager%20(LVM)%20format.asciidoc#2-physical-volume-label

pub struct Lvm2 {
    pvh: PhysicalVolumeHeader,
    pv_name: String,
    vg_name: String,
    vg_config: MetadataRoot,
}

#[derive(Debug, Snafu)]
pub enum Error {
    Io { source: acid_io::Error },
    WrongMagic,
    ParseError { error: String },
    MultipleVGsError,
    PVDoesntContainItself,
    Serde { source: serde::de::value::Error },
    MissingMetadata,
}


mod header;
pub mod metadata;
mod force_de_typed_map;
mod lv;
pub use lv::*;



impl Lvm2 {
    pub fn open<T: Read + Seek>(mut reader: T) -> Result<Self, Error> {
        reader.seek(SeekFrom::Start(512)).context(IoSnafu)?; // skip zero sheet

        let mut buf = [0u8; 512];
        reader.read_exact(&mut buf).context(IoSnafu)?; // read header
        tracing::trace!(?buf);

        let (_, vhl) = PhysicalVolumeLabelHeader::parse(&buf).map_err(|e| Error::ParseError { error: e.to_string() })?;
        tracing::trace!(?vhl);
        let (_, pvh) = PhysicalVolumeHeader::parse(&buf[(vhl.data_offset as usize)..]).map_err(|e| Error::ParseError { error: e.to_string() })?;
        tracing::trace!(?pvh);

        let metadata_descriptor = pvh.metadata_descriptors.first().context(MissingMetadataSnafu)?;

        reader.seek(acid_io::SeekFrom::Start(metadata_descriptor.offset)).context(IoSnafu)?; // skip zero sheet
        reader.read_exact(&mut buf).context(IoSnafu)?;
        let (_, mah) = MetadataAreaHeader::parse(&buf).map_err(|e| Error::ParseError { error: e.to_string() })?;
        tracing::trace!(?mah);

        let mut metadata = String::new();
        for locdesc in &mah.location_descriptors {
            reader.seek(acid_io::SeekFrom::Start(metadata_descriptor.offset + locdesc.data_area_offset)).context(IoSnafu)?; // skip zero sheet
            reader.by_ref().take(locdesc.data_area_size).read_to_string(&mut metadata).context(IoSnafu)?;
        }
        tracing::debug!(%metadata);

        let (trailing_garbage, metadata) = MetadataElements::parse(&metadata).map_err(|e| Error::ParseError { error: e.to_string() })?;
        tracing::debug!(?trailing_garbage, ?metadata);

        let meta_root = force_de_typed_map::ForceDeTypedMap::<String, MetadataRoot>::deserialize(&metadata).context(SerdeSnafu)?;
        tracing::debug!(?meta_root);

        ensure!(meta_root.0.len() == 1, MultipleVGsSnafu);
        let (vg_name, vg_config) = meta_root.0.into_iter().next().unwrap();

        let pv_name = vg_config.physical_volumes.iter()
            .find(|(_, v)| v.id.replace('-', "") == pvh.pv_ident)
            .context(PVDoesntContainItselfSnafu)?
            .0.clone();

        Ok(Self { pvh, pv_name, vg_name, vg_config })
    }

    pub fn pv_id(&self) -> &str {
        &self.pvh.pv_ident
    }
    pub fn pv_name(&self) -> &str {
        &self.pv_name
    }

    pub fn vg_name(&self) -> &str {
        &self.vg_name
    }
    pub fn vg_id(&self) -> &str {
        &self.vg_config.id
    }

    pub fn lvs(&self) -> impl Iterator<Item=LV> {
        self.vg_config.logical_volumes.iter().map(|(name, desc)| LV { name, desc })
    }
    pub fn open_lv_by_name<'a, T: Read + Seek>(&'a self, name: &str, reader: T) -> Option<OpenLV<'a, T>> {
        self.vg_config.logical_volumes.get_key_value(name).map(move |(name, desc)| self.open_lv(LV { name, desc }, reader))
    }
    pub fn open_lv_by_id<'a, T: Read + Seek>(&'a self, id: &str, reader: T) -> Option<OpenLV<'a, T>> {
        self.lvs().find(|lv| lv.id() == id).map(move |lv| self.open_lv(lv, reader))
    }
    pub fn open_lv<'a, T: Read + Seek>(&'a self, lv: LV<'a>, reader: T) -> OpenLV<'a, T> {
        OpenLV {
            lv,
            lvm: self,
            reader,

            position: 0,
            current_segment_end: 0,
        }
    }

    pub fn extent_size(&self) -> u64 {
        self.vg_config.extent_size * 512
    }
}


