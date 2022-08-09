#![no_std]
extern crate alloc;

use core::marker::PhantomData;
use core::ops::Range;

use acid_io::{Read, Seek, SeekFrom};
use alloc::borrow::ToOwned;
use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use nom::IResult;
use nom::branch::alt;
use nom::bytes::complete::{take, take_until, take_till1};
use nom::bytes::streaming::tag;
use nom::character::complete::{char, multispace0, line_ending, not_line_ending};
use nom::combinator::map;
use nom::error::ParseError;
use nom::multi::{many_till, many0, separated_list0};
use nom::number::complete::le_u32;
use nom::number::streaming::le_u64;
use nom::sequence::{preceded, delimited, tuple};
use serde::de::value::StrDeserializer;
use serde::{Deserializer, de, forward_to_deserialize_any, Deserialize};
use snafu::{Snafu, ResultExt, ensure, OptionExt};

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

#[derive(Debug)]
pub struct PhysicalVolumeLabel {
    pub label_header: PhysicalVolumeLabelHeader,
    pub header: PhysicalVolumeHeader,
}


#[derive(Debug)]
pub struct PhysicalVolumeLabelHeader {
    pub sector_number: u64,
    pub checksum: u32,
    pub data_offset: u32,
}
impl PhysicalVolumeLabelHeader {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let (input, _) = tag(b"LABELONE")(input)?;
        let (input, sector_number) = le_u64(input)?;
        let (input, checksum) = le_u32(input)?;
        let (input, data_offset) = le_u32(input)?;
        let (input, _) = tag(b"LVM2 001")(input)?;
        Ok((input, Self { sector_number, checksum, data_offset }))
    }
}
#[derive(Debug)]
pub struct PhysicalVolumeHeader {
    pub pv_ident: String,
    pub pv_size: u64,
    pub data_descriptors: Vec<DataDescriptor>,
    pub metadata_descriptors: Vec<DataDescriptor>,
}
impl PhysicalVolumeHeader {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let (input, pv_ident_raw) = take(32usize)(input)?;
        let pv_ident = alloc::str::from_utf8(pv_ident_raw).map_err(|_| nom::Err::Failure(nom::error::Error::from_error_kind(pv_ident_raw, nom::error::ErrorKind::Char)))?.to_owned();
        let (input, pv_size) = le_u64(input)?;
        let (input, (data_descriptors, _)) = many_till(DataDescriptor::parse, tag(&[0u8; 16]))(input)?;
        let (input, (metadata_descriptors, _)) = many_till(DataDescriptor::parse, tag(&[0u8; 16]))(input)?;
        Ok((input, Self { pv_ident, pv_size, data_descriptors, metadata_descriptors }))
    }
}
#[derive(Debug)]
pub struct DataDescriptor {
    pub offset: u64,
    pub size: u64,
}
impl DataDescriptor {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let (input, offset) = le_u64(input)?;
        let (input, size) = le_u64(input)?;
        Ok((input, Self { offset, size }))
    }
}

#[derive(Debug)]
pub struct MetadataAreaHeader {
    pub checksum: u32,
    pub version: u32,
    pub metadata_area_offset: u64,
    pub metadata_area_size: u64,
    pub location_descriptors: Vec<LocationDescriptor>,
}
impl MetadataAreaHeader {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let (input, checksum) = le_u32(input)?;
        let (input, _) = tag(b" LVM2 x[5A%r0N*>")(input)?; // lol
        let (input, version) = le_u32(input)?;
        let (input, metadata_area_offset) = le_u64(input)?;
        let (input, metadata_area_size) = le_u64(input)?;
        let (input, (location_descriptors, _)) = many_till(LocationDescriptor::parse, tag(&[0u8; 24]))(input)?;
        Ok((input, Self { checksum, version, metadata_area_offset, metadata_area_size, location_descriptors }))
    }
}
#[derive(Debug)]
pub struct LocationDescriptor {
    pub data_area_offset: u64,
    pub data_area_size: u64,
    pub checksum: u32,
    pub flags: u32,
}
impl LocationDescriptor {
    pub fn parse(input: &[u8]) -> IResult<&[u8], Self> {
        let (input, data_area_offset) = le_u64(input)?;
        let (input, data_area_size) = le_u64(input)?;
        let (input, checksum) = le_u32(input)?;
        let (input, flags) = le_u32(input)?;
        Ok((input, Self { data_area_offset, data_area_size, checksum, flags }))
    }
}


// a little metadata parser
#[derive(Debug)]
enum MetadataValue {
    String(String),
    Number(i64),
    Array(Vec<MetadataValue>),
}
impl MetadataValue {
    pub fn parse(input: &str) -> IResult<&str, Self> {
        delimited(
            whitespace,
            alt((
                map(nom::character::complete::i64, MetadataValue::Number),
                map(delimited(char('['), separated_list0(char(','), MetadataValue::parse), char(']')), MetadataValue::Array),
                map(delimited(char('"'), take_until("\""), char('"')), |x: &str| MetadataValue::String(x.to_owned()))
            )),
            whitespace
        )(input)
    }
}
#[derive(Debug)]
struct MetadataElements<'a>(Vec<MetadataElement<'a>>);
impl<'a> MetadataElements<'a> {
    pub fn parse(input: &'a str) -> IResult<&'a str, Self> {
        map(many0(MetadataElement::parse), MetadataElements)(input)
    }
}

#[derive(Debug)]
enum MetadataElement<'a> {
    Group {
        name: &'a str,
        contents: MetadataElements<'a>,
    },
    Value {
        name: &'a str,
        value: MetadataValue,
    },
}
fn parse_ident(input: &str) -> IResult<&str, &str> {
    take_till1(|x| " \r\n\t{}=[]".contains(x))(input)
}
fn comment(input: &str) -> IResult<&str, &str> {
    preceded(multispace0, delimited(char('#'), not_line_ending, line_ending))(input)
}
fn whitespace(input: &str) -> IResult<&str, ()> {
    let (input, _) = many0(comment)(input)?;
    let (input, _) = multispace0(input)?;
    Ok((input, ()))
}
impl<'a> MetadataElement<'a> {
    pub fn name(&self) -> &'a str {
        match self {
            MetadataElement::Group { name, .. } => &name,
            MetadataElement::Value { name, .. } => &name,
        }
    }
    pub fn parse(input: &'a str) -> IResult<&'a str, Self> {
        delimited(
            whitespace,
            alt((
                map(tuple((parse_ident, whitespace, char('='), whitespace, MetadataValue::parse)), |(name, _, _, _, v)| MetadataElement::Value { name, value: v }),

                map(tuple((parse_ident, whitespace, char('{'), MetadataElements::parse, char('}'))), |(name, _, _, contents, _)| MetadataElement::Group { name, contents })
            )),
            whitespace
        )(input)
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a MetadataElements<'de> {
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }

    type Error = serde::de::value::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de> {
        visitor.visit_map(MetadataElementsAccess(self.0.iter().peekable()))
    }
}
struct MetadataElementsAccess<'a, 'de>(core::iter::Peekable<alloc::slice::Iter<'a, MetadataElement<'de>>>);

impl<'de, 'a> de::MapAccess<'de> for MetadataElementsAccess<'a, 'de> {
    type Error = serde::de::value::Error;

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: de::DeserializeSeed<'de> {
        match self.0.peek() {
            None => Ok(None),
            Some(x) => seed.deserialize(StrDeserializer::new(x.name())).map(Some),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: de::DeserializeSeed<'de> {
        match self.0.next() {
            None => panic!("deserializing value before key"),
            Some(MetadataElement::Group { contents, .. }) => seed.deserialize(contents),
            Some(MetadataElement::Value { value, .. }) => seed.deserialize(value),
        }
    }
}
impl<'de, 'a> de::Deserializer<'de> for &'a MetadataValue {
    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf option unit unit_struct newtype_struct seq tuple
        tuple_struct map struct enum identifier ignored_any
    }

    type Error = serde::de::value::Error;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de> {
        match self {
            MetadataValue::String(s) => visitor.visit_str(s),
            &MetadataValue::Number(n) => visitor.visit_i64(n),
            MetadataValue::Array(a) => visitor.visit_seq(MetadataValuesAccess(a.iter())),
        }
    }
}
struct MetadataValuesAccess<'a>(alloc::slice::Iter<'a, MetadataValue>);
impl<'de, 'a> de::SeqAccess<'de> for MetadataValuesAccess<'a> {
    type Error = serde::de::value::Error;

    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: de::DeserializeSeed<'de> {
        match self.0.next() {
            None => Ok(None),
            Some(x) => seed.deserialize(x).map(Some),
        }
    }
}

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

        let meta_root = ForceDeTypedMap::<String, MetadataRoot>::deserialize(&metadata).context(SerdeSnafu)?;
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
    pub fn open_lv<'a, T: Read + Seek>(&'a self, lv: &'a LV<'a>, reader: T) -> OpenLV<'a, T> {
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



pub struct OpenLV<'a, T> {
    lv: &'a LV<'a>,
    lvm: &'a Lvm2,
    reader: T,

    position: u64,
    current_segment_end: u64,
}
impl<'a, T: Read + Seek> Read for OpenLV<'a, T> {
    fn read(&mut self, mut buf: &mut [u8]) -> acid_io::Result<usize> {
        if self.position == self.current_segment_end {
            // trigger a seek to load the next segment
            self.seek(SeekFrom::Current(0))?;
        }

        let max_read = self.current_segment_end - self.position;
        if u64::try_from(buf.len()).unwrap() > max_read {
            buf = &mut buf[..(max_read as usize)];
        }
        self.reader.read(buf)
    }
}
impl<'a, T: Read + Seek> Seek for OpenLV<'a, T> {
    fn seek(&mut self, pos: SeekFrom) -> acid_io::Result<u64> {
        let pos = match pos {
            SeekFrom::Start(x) => x,
            SeekFrom::End(x) => ((self.lv.size_in_extents() * self.lvm.extent_size()) as i64 - x) as u64,
            SeekFrom::Current(x) => ((self.position as i64) + x) as u64,
        };
        let target_extent = pos / self.lvm.extent_size();

        let segment = self.lv.desc.segments.0.values().find(|x| x.extents().contains(&target_extent)).ok_or(acid_io::Error::new(acid_io::ErrorKind::Other, "no suitable segment found at this place"))?;
        if segment.r#type != "striped" || segment.stripe_count != Some(1) {
            return Err(acid_io::Error::new(acid_io::ErrorKind::Other, "segment is not linear"));
        }

        let offs_in_segment = pos - (segment.start_extent * self.lvm.extent_size());

        let (pv, loc) = segment.stripes.as_ref().ok_or(acid_io::Error::new(acid_io::ErrorKind::Other, "segment has no stripes"))?;
        if pv != self.lvm.pv_name() {
            return Err(acid_io::Error::new(acid_io::ErrorKind::Other, "data is not on this PV"));
        }

        let mut seek_target = loc * self.lvm.extent_size() + offs_in_segment;
        let mut found = false;
        for dd in &self.lvm.pvh.data_descriptors {
            if dd.size == 0 || dd.size > seek_target {
                seek_target += dd.offset;
                found = true;
                break;
            } else {
                seek_target -= dd.size;
            }
        };
        if !found {
            return Err(acid_io::Error::new(acid_io::ErrorKind::Other, "data is beyond the end of this PV"));
        }

        // TODO: what is the purpose of this value if it's not needed for this?
        //let pe_start = self.lvm.vg_config.physical_volumes.get(pv).unwrap().pe_start;

        self.reader.seek(SeekFrom::Start(seek_target))?;

        self.current_segment_end = segment.extents().end * self.lvm.extent_size();
        self.position = pos;
        Ok(pos)
    }
}

pub struct LV<'a> {
    name: &'a str,
    desc: &'a LVDesc,
}
impl<'a> LV<'a> {
    pub fn name(&self) -> &'a str {
        &self.name
    }
    pub fn id(&self) -> &'a str {
        &self.desc.id
    }
    pub fn size_in_extents(&self) -> u64 {
        self.desc.segments.0.values().map(|x| x.start_extent + x.extent_count).max()
            .expect("LV has no segments??")
    }
}

#[derive(Clone, Debug)]
pub struct ForceDeTypedMap<K, V>(BTreeMap<K, V>);
impl<'de, K: Deserialize<'de> + Ord, V: Deserialize<'de>> Deserialize<'de> for ForceDeTypedMap<K, V> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de> {
        struct MyVisitor<K, V>(PhantomData<K>, PhantomData<V>);
        impl<'de, K: Deserialize<'de> + Ord, V: Deserialize<'de>> de::Visitor<'de> for MyVisitor<K, V> {
            type Value = ForceDeTypedMap<K, V>;

            fn expecting(&self, formatter: &mut alloc::fmt::Formatter) -> alloc::fmt::Result {
                formatter.write_str("a ForceDeTypedMap")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
                where
                    A: de::MapAccess<'de>, {
                let mut m = BTreeMap::new();

                let mut last_error = None;
                while let Some(k) = map.next_key()? {
                    match map.next_value() {
                        Ok(v) => { m.insert(k, v); }
                        Err(e) if last_error.is_none() => last_error = Some(e),
                        _ => (),
                    }
                }

                match last_error {
                    Some(e) if m.is_empty() => Err(e),
                    _ => Ok(ForceDeTypedMap(m)),
                }
            }
        }
        deserializer.deserialize_map(MyVisitor::<K, V>(PhantomData, PhantomData))
    }
}


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
