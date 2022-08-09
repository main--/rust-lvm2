#![no_std]
extern crate alloc;

use core::marker::PhantomData;

use acid_io::{Read, Seek};
use alloc::borrow::ToOwned;
use alloc::collections::BTreeMap;
use alloc::string::{String, ToString};
use alloc::vec::Vec;
use nom::IResult;
use nom::branch::alt;
use nom::bytes::complete::{take, take_until, take_till1};
use nom::bytes::streaming::tag;
use nom::character::complete::{char, multispace0, multispace1, one_of, line_ending, not_line_ending};
use nom::combinator::map;
use nom::error::ParseError;
use nom::multi::{many_till, many0, separated_list0};
use nom::number::complete::le_u32;
use nom::number::streaming::le_u64;
use nom::sequence::{preceded, delimited, tuple};
use serde::de::value::StrDeserializer;
use serde::{Deserializer, de, forward_to_deserialize_any, Deserialize};
use snafu::{Snafu, ResultExt, ensure};

// Vocabulary: in this crate we use the term "sheet" to describe a block of exactly 512 bytes
// (to avoid confusion around the word "sector")

// spec: https://github.com/libyal/libvslvm/blob/main/documentation/Logical%20Volume%20Manager%20(LVM)%20format.asciidoc#2-physical-volume-label

pub struct Lvm2<T> {
    reader: T,
}

#[derive(Debug, Snafu)]
pub enum Error {
    Io { source: acid_io::Error },
    WrongMagic,
    ParseError { error: String },
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
struct MetadataElements(Vec<MetadataElement>);
impl MetadataElements {
    pub fn parse(input: &str) -> IResult<&str, Self> {
        map(many0(MetadataElement::parse), MetadataElements)(input)
    }
}

#[derive(Debug)]
enum MetadataElement {
    Group {
        name: String,
        contents: MetadataElements,
    },
    Value {
        name: String,
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
impl MetadataElement {
    pub fn name(&self) -> &str {
        match self {
            MetadataElement::Group { name, .. } => &name,
            MetadataElement::Value { name, .. } => &name,
        }
    }
    pub fn parse(input: &str) -> IResult<&str, Self> {
        delimited(
            whitespace,
            alt((
                map(tuple((parse_ident, whitespace, char('='), whitespace, MetadataValue::parse)), |(k, _, _, _, v)| MetadataElement::Value { name: k.to_owned(), value: v }),

                map(tuple((parse_ident, whitespace, char('{'), MetadataElements::parse, char('}'))), |(k, _, _, contents, _)| MetadataElement::Group { name: k.to_owned(), contents })
            )),
            whitespace
        )(input)
    }
}

impl<'de, 'a> de::Deserializer<'de> for &'a MetadataElements {
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
struct MetadataElementsAccess<'a>(core::iter::Peekable<alloc::slice::Iter<'a, MetadataElement>>);

impl<'de, 'a> de::MapAccess<'de> for MetadataElementsAccess<'a> {
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

impl<T: Read + Seek> Lvm2<T> {
    pub fn open(mut reader: T) -> Result<Self, Error> {
        reader.seek(acid_io::SeekFrom::Start(512)).context(IoSnafu)?; // skip zero sheet

        let mut buf = [0u8; 512];
        reader.read_exact(&mut buf).context(IoSnafu)?; // read header
        tracing::trace!(?buf);

        let (_, vhl) = PhysicalVolumeLabelHeader::parse(&buf).map_err(|e| Error::ParseError { error: e.to_string() })?;
        tracing::trace!(?vhl);
        let (_, pvh) = PhysicalVolumeHeader::parse(&buf[(vhl.data_offset as usize)..]).map_err(|e| Error::ParseError { error: e.to_string() })?;
        tracing::trace!(?pvh);

        reader.seek(acid_io::SeekFrom::Start(pvh.metadata_descriptors[0].offset)).context(IoSnafu)?; // skip zero sheet
        reader.read_exact(&mut buf).context(IoSnafu)?;
        let (_, mah) = MetadataAreaHeader::parse(&buf).map_err(|e| Error::ParseError { error: e.to_string() })?;
        tracing::trace!(?mah);

        let mut metadata = String::new();
        reader.seek(acid_io::SeekFrom::Start(pvh.metadata_descriptors[0].offset + mah.location_descriptors[0].data_area_offset)).context(IoSnafu)?; // skip zero sheet
        reader.by_ref().take(mah.location_descriptors[0].data_area_size).read_to_string(&mut metadata).context(IoSnafu)?;
        tracing::debug!(%metadata);

        let (trailing_garbage, metadata) = MetadataElements::parse(&metadata).unwrap();
        tracing::debug!(?trailing_garbage, ?metadata);

        let meta_root = ForceDeTypedMap::<String, MetadataRoot>::deserialize(&metadata).unwrap();
        tracing::debug!(?meta_root);


        Ok(Self { reader })
    }
    pub fn foo(&mut self) -> Result<(), Error> {
        /*let mut buf = [0u8; 512];
        self.reader.read_exact(&mut buf)?; // first there is a 512-byte block of zeroes
        self.reader.read_exact(&mut buf)?; // then the actual header

        tracing::trace!(header = ?buf);
        */



        Ok(())
    }
}

#[derive(Debug)]
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


#[derive(Deserialize, Debug)]
pub struct MetadataRoot {
    pub id: String,
    pub extent_size: u64,
    pub physical_volumes: BTreeMap<String, PVDesc>,
    pub logical_volumes: BTreeMap<String, LVDesc>,
}
#[derive(Deserialize, Debug)]
pub struct PVDesc {
    pub id: String,
    pub device: String,
}
#[derive(Deserialize, Debug)]
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
#[derive(Deserialize, Debug)]
pub struct LVSegmentDesc {
    pub start_extent: u64,
    pub extent_count: u64,
    pub r#type: String,
    pub stripe_count: Option<usize>,
    pub stripe_size: Option<usize>,
    pub stripes: Option<(String, u64)>,
    pub raid0_lvs: Option<Vec<String>>,
}
