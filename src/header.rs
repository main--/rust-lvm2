use alloc::borrow::ToOwned;
use alloc::string::String;
use alloc::vec::Vec;
use nom::IResult;
use nom::bytes::complete::take;
use nom::bytes::streaming::tag;
use nom::error::ParseError;
use nom::multi::many_till;
use nom::number::complete::le_u32;
use nom::number::streaming::le_u64;


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
