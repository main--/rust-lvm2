use alloc::borrow::ToOwned;
use alloc::string::String;
use alloc::vec::Vec;
use nom::IResult;
use nom::branch::alt;
use nom::bytes::complete::{take_until, take_till1};
use nom::character::complete::{char, multispace0, line_ending, not_line_ending, i64};
use nom::combinator::map;
use nom::multi::{many0, separated_list0};
use nom::sequence::{preceded, delimited, tuple};
use serde::de::value::StrDeserializer;
use serde::{de, forward_to_deserialize_any};

// a little metadata parser
#[derive(Debug)]
pub enum MetadataValue {
    String(String),
    Number(i64),
    Array(Vec<MetadataValue>),
}
impl MetadataValue {
    pub fn parse(input: &str) -> IResult<&str, Self> {
        delimited(
            whitespace,
            alt((
                map(i64, MetadataValue::Number),
                map(delimited(char('['), separated_list0(char(','), MetadataValue::parse), char(']')), MetadataValue::Array),
                map(delimited(char('"'), take_until("\""), char('"')), |x: &str| MetadataValue::String(x.to_owned()))
            )),
            whitespace
        )(input)
    }
}
#[derive(Debug)]
pub struct MetadataElements<'a>(Vec<MetadataElement<'a>>);
impl<'a> MetadataElements<'a> {
    pub fn parse(input: &'a str) -> IResult<&'a str, Self> {
        map(many0(MetadataElement::parse), MetadataElements)(input)
    }
}

#[derive(Debug)]
pub enum MetadataElement<'a> {
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
pub struct MetadataElementsAccess<'a, 'de>(core::iter::Peekable<alloc::slice::Iter<'a, MetadataElement<'de>>>);

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
pub struct MetadataValuesAccess<'a>(alloc::slice::Iter<'a, MetadataValue>);
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