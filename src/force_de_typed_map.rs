use core::marker::PhantomData;

use alloc::collections::BTreeMap;
use serde::{Deserialize, Deserializer, de};


#[derive(Clone, Debug)]
pub struct ForceDeTypedMap<K, V>(pub BTreeMap<K, V>);

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
