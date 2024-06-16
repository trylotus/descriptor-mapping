pub mod base64a {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine;
    use serde::{Deserialize, Serialize};
    use serde::{Deserializer, Serializer};

    #[allow(unused)]
    pub fn serialize<S: Serializer>(v: &Vec<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        let base64a: Vec<String> = v.iter().map(|e| STANDARD.encode(e)).collect();
        base64a.serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<Vec<u8>>, D::Error> {
        let base64a = Vec::<String>::deserialize(d)?;
        let mut vv = Vec::<Vec<u8>>::new();
        for base64 in base64a {
            let v = STANDARD
                .decode(base64.as_bytes())
                .map_err(|err| serde::de::Error::custom(err))?;
            vv.push(v);
        }
        Ok(vv)
    }
}
