use rustc_serialize::json::Json;

pub trait Decodable {
    fn decode(json: &mut Json) -> Result<Self, String>;
}
