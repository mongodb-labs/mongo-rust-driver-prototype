struct MongoCursor {
    id : i64,
    conn : Option<@MongoConnection>,
    flags : i32, // tailable, slave_ok, oplog_replay, no_timeout, await_data, exhaust, partial, can set during find() too
}

impl Iterator<util::Json> for MongoCursor {
    fn next(&self) -> util::Json;
}

fn cursor_tmp() -> MongoCursor { MongoCursor {id:0, conn:None, flags:None} }

impl MongoCursor {
    fn explain(&self)/* -> Json */ { }

    fn hint(&self, index : MongoIndex) -> MongoCursor { cursor_tmp() }

    fn sort(&self/*, order : Json*/) -> MongoCursor { cursor_tmp() }

    fn limit(&self, n : int) -> MongoCursor { cursor_tmp() }

    fn skip(&self, n : int) -> MongoCursor { cursor_tmp() }

    fn to_array(&self) -> ~[Json] {

    }
}
