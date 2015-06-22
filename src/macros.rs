use bson::{Bson, Document};

#[macro_export]
macro_rules! add_to_doc {
    ($doc:expr, $key:expr => ($val:expr)) => {{
        $doc.insert($key.to_owned(), $val);
    }};

    ($doc:expr, $key:expr => [$($val:expr),*]) => {{
        let vec = vec![$($val),*];
        $doc.insert($key.to_owned(), Bson::Array(vec));
    }};

    ($doc:expr, $key:expr => { $($k:expr => $v:tt),* }) => {{
        $doc.insert($key.to_owned(), Bson::Document(doc! {
            $(
                $k => $v
            ),*
        }));
    }};
}

#[macro_export]
macro_rules! doc {
    ( $($key:expr => $val:tt),* ) => {{
        let mut document = Document::new();

        $(
            add_to_doc!(document, $key => $val);
        )*

        document
    }};
}
