#[macro_export]
macro_rules! doc {
    ( $( $k:expr => $v: expr),* ) => {
        {
            let mut doc = Document::new();
            $(
                doc.insert($k.to_owned(), $v);
            )*
            doc
        }
    };
}

#[macro_export]
macro_rules! nested_doc {
    ( $( $k:expr => $v: expr),* ) => {
        Bson::Document(doc!(
            $( $k => $v),*
        ))
    }
}

// Example for future documentation use
//
// ```
// doc! {
//     "_id" => Bson::I32(1),
//     "x" => Bson::I32(11),
//     "$filter" => nested_doc! {
//         "_id" => nested_doc! {
//             "$gt" => 1,
//             "$lt" => 6
//         }
//     }
// }
// ```
