#[macro_export]
macro_rules! val_or_err {
    ( $exp:expr, $pat:pat => $ret:expr, $err:expr ) => {
        match $exp {
            $pat => $ret,
            _ => return Err($err.to_owned())
        }
    };
}

#[macro_export]
macro_rules! var_match {
    ( $exp:expr, $pat:pat => $ret:expr ) => {
        match $exp {
            $pat => $ret,
            _ => false
        }
    };
}
