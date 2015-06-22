#[macro_export]
macro_rules! val_or_err {
    ( $m:expr, $p:pat => $r:expr, $e:expr ) => {
        match $m {
            $p => $r,
            _ => return Err($e.to_owned())
        }
    };
}

#[macro_export]
macro_rules! var_match {
    ( $v:expr, $p:pat => $r:expr ) => {
        match $v {
            $p => $r,
            _ => false
        }
    };
}
