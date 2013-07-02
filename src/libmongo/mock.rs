use mockable::*;

macro_rules! mock (

    (with state $state:expr, $tr:ty => $strc:ident: $($fname:ident(&self) -> $rettype:ty)|*) => {
        impl $tr for $strc {
            $(
                fn $fname(&self) -> $rettype {
                    Mockable::mock::<$rettype>($state)
                }
            )*
        }
    };
    ($tr:ty => $strc:ident: $($fname:ident(&self) -> $rettype:ty)|*) => {
        impl $tr for $strc {
            $(
                fn $fname(&self) -> $rettype {
                    Mockable::mock::<$rettype>(self.state)
                }
            )*
        }
    };/*
    ($tr:ty => $strc:ident: $($fname:ident(&self, $($argname:ident: $argtype:ty),*) -> $rettype:ty s=$state:expr)|*) => {
         impl $tr for $strc {
            $(
                fn $fname(&self, $(_: $argtype)*) -> $rettype {
                    Mockable::mock::<$rettype>($state)
                }
            )*
        }
    };
    ($tr:ident => $strc:ident: $($fname:ident($($argname:ident: $argtype:ty),*) -> $rettype:ty s=$state:expr)|*) => {
        impl $tr for $strc {
            $(
                fn $fname($(_: $argtype)*) -> $rettype {
                    Mockable::mock::<$rettype>($state)
                }
            )*
        }
    };*/
)

trait FooTrait {
    fn bar(&self) -> int;
    fn qux(&self) -> char;
}

struct Baz;

mock!(with state 0, FooTrait => Baz: bar(&self) -> int| qux(&self) -> char)
