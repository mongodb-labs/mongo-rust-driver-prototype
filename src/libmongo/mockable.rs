use extra::future;

pub trait Mockable {
    fn mock(state: int) -> Self;
}

impl Mockable for () {
    fn mock(_: int) -> () { () }
}
impl Mockable for char {
    fn mock(_: int) -> char { 0 as char }
}

impl Mockable for int {
    fn mock(_: int) -> int { 0 }
}

impl Mockable for i8 {
    fn mock(_: int) -> i8 { Mockable::mock::<int>(0) as i8 }
}

impl Mockable for uint {
    fn mock(_: int) -> uint { 0u }
}

impl Mockable for u8 {
    fn mock(_: int) -> u8 { Mockable::mock::<uint>(0) as u8 }
}

impl Mockable for float {
    fn mock(_: int) -> float { 0f }
}

impl Mockable for ~str {
    fn mock(_: int) -> ~str { ~"" }
}

impl<T:Mockable> Mockable for ~T {
    fn mock(state: int) -> ~T {
        ~Mockable::mock::<T>(state)
    }
}

impl<T:Mockable> Mockable for @T {
    fn mock(state: int) -> @T {
        @Mockable::mock::<T>(state)
    }
}

impl<T:Mockable> Mockable for ~[T] {
    fn mock(state: int) -> ~[T] {
        ~[Mockable::mock::<T>(state)]
    }
}

impl<T:Mockable> Mockable for Option<T> {
    fn mock(state: int) -> Option<T> {
        if state == 0 {
            Some(Mockable::mock::<T>(state))
        }
        else {
            None
        }
    }
}

impl<T:Mockable,U:Mockable> Mockable for Result<T,U> {
    fn mock(state: int) -> Result<T,U> {
        if state == 0 {
            Ok(Mockable::mock::<T>(state))
        }
        else if state == 1 {
            Err(Mockable::mock::<U>(state))
        }
        else {
            fail!("mocking error: invalid state from Result")
        }
    }
}

impl<T:Mockable + Send> Mockable for future::Future<T> {
    fn mock(state: int) -> future::Future<T> {
        do future::spawn { Mockable::mock::<T>(state) }
    }
}
