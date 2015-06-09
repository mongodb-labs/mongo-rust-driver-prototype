pub struct OpQueryFlags {
    pub tailable_cursor: bool,    // Bit 1
    pub slave_ok: bool,           // Bit 2
    pub oplog_relay: bool,        // Bit 3
    pub no_cursor_timeout: bool,  // Bit 4
    pub await_data: bool,         // Bit 5
    pub exhaust: bool,            // Bit 6
    pub partial: bool,            // Bit 7

    // All other bits must be 0
}

pub struct OpInsertFlags {
    pub continue_on_error: bool,  // Bit 0

    // All othe bits must be 0
}


pub trait Flags<T> {
    fn no_flags() -> T;
    fn to_i32(&self) -> i32;
}

impl Flags<OpQueryFlags> for OpQueryFlags {
    fn no_flags() -> OpQueryFlags {
        OpQueryFlags::new(false, false, false, false, false, false, false)
    }

    fn to_i32(&self) -> i32 {
        let mut i = 0 as i32;

        if self.tailable_cursor {
            let bit = 1 << 1;

            i |= bit;
        }

        if self.slave_ok {
            let bit = 1 << 2;

            i |= bit;
        }

        if self.oplog_relay {
            let bit = 1 << 3;

            i |= bit;
        }

        if self.no_cursor_timeout {
            let bit = 1 << 4;

            i |= bit;
        }

        if self.await_data {
            let bit = 1 << 5;

            i |= bit;
        }

        if self.exhaust {
            let bit = 1 << 6;

            i |= bit;
        }

        if self.partial {
            let bit = 1 << 7;

            i |= bit;
        }

        i
    }
}

impl Flags<OpInsertFlags> for OpInsertFlags {
    fn no_flags() -> OpInsertFlags {
        OpInsertFlags { continue_on_error: false }
    }

    fn to_i32(&self) -> i32 {
        if self.continue_on_error {
            1
        } else {
            0
        }
    }
}
