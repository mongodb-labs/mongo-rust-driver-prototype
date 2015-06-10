pub struct OpReplyFlags {
    cursor_not_found: bool, // Bit 0
    query_failure: bool,    // Bit 1
    await_capable: bool,    // Bit 3

    // All bits remaining must be 0
}

impl OpReplyFlags {
    pub fn from_i32(i: i32) -> OpReplyFlags {
        let cnf = (i & 1) != 0;
        let qf = (i & (1 << 1)) != 0;
        let ac = (i & (1 << 3)) != 0;

        OpReplyFlags { cursor_not_found: cnf, query_failure: qf, await_capable: ac }
    }
}

pub struct OpInsertFlags {
    pub continue_on_error: bool,  // Bit 0

    // All bits remaining must be 0
}

pub struct OpQueryFlags {
    pub tailable_cursor: bool,    // Bit 1
    pub slave_ok: bool,           // Bit 2
    pub oplog_relay: bool,        // Bit 3
    pub no_cursor_timeout: bool,  // Bit 4
    pub await_data: bool,         // Bit 5
    pub exhaust: bool,            // Bit 6
    pub partial: bool,            // Bit 7

    // All bits remaining must be 0
}


impl OpInsertFlags {
    pub fn no_flags() -> OpInsertFlags {
        OpInsertFlags { continue_on_error: false }
    }

    pub fn to_i32(&self) -> i32 {
        if self.continue_on_error {
            1
        } else {
            0
        }
    }
}

impl OpQueryFlags {
    pub fn no_flags() -> OpQueryFlags {
        OpQueryFlags { tailable_cursor: false, slave_ok: false,
                       oplog_relay: false, no_cursor_timeout: false,
                       await_data: false, exhaust: false, partial: false }
    }

    pub fn to_i32(&self) -> i32 {
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
