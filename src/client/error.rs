use bson;
use client::coll::options::WriteModel;
use std::{error, fmt, io, sync};

pub type MongoResult<T> = Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    WriteError(WriteException),
    BulkWriteError(BulkWriteException),    
    EncoderError(bson::EncoderError),
    IoError(io::Error),
    //ArgumentError(String),  // For things like replace, update validation
    LockError,
    ReadError,
    Default(String),
}

#[derive(Debug)]
pub struct WriteException {
    kind: WriteExceptionKind,
    code: i32,
    message: String,
}

#[derive(Debug)]
pub enum WriteExceptionKind {
    WriteConcernError(bson::Document),
    WriteError,
}

#[derive(Debug)]
pub struct BulkWriteException {
    processed_requests: Vec<WriteModel>,
    unprocessed_requests: Vec<WriteModel>,
    write_concern_error: Option<WriteException>,
    write_errors: Vec<BulkWriteError>,
    message: String,
}

#[derive(Debug)]
pub struct BulkWriteError {
    index: i32,
    request: Option<WriteModel>,
}

impl<'a> From<&'a str> for Error {
    fn from(s: &str) -> Error {
        Error::Default(s.to_owned())
    }
}

impl From<String> for Error {
    fn from(s: String) -> Error {
        Error::Default(s.to_owned())
    }
}

impl From<WriteException> for Error {
    fn from(err: WriteException) -> Error {
        Error::WriteError(err)
    }
}

impl From<BulkWriteException> for Error {
    fn from(err: BulkWriteException) -> Error {
        Error::BulkWriteError(err)
    }
}

impl From<bson::EncoderError> for Error {
    fn from(err: bson::EncoderError) -> Error {
        Error::EncoderError(err)
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::IoError(err)
    }
}

impl<T> From<sync::PoisonError<T>> for Error {
    fn from(_: sync::PoisonError<T>) -> Error {
        Error::LockError
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            &Error::WriteError(ref inner) => inner.fmt(fmt),
            &Error::BulkWriteError(ref inner) => inner.fmt(fmt),
            &Error::EncoderError(ref inner) => inner.fmt(fmt),
            &Error::IoError(ref inner) => inner.fmt(fmt),
            &Error::LockError => write!(fmt, "Socket lock poisoned."),
            &Error::ReadError => write!(fmt, "Read error"),
            &Error::Default(ref inner) => inner.fmt(fmt),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match self {
            &Error::WriteError(ref inner) => inner.description(),
            &Error::BulkWriteError(ref inner) => inner.description(),
            &Error::EncoderError(ref inner) => inner.description(),
            &Error::IoError(ref inner) => inner.description(),
            &Error::LockError => "Socket lock poisoned",
            &Error::ReadError => "ReadError",
            &Error::Default(ref inner) => &inner,
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match self {
            &Error::WriteError(ref inner) => Some(inner),
            &Error::BulkWriteError(ref inner) => Some(inner),
            &Error::EncoderError(ref inner) => Some(inner),
            &Error::IoError(ref inner) => Some(inner),
            &Error::LockError => Some(self),
            &Error::ReadError => Some(self),
            &Error::Default(_) => Some(self),
        }
    }
}

impl error::Error for WriteException {
    fn description(&self) -> &str {
        &self.message
    }

    fn cause(&self) -> Option<&error::Error> {
        Some(self)
    }
}

impl error::Error for BulkWriteException {
    fn description(&self) -> &str {
        &self.message
    }

    fn cause(&self) -> Option<&error::Error> {
        match self.write_concern_error {
            Some(ref err) => Some(err),
            None => Some(self),
        }
    }
}

impl fmt::Display for WriteException {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{:?} ({}): {}\n", self.kind, self.code, self.message)
    }
}

impl fmt::Display for BulkWriteException {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(fmt, "BulkWriteException:\n"));

        try!(write!(fmt, "Processed Requests:\n"));
        for v in &self.processed_requests {
            try!(write!(fmt, "{:?}\n", v));
        }

        try!(write!(fmt, "Unprocessed Requests:\n"));
        for v in &self.unprocessed_requests {
            try!(write!(fmt, "{:?}\n", v));
        }

        match self.write_concern_error {
            Some(ref error) => try!(write!(fmt, "{}\n", error)),
            None => (),
        }

        for v in &self.write_errors {
            try!(write!(fmt, "{}\n", v));
        }

        Ok(())
    }
}

impl fmt::Display for BulkWriteError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(fmt, "BulkWriteError at index {}: ", self.index));
        match self.request {
            Some(ref request) => try!(write!(fmt, "Failed to execute request {:?}\n", request)),
            None => try!(write!(fmt, "No additional error information was received.\n")),
        }
        Ok(())
    }
}

impl WriteException {
    pub fn new<T: ToString>(kind: WriteExceptionKind, code: i32, message: T) -> WriteException {
        WriteException {
            kind: kind,
            code: code,
            message: message.to_string(),
        }
    }

    //pub fn with_document(doc: bson::Document) -> WriteException {
    //
    //}
}

impl BulkWriteException {
    pub fn new<T: ToString>(processed: Vec<WriteModel>, unprocessed: Vec<WriteModel>,
                            write_errors: Vec<BulkWriteError>, write_concern_error: Option<WriteException>)
                            -> BulkWriteException {        
        BulkWriteException {
            processed_requests: processed,
            unprocessed_requests: unprocessed,
            write_concern_error: write_concern_error,
            write_errors: write_errors,
            message: "".to_owned(),
        }
    }

    //pub fn with_document(doc: bson::Document) -> BulkWriteException {
    // Do  From<bson::Document> instead?
    //}
}
