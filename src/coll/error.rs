use bson::{self, Bson};
use super::options::WriteModel;
use common::WriteConcern;
use {Error, Result};
use std::{error, fmt};

/// The error type for Write-related MongoDB operations.
#[derive(Debug, Clone)]
pub struct WriteException {
    pub write_concern_error: Option<WriteConcernError>,
    pub write_error: Option<WriteError>,
    pub message: String,
}

/// The error struct for a write-concern related error.
#[derive(Debug, Clone)]
pub struct WriteConcernError {
    pub code: i32,
    pub details: WriteConcern,
    pub message: String,
}

/// The error struct for a write-related error.
#[derive(Debug, Clone)]
pub struct WriteError {
    pub code: i32,
    pub message: String,
}

/// The error struct for Bulk-Write related MongoDB operations.
#[derive(Debug, Clone)]
pub struct BulkWriteException {
    pub processed_requests: Vec<WriteModel>,
    pub unprocessed_requests: Vec<WriteModel>,
    pub write_errors: Vec<BulkWriteError>,
    pub write_concern_error: Option<WriteConcernError>,
    pub message: String,
}

/// The error struct for a single bulk-write step, indicating the request
/// and its index in the original bulk-write request.
#[derive(Debug, Clone)]
pub struct BulkWriteError {
    pub index: i32,
    pub code: i32,
    pub message: String,
    pub request: Option<WriteModel>,
}

impl error::Error for WriteException {
    fn description(&self) -> &str {
        &self.message
    }

    fn cause(&self) -> Option<&error::Error> {
        None
    }
}

impl error::Error for BulkWriteException {
    fn description(&self) -> &str {
        &self.message
    }

    fn cause(&self) -> Option<&error::Error> {
        None
    }
}

impl fmt::Display for WriteException {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let ref wc_err = self.write_concern_error;
        let ref w_err = self.write_error;

        try!(write!(fmt, "WriteException:\n"));
        if wc_err.is_some() {
            try!(write!(fmt, "{:?}\n", wc_err));
        }

        if w_err.is_some() {
            try!(write!(fmt, "{:?}\n", w_err));
        }

        Ok(())
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
            Some(ref error) => try!(write!(fmt, "{:?}\n", error)),
            None => (),
        }

        for v in &self.write_errors {
            try!(write!(fmt, "{:?}\n", v));
        }

        Ok(())
    }
}

impl fmt::Display for BulkWriteError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(fmt, "BulkWriteError at index {} (code {}): {}\n",
                    self.index, self.code, self.message));

        match self.request {
            Some(ref request) => try!(write!(fmt, "Failed to execute request {:?}\n.", request)),
            None => try!(write!(fmt, "No additional error information was received.\n")),
        }

        Ok(())
    }
}

impl WriteException {
    /// Returns a new WriteException containing the given errors.
    pub fn new(wc_err: Option<WriteConcernError>, w_err: Option<WriteError>) -> WriteException {
        let mut s = match wc_err {
            Some(ref error) => format!("{:?}\n", error),
            None => "".to_owned(),
        };

        match w_err {
            Some(ref error) => s.push_str(&format!("{:?}\n", error)[..]),
            None => (),
        }

        WriteException {
            write_concern_error: wc_err,
            write_error: w_err,
            message: s.to_owned(),
        }
    }

    /// Downgrades a BulkWriteException into a WriteException, retrieving the
    /// last write error to emulate the behavior of continue_on_error.
    pub fn with_bulk_exception(bulk_exception: BulkWriteException) -> WriteException {
        let len = bulk_exception.write_errors.len();
        let write_error = match bulk_exception.write_errors.get(len - 1) {
            Some(ref e) => Some(WriteError::new(e.code, e.message.to_owned())),
            None => None,
        };

        WriteException::new(bulk_exception.write_concern_error, write_error)
    }

    /// Validates a single-write result.
    pub fn validate_write_result(result: bson::Document, write_concern: WriteConcern) -> Result<()> {
        let bulk_err_result = BulkWriteException::validate_bulk_write_result(result, write_concern);

        // Convert a bulk-write error into a write error, if it exists,
        // or propagate any other results.
        match bulk_err_result {
            Err(Error::BulkWriteError(bulk_exception)) => {
                Err(Error::WriteError(WriteException::with_bulk_exception(bulk_exception)))
            },
            Err(err) => Err(err),
            Ok(()) => Ok(()),
        }
    }
}

impl WriteConcernError {
    /// Returns a new WriteConcernError containing the provided error information.
    pub fn new<T: ToString>(code: i32, details: WriteConcern, message: T) -> WriteConcernError {
        WriteConcernError {
            code: code,
            details: details,
            message: message.to_string(),
        }
    }

    /// Parses a Bson document into a WriteConcernError with the provided write concern.
    pub fn parse(error: bson::Document, write_concern: WriteConcern) -> Result<WriteConcernError> {
        if let Some(&Bson::I32(ref code)) = error.get("code") {
            if let Some(&Bson::String(ref message)) = error.get("errmsg") {
                return Ok(WriteConcernError::new(*code, write_concern, message))
            }
        }
        Err(Error::ResponseError(format!("WriteConcernError document is invalid: {:?}", error)))
    }
}

impl WriteError {
    /// Returns a new WriteError containing the provided error information.
    pub fn new<T: ToString>(code: i32, message: T) -> WriteError {
        WriteError {
            code: code,
            message: message.to_string(),
        }
    }

    /// Parses a Bson document into a WriteError.
    pub fn parse(error: bson::Document) -> Result<WriteError> {
        if let Some(&Bson::I32(ref code)) = error.get("code") {
            if let Some(&Bson::String(ref message)) = error.get("errmsg") {
                return Ok(WriteError::new(*code, message))
            }
        }
        Err(Error::ResponseError(format!("WriteError document is invalid: {:?}", error)))
    }
}

impl BulkWriteError {
    /// Returns a new BulkWriteError containing the provided error information.
    pub fn new<T: ToString>(index: i32, code: i32, message: T, request: Option<WriteModel>) -> BulkWriteError {
        BulkWriteError {
            index: index,
            code: code,
            message: message.to_string(),
            request: request,
        }
    }

    /// Parses a Bson document into a BulkWriteError.
    pub fn parse(error: bson::Document) -> Result<BulkWriteError> {
        if let Some(&Bson::I32(ref index)) = error.get("index") {
            if let Some(&Bson::I32(ref code)) = error.get("code") {
                if let Some(&Bson::String(ref message)) = error.get("errmsg") {
                    return Ok(BulkWriteError::new(*index, *code, message, None))
                }
            }
        }
        Err(Error::ResponseError(format!("WriteError document is invalid: {:?}", error)))
    }
}

impl BulkWriteException {
    /// Returns a new BulkWriteException containing the provided error information.
    pub fn new(processed: Vec<WriteModel>, unprocessed: Vec<WriteModel>,
               write_errors: Vec<BulkWriteError>, write_concern_error: Option<WriteConcernError>)
               -> BulkWriteException {

        let mut s = match write_concern_error {
            Some(ref error) => format!("{:?}\n", error),
            None => "".to_owned(),
        };

        for v in &write_errors {
            s.push_str(&format!("{:?}\n", v)[..]);
        }

        BulkWriteException {
            processed_requests: processed,
            unprocessed_requests: unprocessed,
            write_concern_error: write_concern_error,
            write_errors: write_errors,
            message: s.to_owned(),
        }
    }

    /// Adds a model to the vector of unprocessed models
    pub fn add_unproccessed_model(&mut self, model: WriteModel) {
        self.unprocessed_requests.push(model);
    }

    /// Adds a vector of models to the vector of unprocessed models.
    pub fn add_unproccessed_models(&mut self, models: Vec<WriteModel>) {
        self.unprocessed_requests.extend(models.into_iter());
    }

    /// Adds the data contined by another BulkWriteException to this one.
    pub fn add_bulk_write_exception(&mut self,
                                    exception_opt: Option<BulkWriteException>,
                                    models: Vec<WriteModel>) -> bool {
        let exception = match exception_opt {
            Some(exception) => exception,
            None => {
                self.processed_requests.extend(models.into_iter());
                return true
            }
        };

        for req in exception.processed_requests.iter() {
            self.processed_requests.push(req.clone());
        }

        for req in exception.unprocessed_requests.iter() {
            self.unprocessed_requests.push(req.clone());
        }

        for err in exception.write_errors.iter() {
            self.write_errors.push(err.clone());
        }

        if exception.write_concern_error.is_some() {
            self.write_concern_error = exception.write_concern_error;
        };

        self.message.push_str(&exception.message);

        false
    }

    /// Validates a bulk write result.
    pub fn validate_bulk_write_result(result: bson::Document, write_concern: WriteConcern) -> Result<()> {

        // Parse out any write concern errors.
        let wc_err = if let Some(&Bson::Document(ref error)) = result.get("writeConcernError") {
            Some(try!(WriteConcernError::parse(error.clone(), write_concern)))
        } else {
            None
        };

        // Parse out any write errors.
        let w_errs = if let Some(&Bson::Array(ref errors)) = result.get("writeErrors") {
            if errors.is_empty() {
                return Err(Error::ResponseError(
                    "Server indicates a write error, but none were found.".to_owned()));
            }

            let mut vec = Vec::new();
            for err in errors {
                if let &Bson::Document(ref doc) = err {
                    vec.push(try!(BulkWriteError::parse(doc.clone())));
                } else {
                    return Err(Error::ResponseError(
                        "WriteError provided was not a bson document.".to_owned()));
                }
            }
            vec
        } else {
            Vec::new()
        };

        // Return a bulk-write error if any errors were found.
        if wc_err.is_none() && w_errs.is_empty() {
            Ok(())
        } else {
            Err(Error::BulkWriteError(BulkWriteException::new(Vec::new(), Vec::new(), w_errs, wc_err)))
        }
    }
}
