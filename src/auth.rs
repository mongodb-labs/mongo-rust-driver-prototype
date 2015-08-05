use bson::Bson::{self, Binary};
use bson::spec::BinarySubtype::Generic;
use CommandType::Suppressed;
use crypto::digest::Digest;
use crypto::hmac::Hmac;
use crypto::mac::Mac;
use crypto::md5::Md5;
use crypto::pbkdf2;
use crypto::sha1::Sha1;
use db::{Database, ThreadedDatabase};
use error::Error::{DefaultError, MaliciousServerError, ResponseError};
use error::MaliciousServerErrorType;
use error::Result;
use rustc_serialize::base64::{self, FromBase64, ToBase64};
use textnonce::TextNonce;

const B64_CONFIG : base64::Config = base64::Config { char_set: base64::CharacterSet::Standard,
                                                     newline: base64::Newline::LF,
                                                     pad: true, line_length: None };

pub trait Authenticator {
    fn auth(&self, user: &str, password: &str) -> Result<()>;
}

macro_rules! start {
    ($db:expr, $user:expr) => {{
        let text_nonce = match TextNonce::sized(64) {
            Ok(text_nonce) => text_nonce,
            Err(string) => return Err(DefaultError(string))
        };

        let nonce = format!("{}", text_nonce);
        let message = format!("n={},r={}", $user, nonce);
        let bytes = format!("n,,{}", message).into_bytes();
        let binary = Binary(Generic, bytes);

        let start_doc = doc! {
            "saslStart" => 1,
            "autoAuthorize" => 1,
            "payload" => binary,
            "mechanism" => "SCRAM-SHA-1"
        };

        let doc = try!($db.command(start_doc, Suppressed));

        let data = match doc.get("payload") {
            Some(&Binary(_, ref payload)) => payload.to_owned(),
            _ => return Err(ResponseError("Invalid payload returned".to_owned()))
        };

        let id = match doc.get("conversationId") {
            Some(bson) => bson.clone(),
            None => return Err(ResponseError("No conversationId returned".to_owned()))
        };

        match String::from_utf8(data) {
            Ok(string) => (message, string, nonce, id),
            Err(_) => return Err(ResponseError("Invalid UTF-8 payload returned".to_owned()))
        }
    }};
}

macro_rules! next {
    ($db:expr, $message:expr, $response:expr, $password:expr, $nonce:expr, $id:expr) => {{
        let (rnonce_opt, salt_opt, i_opt) = scan_fmt!($response, "r={},s={},i={}", String, String, u32);

        let rnonce_b64 = match rnonce_opt {
            Some(val) => val,
            None => return Err(ResponseError("Invalid rnonce returned".to_owned()))
        };

        if !rnonce_b64.starts_with(&$nonce) {
            return Err(MaliciousServerError(MaliciousServerErrorType::InvalidRnonce))
        }

        let salt_b64 = match salt_opt {
            Some(val) => val,
            None => return Err(ResponseError("Invalid salt returned".to_owned()))
        };

        let salt = match salt_b64.from_base64() {
            Ok(val) => val,
            Err(_) => return Err(ResponseError("Invalid base64 salt returned".to_owned()))
        };


        let i = match i_opt {
            Some(val) => val,
            None => return Err(ResponseError("Invalid iteration count returned".to_owned()))
        };

        // let password_bytes = $password.into_bytes();
        let mut md5 = Md5::new();
        md5.input_str(&$password[..]);
        let hashed_password = md5.result_str();

        let mut hmac = Hmac::new(Sha1::new(), hashed_password.as_bytes());
        let mut salted_password : Vec<_> = (0..hmac.output_bytes()).map(|_| 0).collect();
        pbkdf2::pbkdf2(&mut hmac, &salt[..], i, &mut salted_password);

        let mut server_key_hmac = Hmac::new(Sha1::new(), &salted_password[..]);
        let server_key_bytes = "Server Key".as_bytes();
        server_key_hmac.input(server_key_bytes);
        let server_key = server_key_hmac.result().code().to_owned();

        let mut client_key_hmac = Hmac::new(Sha1::new(), &salted_password[..]);
        let client_key_bytes = "Client Key".as_bytes();
        client_key_hmac.input(client_key_bytes);
        let client_key = client_key_hmac.result().code().to_owned();

        let mut stored_key_sha1 = Sha1::new();
        stored_key_sha1.input(&client_key[..]);

        let mut stored_key : Vec<_> = (0..stored_key_sha1.output_bytes()).map(|_| 0).collect();
        stored_key_sha1.result(&mut stored_key);

        let without_proof = format!("c=biws,r={}", rnonce_b64);
        let auth_message = format!("{},{},{}", $message, $response, without_proof);

        let mut signature_hmac = Hmac::new(Sha1::new(), &stored_key[..]);
        signature_hmac.input(auth_message.as_bytes());
        let signature = signature_hmac.result().code().to_owned();

        if client_key.len() != signature.len() {
            return Err(DefaultError("Generated client and/or server key is invalid".to_owned()));
        }

        let mut proof = vec![];
        for i in 0..client_key.len() {
            proof.push(client_key[i] ^ signature[i]);
        }

        let b64_proof = proof.to_base64(B64_CONFIG);
        let final_message = format!("{},p={}", without_proof, b64_proof);
        let binary = Binary(Generic, final_message.into_bytes());

        let next_doc = doc! {
            "saslContinue" => 1,
            "payload" => binary,
            "conversationId" => ($id.clone())
        };

        let mut doc = try!($db.command(next_doc, Suppressed));

        let final_doc = doc! {
            "saslContinue" => 1,
            "payload" => (Binary(Generic, vec![])),
            "conversationId" => $id
        };

        let mut server_signature_hmac = Hmac::new(Sha1::new(), &server_key[..]);
        server_signature_hmac.input(auth_message.as_bytes());
        let server_signature = server_signature_hmac.result().code().to_owned();

        loop {
            if let Some(&Bson::Boolean(true)) = doc.get("done") {
                break;
            }

            if let Some(&Binary(_, ref payload)) = doc.get("payload") {
                let payload_str = String::from_utf8_lossy(payload);
                let verifier = match scan_fmt!(&payload_str[..], "v={}", String) {
                    Some(string) => string,
                    None => return Err(MaliciousServerError(MaliciousServerErrorType::NoServerSignature)),
                };

                if verifier.ne(&server_signature.to_base64(B64_CONFIG)[..]) {
                    return Err(MaliciousServerError(MaliciousServerErrorType::InvalidServerSignature));
                }
            }

            doc = try!($db.command(final_doc.clone(), Suppressed));
        }
    }};
}

impl Authenticator for Database {
    fn auth(&self, user: &str, password: &str) -> Result<()> {
        let (message, response, nonce, id) = start!(self, user);
        let full_password = format!("{}:mongo:{}", user, password);
        next!(self, message, &response[..], full_password, nonce, id);
        Ok(())
    }
}
