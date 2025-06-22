//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use rsa::{
    pkcs8::{DecodePrivateKey, EncodePrivateKey},
    RsaPrivateKey,
};
use std::error::Error;
use std::fmt::Debug;

use crate::auth_common::file_utils::expand_user_home;

pub trait Supplier: Send + Sync + Debug + SupplierClone {
    fn get_key(&self) -> Result<RsaPrivateKey, Box<dyn Error>>;
}

pub trait SupplierClone {
    fn clone_box(&self) -> Box<dyn Supplier>;
}

impl<T> SupplierClone for T
where
    T: 'static + Supplier + Clone,
{
    fn clone_box(&self) -> Box<dyn Supplier> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Supplier> {
    fn clone(&self) -> Box<dyn Supplier> {
        self.clone_box()
    }
}

#[derive(Debug, Clone)]
pub struct PrivateKeySupplier {
    key_content: String,
    passphrase: Option<Vec<char>>,
}

impl PrivateKeySupplier {
    #[allow(dead_code)]
    pub fn new(key_content: String) -> Self {
        PrivateKeySupplier {
            key_content,
            passphrase: None,
        }
    }

    #[allow(dead_code)]
    pub fn new_with_passphrase(key_content: String, passphrase: Option<Vec<char>>) -> Self {
        PrivateKeySupplier {
            key_content,
            passphrase,
        }
    }
}

impl Supplier for PrivateKeySupplier {
    fn get_key(&self) -> Result<RsaPrivateKey, Box<dyn Error>> {
        if self.passphrase.is_some() {
            return Err(
                "Passphrase-protected keys are not supported in this simplified implementation"
                    .into(),
            );
        }
        Ok(RsaPrivateKey::from_pkcs8_pem(&self.key_content)?)
    }
}

#[derive(Debug, Clone)]
pub struct FilePrivateKeySupplier {
    key_path: String,
    passphrase: Option<Vec<char>>,
}

impl FilePrivateKeySupplier {
    #[allow(dead_code)]
    pub fn new(key_path: String) -> Self {
        FilePrivateKeySupplier {
            key_path,
            passphrase: None,
        }
    }

    #[allow(dead_code)]
    pub fn new_with_passphrase(key_path: String, passphrase: Option<Vec<char>>) -> Self {
        FilePrivateKeySupplier {
            key_path,
            passphrase,
        }
    }
}

impl Supplier for FilePrivateKeySupplier {
    fn get_key(&self) -> Result<RsaPrivateKey, Box<dyn Error>> {
        // TODO: cache key in memory, don't go to disk for every request
        let key_content = match std::fs::read_to_string(&expand_user_home(&self.key_path)) {
            Ok(kc) => kc,
            Err(e) => {
                return Err(format!(
                    "Unable to read private fey file '{}': {}",
                    self.key_path,
                    e.to_string()
                )
                .as_str()
                .into());
            }
        };
        if self.passphrase.is_some() {
            return Err(
                "Passphrase-protected keys are not supported in this simplified implementation"
                    .into(),
            );
        }
        Ok(RsaPrivateKey::from_pkcs8_pem(&key_content)?)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use rsa::RsaPrivateKey;

    #[test]
    #[should_panic]
    fn test_file_private_key_supplier_with_key_file_missing() {
        let file_key_provider = FilePrivateKeySupplier::new(String::from("~/invalid_path"));
        file_key_provider.get_key().unwrap();
    }

    #[test]
    #[should_panic]
    fn test_private_key_supplier_with_invalid_key() {
        let key_supplier = PrivateKeySupplier::new(String::from("invalid key"));
        key_supplier.get_key().unwrap();
    }

    #[test]
    #[should_panic]
    fn test_private_key_with_invalid_passphrase() {
        let private_key_supplier = PrivateKeySupplier::new_with_passphrase(
            String::from("some key content"),
            Some("Invalid passphrase".chars().collect::<Vec<_>>()),
        );
        private_key_supplier.get_key().unwrap();
    }

    #[test]
    fn test_private_key_with_valid_passphrase() {
        // This test is simplified since we no longer support passphrases
        let mut rng = rand::thread_rng();
        let bits = 2048;
        let rsa = RsaPrivateKey::new(&mut rng, bits).unwrap();
        let key_pem = rsa.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();

        let private_key_supplier = PrivateKeySupplier::new(key_pem.to_string());
        private_key_supplier.get_key().unwrap();
    }
}
