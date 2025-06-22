//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use rand;
use rsa::signature::RandomizedSigner;
use rsa::{Pkcs1v15Sign, RsaPrivateKey};
use sha2::{Digest, Sha256};

pub fn sign(private_key: RsaPrivateKey, data: &[u8]) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data);
    let hashed = hasher.finalize();

    let mut rng = rand::thread_rng();
    let signature = private_key
        .sign_with_rng(&mut rng, Pkcs1v15Sign::new::<Sha256>(), &hashed)
        .expect("Failed to sign data");

    signature.to_vec()
}
