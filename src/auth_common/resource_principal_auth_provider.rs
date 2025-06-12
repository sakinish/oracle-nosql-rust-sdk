//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use base64ct::{Base64Unpadded, Encoding};
use openssl::pkey::Private;
use openssl::rsa::Rsa;
use serde_json::Value;
use std::env;
use std::error::Error;
use tracing::trace;
use tracing::debug;

use crate::auth_common::authentication_provider::AuthenticationProvider;

static EMPTY_STRING: &str = "";

// supported version for resource principal
static RP_VERSION_2_2: &str = "2.2";

// environment variable that specifies a resource principal version
static RP_VERSION_ENV: &str = "OCI_RESOURCE_PRINCIPAL_VERSION";

// environment variable that specifies a security token or a path to the token file
static RP_RPST_ENV: &str = "OCI_RESOURCE_PRINCIPAL_RPST";

// environment variable that specifies an RSA private key in pem format or a path to the key file
static RP_PRIVATE_PEM_ENV: &str = "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM";

// environment variable that specifies the passphrase to a key or a path to the passphrase file
static RP_PRIVATE_PEM_PASSPHRASE_ENV: &str = "OCI_RESOURCE_PRINCIPAL_PRIVATE_PEM_PASSPHRASE";

// environment variable that specifies a region
static RP_REGION_ENV: &str = "OCI_RESOURCE_PRINCIPAL_REGION";

// the key used to look up the resource tenancy in an RPST
static TENANCY_CLAIM_KEY: &str = "res_tenant";

#[derive(Debug, Clone)]
pub struct ResourcePrincipalAuthProvider {
    token: String,
    session_private_key: Rsa<Private>,
    tenancy_id: String,
    region: String,
    //expiration: u64, // seconds since the epoch
}

impl AuthenticationProvider for ResourcePrincipalAuthProvider {
    fn tenancy_id(&self) -> &str {
        &self.tenancy_id
    }
    fn fingerprint(&self) -> &str {
        EMPTY_STRING
    }
    fn user_id(&self) -> &str {
        EMPTY_STRING
    }
    fn private_key(&self) -> Result<Rsa<Private>, Box<dyn Error>> {
        // TODO: optimize away this clone
        Ok(self.session_private_key.clone())
    }
    fn region_id(&self) -> &str {
        &self.region
    }
    fn key_id(&self) -> String {
        self.token.clone()
    }
}

fn get_env(var: &str) -> Result<String, Box<dyn Error>> {
    let v = match env::var(var) {
        Ok(val) => val,
        Err(e) => {
            return Err(format!(
                "error reading environment variable '{}': {}",
                var,
                e.to_string()
            )
            .into())
        }
    };
    Ok(v)
}

impl ResourcePrincipalAuthProvider {
    pub fn new() -> Result<ResourcePrincipalAuthProvider, Box<dyn Error>> {
        let rp_version = get_env(RP_VERSION_ENV)?;
        if rp_version != RP_VERSION_2_2 {
            return Err(format!(
                "resource principal version '{}' incorrect: expected {}",
                rp_version, RP_VERSION_2_2
            )
            .as_str()
            .into());
        }

        Self::new_from_values(
            get_env(RP_RPST_ENV)?,
            get_env(RP_PRIVATE_PEM_ENV)?,
            get_env(RP_PRIVATE_PEM_PASSPHRASE_ENV).ok(),
            get_env(RP_REGION_ENV)?,
        )
    }

    pub fn new_from_values(
        rpst: String,
        private_pem: String,
        passphrase: Option<String>,
        region: String,
    ) -> Result<ResourcePrincipalAuthProvider, Box<dyn Error>> {
        debug!("🔧 ResourcePrincipalAuthProvider::new_from_values() starting");
        debug!("   📍 Region: {}", region);
        debug!("   🔑 RPST token length: {} chars", rpst.len());
        debug!("   🔑 RPST token (first 64 / last 64 chars): {}... / ...{}", 
               &rpst[..64.min(rpst.len())], 
               &rpst[(rpst.len().saturating_sub(64))..]);
        debug!("   🗝️  Private PEM length: {} chars", private_pem.len());
        debug!("   🗝️  Private PEM (first 64 / last 64 chars): {}... / ...{}", 
               &private_pem[..64.min(private_pem.len())], 
               &private_pem[(private_pem.len().saturating_sub(64))..]);
        debug!("   🔒 Passphrase provided: {}", passphrase.is_some());
        if let Some(ref p) = passphrase {
            debug!("   🔒 Passphrase length: {} chars", p.len());
            debug!("   🔒 Passphrase (first 16 / last 16 chars): {}... / ...{}", 
                   &p[..16.min(p.len())], 
                   &p[(p.len().saturating_sub(16))..]);
        }
        debug!("   📂 RPST is path: {}", is_path(&rpst));
        debug!("   📂 Private PEM is path: {}", is_path(&private_pem));

        // Check the the passphrase and the key are both paths or are both strings
        match &passphrase {
            Some(p) => {
                debug!("   🔍 Validating passphrase and private key consistency...");
                if is_path(p) != is_path(&private_pem) {
                    debug!("   ❌ ERROR: passphrase and private key path/value mismatch");
                    return Err(
                        "passphrase and private key must be either both full paths or both values"
                            .into(),
                    );
                }
                debug!("   ✅ Passphrase and private key consistency check passed");
            }
            None => {
                debug!("   ℹ️  No passphrase provided");
            }
        }

        // TODO check region is non-empty?
        if region.is_empty() {
            debug!("   ❌ ERROR: Region is empty");
            return Err("Region cannot be empty".into());
        }
        debug!("   ✅ Region validation passed");

        debug!("   🔑 Loading session private key...");
        let session_private_key = {
            if is_path(&private_pem) {
                debug!("      📖 Loading private key from file: {}", private_pem);
                // load info from path(s) given
                let byte_vec = std::fs::read(&private_pem)?;
                debug!("      📊 Private key file size: {} bytes", byte_vec.len());
                match &passphrase {
                    Some(p) => {
                        debug!("      🔓 Loading private key with passphrase from file: {}", p);
                        let pass_vec = std::fs::read(p)?;
                        debug!("      📊 Passphrase file size: {} bytes", pass_vec.len());
                        Rsa::private_key_from_pem_passphrase(&byte_vec, &pass_vec)?
                    }
                    None => {
                        debug!("      🔓 Loading private key without passphrase");
                        Rsa::private_key_from_pem(&byte_vec)?
                    }
                }
            } else {
                debug!("      🔗 Loading private key from string value");
                // info given directly
                match passphrase {
                    Some(p) => {
                        debug!("      🔓 Loading private key with passphrase from string");
                        Rsa::private_key_from_pem_passphrase(private_pem.as_bytes(), p.as_bytes())?
                    }
                    None => {
                        debug!("      🔓 Loading private key without passphrase from string");
                        Rsa::private_key_from_pem(private_pem.as_bytes())?
                    }
                }
            }
        };
        debug!("   ✅ Session private key loaded successfully");

        debug!("   🎟️  Processing RPST token...");
        // decode token string: if file, read that
        let token = {
            if is_path(&rpst) {
                debug!("      📖 Reading RPST token from file: {}", rpst);
                let byte_vec = std::fs::read(&rpst)?;
                debug!("      📊 RPST token file size: {} bytes", byte_vec.len());
                debug!("      📊 Raw file bytes (first 64 / last 64): {:02x?}... / ...{:02x?}", 
                       &byte_vec[..64.min(byte_vec.len())].to_vec(),
                       &byte_vec[(byte_vec.len().saturating_sub(64))..].to_vec());
                let token_content = String::from_utf8(byte_vec)?
                    .lines()
                    .next()
                    .ok_or("invalid data in RPST token file")?
                    .to_string();
                debug!("      ✅ RPST token read from file, length: {} chars", token_content.len());
                debug!("      ✅ Token from file (first 64 / last 64 chars): {}... / ...{}", 
                       &token_content[..64.min(token_content.len())], 
                       &token_content[(token_content.len().saturating_sub(64))..]);
                token_content
            } else {
                debug!("      🔗 Using RPST token from string value");
                debug!("      🔗 Token from string (first 64 / last 64 chars): {}... / ...{}", 
                       &rpst[..64.min(rpst.len())], 
                       &rpst[(rpst.len().saturating_sub(64))..]);
                rpst
            }
        };

        debug!("   🎫 Extracting final_st_token by JSON decoding rpst variable...");
        // JSON decode the rpst/token to extract the 'token' field directly
        let final_st_token = match serde_json::from_str::<Value>(&token) {
            Ok(rpst_json) => {
                debug!("      ✅ Successfully JSON decoded rpst variable");
                debug!("      📋 Available fields in rpst JSON: {:?}", 
                       rpst_json.as_object().map(|obj| obj.keys().collect::<Vec<_>>()));
                
                rpst_json["token"]
                    .as_str()
                    .map(|s| {
                        debug!("      ✅ Extracted final_st_token from rpst 'token' field: {} chars", s.len());
                        debug!("      🎟️  final_st_token (first 64 / last 64 chars): {}... / ...{}", 
                               &s[..64.min(s.len())], 
                               &s[(s.len().saturating_sub(64))..]);
                        s.to_owned()
                    })
                    .ok_or_else(|| {
                        debug!("   ❌ ERROR: 'token' field not found in rpst JSON");
                        format!("'token' field not found in rpst JSON")
                    })?
            },
            Err(e) => {
                debug!("   ⚠️  Could not JSON decode rpst, will extract from JWT payload instead. Error: {}", e);
                debug!("   ⚠️  Falling back to JWT payload extraction...");
                // Will be extracted later from JWT payload
                String::new()
            }
        };

        // Note: in Resource Principal, the tenancy is extracted from the given RPST token.
        // In Instance Principal, the tenancy is extracted from the leaf certificate.

        debug!("   🔍 Parsing RPST token for tenancy and expiration...");
        debug!("   🔍 Full token being parsed (first 64 / last 64 chars): {}... / ...{}", 
               &token[..64.min(token.len())], 
               &token[(token.len().saturating_sub(64))..]);
        // decode token, get "res_tenant" for tenancyOCID and "exp" for expiration
        // token is a three-part string, dot-separated:
        // header.payload.<something?>
        // the fields we want are in the payload, which is base64-encoded JSON (how fun!)
        let mut parts = token.split('.');
        // header: skip for now
        let header = parts.next();
        if header.is_none() {
            debug!("   ❌ ERROR: RPST token missing header");
            return Err("invalid RPST token: missing header".into());
        }
        debug!("      📦 RPST header found, length: {} chars", header.unwrap().len());
        debug!("      📦 Header (first 64 / last 64 chars): {}... / ...{}", 
               &header.unwrap()[..64.min(header.unwrap().len())], 
               &header.unwrap()[(header.unwrap().len().saturating_sub(64))..]);
        
        let payload = match parts.next() {
            Some(p) => {
                debug!("      📦 RPST payload found, length: {} chars", p.len());
                debug!("      📦 Payload (first 64 / last 64 chars): {}... / ...{}", 
                       &p[..64.min(p.len())], 
                       &p[(p.len().saturating_sub(64))..]);
                p
            },
            None => {
                debug!("   ❌ ERROR: RPST token missing payload");
                return Err("invalid RPST token: missing payload".into());
            }
        };
        
        let signature = parts.next();
        if let Some(sig) = signature {
            debug!("      📦 RPST signature found, length: {} chars", sig.len());
            debug!("      📦 Signature (first 64 / last 64 chars): {}... / ...{}", 
                   &sig[..64.min(sig.len())], 
                   &sig[(sig.len().saturating_sub(64))..]);
        } else {
            debug!("      ⚠️  No signature part found in token");
        }

        debug!("      🔓 Decoding base64 payload...");
        // the payload should not be padded
        let decoded = Base64Unpadded::decode_vec(&payload)?;
        debug!("      📊 Decoded payload size: {} bytes", decoded.len());
        debug!("      📊 Decoded raw bytes (first 64 / last 64): {:02x?}... / ...{:02x?}", 
               &decoded[..64.min(decoded.len())].to_vec(),
               &decoded[(decoded.len().saturating_sub(64))..].to_vec());
        
        debug!("      🎫 Extracting token using jwt.rs pattern...");
        // Following jwt.rs pattern: bytes -> string -> JSON -> extract token field
        let payload_str = String::from_utf8(decoded.clone())
            .map_err(|e| format!("JWT payload not valid UTF-8: {}", e))?;
        
        debug!("      📋 Decoded JWT payload string (first 256 chars): {}...", 
               &payload_str[..256.min(payload_str.len())]);
        
        // Parse the JSON payload (following jwt.rs pattern)
        let payload_json: Value = serde_json::from_str(&payload_str)
            .map_err(|e| format!("Failed to parse JWT payload JSON: {}", e))?;
        
        debug!("      ✅ Successfully parsed JWT payload JSON");
                
        debug!("      📋 Re-parsing JSON payload for tenancy and expiration...");
        let v: Value = serde_json::from_slice(&decoded)?;
        debug!("      ✅ JSON payload parsed successfully");
        
        // TODO: better method for checking these values (not checking for "null")
        debug!("      🏢 Extracting tenancy from '{}' claim...", TENANCY_CLAIM_KEY);
        let tenancy_raw = format!("{}", v[TENANCY_CLAIM_KEY]);
        debug!("      🏢 Raw tenancy value: {}", tenancy_raw);
        let tenancy = tenancy_raw.replace("\"", "");
        if tenancy == "null" {
            debug!("   ❌ ERROR: RPST token missing '{}' claim", TENANCY_CLAIM_KEY);
            debug!("   ❌ Available claims in JSON: {:?}", v.as_object().map(|obj| obj.keys().collect::<Vec<_>>()));
            return Err(
                format!("RPST token missing '{}' in payload", TENANCY_CLAIM_KEY)
                    .as_str()
                    .into(),
            );
        }
        debug!("      ✅ Tenancy extracted (first 64 / last 64 chars): {}... / ...{}", 
               &tenancy[..64.min(tenancy.len())], 
               &tenancy[(tenancy.len().saturating_sub(64))..]);
        
        debug!("      ⏰ Extracting expiration from 'exp' claim...");
        let exp_raw = format!("{}", v["exp"]);
        debug!("      ⏰ Raw expiration value: {}", exp_raw);
        let exp = exp_raw.replace("\"", "");
        if exp == "null" {
            debug!("   ❌ ERROR: RPST token missing 'exp' claim");
            debug!("   ❌ Available claims in JSON: {:?}", v.as_object().map(|obj| obj.keys().collect::<Vec<_>>()));
            return Err(format!("RPST token missing 'exp' in payload")
                .as_str()
                .into());
        }
        debug!("      ✅ Expiration extracted: {}", exp);
        
        trace!("rpst expiration={}", exp);
        trace!("using RPST token: {}", token);
        
        debug!("   🏗️  Creating ResourcePrincipalAuthProvider instance...");
        let auth_provider = ResourcePrincipalAuthProvider {
            token: final_st_token,
            session_private_key: session_private_key,
            tenancy_id: tenancy,
            region: region,
        };
        
        debug!("✅ ResourcePrincipalAuthProvider::new_from_values() completed successfully");
        debug!("   📍 Final region: {}", auth_provider.region);
        debug!("   🏢 Final tenancy (first 64 / last 64 chars): {}... / ...{}", 
               &auth_provider.tenancy_id[..64.min(auth_provider.tenancy_id.len())], 
               &auth_provider.tenancy_id[(auth_provider.tenancy_id.len().saturating_sub(64))..]);
        debug!("   🎟️  Final token: {} chars", auth_provider.token.len());
        debug!("   🎟️  Final token (first 64 / last 64 chars): {}... / ...{}", 
               &auth_provider.token[..64.min(auth_provider.token.len())], 
               &auth_provider.token[(auth_provider.token.len().saturating_sub(64))..]);

        Ok(auth_provider)
    }
}

// By contract for the the content of a resource principal to be considered path, it needs to be
// an absolute path.
fn is_path(val: &str) -> bool {
    std::path::Path::new(val).is_absolute()
}
