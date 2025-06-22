//
// Copyright (c) 2024, 2025 Oracle and/or its affiliates. All rights reserved.
//
// Licensed under the Universal Permissive License v 1.0 as shown at
//  https://oss.oracle.com/licenses/upl/
//
use rsa::RsaPrivateKey;
use std::error::Error;
use tracing::debug;

use crate::auth_common::authentication_provider::AuthenticationProvider;

static EMPTY_STRING: &str = "";

/// Claims available in resource principal session token (RPST)
#[derive(Debug, Clone)]
pub struct ResourcePrincipalClaims {
    /// Resource tenant OCID, if available
    pub tenant_id: Option<String>,
    /// Resource compartment OCID, if available  
    pub compartment_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ResourcePrincipalAuthProvider {
    token: String,
    session_private_key: RsaPrivateKey,
    tenancy_id: String,
    region: String,
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
    fn private_key(&self) -> Result<RsaPrivateKey, Box<dyn Error>> {
        Ok(self.session_private_key.clone())
    }
    fn region_id(&self) -> &str {
        &self.region
    }
    fn key_id(&self) -> String {
        format!("ST${}", self.token)
    }
}

impl ResourcePrincipalAuthProvider {
    pub fn new_from_values(
        rpst: String,
        private_key: RsaPrivateKey,
        tenancy_id: String,
        region: String,
    ) -> Result<ResourcePrincipalAuthProvider, Box<dyn Error>> {
        debug!("🔧 ResourcePrincipalAuthProvider::new_from_values() starting");

        // Basic validation
        if region.is_empty() {
            return Err("Region cannot be empty".into());
        }
        if tenancy_id.is_empty() {
            return Err("Tenancy ID cannot be empty".into());
        }

        let auth_provider = ResourcePrincipalAuthProvider {
            token: rpst,
            session_private_key: private_key,
            tenancy_id,
            region,
        };

        debug!("✅ ResourcePrincipalAuthProvider::new_from_values() completed successfully");
        Ok(auth_provider)
    }

    /// Get the resource principal claims information
    pub fn get_resource_principal_claims(&self) -> ResourcePrincipalClaims {
        ResourcePrincipalClaims {
            tenant_id: Some(self.tenancy_id.clone()),
            compartment_id: None, // Could be extracted from RPST if needed in future
        }
    }
}
