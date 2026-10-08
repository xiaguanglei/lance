// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::collections::HashMap;
use std::fmt::Debug;

use serde::Deserialize;
use serde::Serialize;

use super::backend::HdfsNativeBuilder;

pub const HDFS_SCHEME_PREFIX: &str = "hdfs://";
pub const HDFS_DEFAULT_AUTHORITY: &str = "nameservice";
pub const HA_NAMENODES_PREFIX: &str = "dfs.ha.namenodes";
pub const HA_NAMENODE_RPC_ADDRESS_PREFIX: &str = "dfs.namenode.rpc-address";

/// Config for HdfsNative services support.
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[serde(default)]
#[non_exhaustive]
pub struct HdfsNativeConfig {
    /// work dir of this backend
    pub root: Option<String>,
    /// name_node of this backend
    pub name_node: Option<String>,
    /// Effective HDFS user; Kerberos keeps the authenticated user as the real user.
    pub user: Option<String>,
    /// Maximum number of complete-file block maps cached by the HDFS client.
    /// Zero or unset disables the cache.
    pub block_locations_cache_capacity: Option<usize>,
    /// Time-to-live in milliseconds for cached block maps.
    pub block_locations_cache_ttl_ms: Option<u64>,
    /// Maximum number of idle DataNode connections retained per client.
    pub datanode_socket_cache_capacity: Option<usize>,
    /// Maximum idle age in milliseconds for cached DataNode connections.
    pub datanode_socket_cache_ttl_ms: Option<u64>,
    /// Maximum number of live DataNode read connections. Zero is unlimited.
    pub datanode_socket_cache_max_total: Option<usize>,
    /// Spread reads across the leading, equal-priority replica group.
    pub datanode_replica_round_robin: Option<bool>,
    /// Deprecated: HDFS Native append capability is enabled by default.
    #[deprecated(
        since = "0.57.0",
        note = "HDFS Native append capability is enabled by default and this option is no longer needed."
    )]
    pub enable_append: bool,
    /// other options for hdfs client
    pub options: Option<HashMap<String, String>>,
}

impl Debug for HdfsNativeConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HdfsNativeConfig")
            .field("root", &self.root)
            .field("name_node", &self.name_node)
            .field("user", &self.user)
            .field(
                "block_locations_cache_capacity",
                &self.block_locations_cache_capacity,
            )
            .field(
                "block_locations_cache_ttl_ms",
                &self.block_locations_cache_ttl_ms,
            )
            .field(
                "datanode_socket_cache_capacity",
                &self.datanode_socket_cache_capacity,
            )
            .field(
                "datanode_socket_cache_ttl_ms",
                &self.datanode_socket_cache_ttl_ms,
            )
            .field(
                "datanode_socket_cache_max_total",
                &self.datanode_socket_cache_max_total,
            )
            .field(
                "datanode_replica_round_robin",
                &self.datanode_replica_round_robin,
            )
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

impl opendal_core::Configurator for HdfsNativeConfig {
    type Builder = HdfsNativeBuilder;

    fn from_uri(uri: &opendal_core::OperatorUri) -> opendal_core::Result<Self> {
        let mut map = uri.options().clone();
        if let Some(authority) = uri.authority() {
            map.insert("name_node".to_string(), format!("hdfs://{authority}"));
        }

        if let Some(root) = uri.root()
            && !root.is_empty()
        {
            map.insert("root".to_string(), root.to_string());
        }

        Self::from_iter(map)
    }

    #[allow(deprecated)]
    fn into_builder(self) -> Self::Builder {
        HdfsNativeBuilder { config: self }
    }
}

pub fn init_hdfs_config(name_node_uri: &str) -> HashMap<String, String> {
    let namenodes = name_node_uri
        .split(",")
        .filter_map(|s| {
            if !s.is_empty() {
                Some(s.trim_start_matches("hdfs://").trim_end_matches("/"))
            } else {
                None
            }
        })
        .collect::<Vec<&str>>();
    // A bare nameservice must be resolved from hdfs-site.xml, not replaced by
    // a synthetic NameNode address. Direct and HA host:port lists use the
    // synthetic authority below.
    if !is_port_based(name_node_uri) {
        return HashMap::new();
    }
    let mut hdfs_config = HashMap::new();
    let mut ha_config_namenodes_vec = Vec::new();
    for (index, namenode) in namenodes.iter().enumerate() {
        hdfs_config.insert(
            format!(
                "{}.{}.nn{}",
                HA_NAMENODE_RPC_ADDRESS_PREFIX, HDFS_DEFAULT_AUTHORITY, index
            ),
            namenode.to_string(),
        );
        ha_config_namenodes_vec.push(format!("nn{}", index));
    }
    hdfs_config.insert(
        format!("{}.{}", HA_NAMENODES_PREFIX, HDFS_DEFAULT_AUTHORITY),
        ha_config_namenodes_vec.join(","),
    );
    hdfs_config
}

pub fn is_port_based(name_node_uri: &str) -> bool {
    name_node_uri
        .split(',')
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            entry
                .trim_start_matches(HDFS_SCHEME_PREFIX)
                .trim_end_matches('/')
        })
        .any(|entry| entry.contains(':'))
}

#[cfg(test)]
mod tests {
    use super::*;
    use opendal_core::Configurator;
    use opendal_core::OperatorUri;

    #[test]
    fn from_uri_sets_name_node_and_root() {
        let uri = OperatorUri::new(
            "hdfs-native://namenode:9000/user/project",
            Vec::<(String, String)>::new(),
        )
        .unwrap();

        let cfg = HdfsNativeConfig::from_uri(&uri).unwrap();
        assert_eq!(cfg.name_node.as_deref(), Some("hdfs://namenode:9000"));
        assert_eq!(cfg.root.as_deref(), Some("user/project"));
    }

    #[test]
    fn from_uri_allows_missing_authority() {
        let uri = OperatorUri::new("hdfs-native", Vec::<(String, String)>::new()).unwrap();

        let cfg = HdfsNativeConfig::from_uri(&uri).unwrap();
        assert!(cfg.name_node.is_none());
    }

    #[test]
    fn from_iter_preserves_user_and_cache_options() {
        let cfg = HdfsNativeConfig::from_iter([
            ("user".to_string(), "alice".to_string()),
            (
                "block_locations_cache_capacity".to_string(),
                "128".to_string(),
            ),
            (
                "block_locations_cache_ttl_ms".to_string(),
                "60000".to_string(),
            ),
            (
                "datanode_socket_cache_capacity".to_string(),
                "16".to_string(),
            ),
            (
                "datanode_socket_cache_ttl_ms".to_string(),
                "3000".to_string(),
            ),
            (
                "datanode_socket_cache_max_total".to_string(),
                "64".to_string(),
            ),
            (
                "datanode_replica_round_robin".to_string(),
                "true".to_string(),
            ),
        ])
        .unwrap();
        assert_eq!(cfg.user.as_deref(), Some("alice"));
        assert_eq!(cfg.block_locations_cache_capacity, Some(128));
        assert_eq!(cfg.block_locations_cache_ttl_ms, Some(60000));
        assert_eq!(cfg.datanode_socket_cache_capacity, Some(16));
        assert_eq!(cfg.datanode_socket_cache_ttl_ms, Some(3000));
        assert_eq!(cfg.datanode_socket_cache_max_total, Some(64));
        assert_eq!(cfg.datanode_replica_round_robin, Some(true));
    }

    #[test]
    fn nameservice_uses_cluster_configuration() {
        assert!(init_hdfs_config("hdfs://nameservice-id/").is_empty());
        assert!(!is_port_based("hdfs://nameservice-id/"));
        assert!(is_port_based("hdfs://nn1:8020,nn2:8020/"));
    }

    #[test]
    fn init_hdfs_config_from_single_namenode() {
        let hdfs_config = init_hdfs_config("hdfs://namenode1:9000/");
        println!("{:?}", hdfs_config);
        assert!(!hdfs_config.is_empty() && hdfs_config.len() == 2);
        assert_eq!(
            hdfs_config.get("dfs.ha.namenodes.nameservice"),
            Some(&"nn0".to_string())
        );
        assert_eq!(
            hdfs_config.get("dfs.namenode.rpc-address.nameservice.nn0"),
            Some(&"namenode1:9000".to_string())
        );
    }

    #[test]
    fn init_hdfs_config_from_multi_namenodes() {
        let hdfs_config = init_hdfs_config("hdfs://namenode1:9000,namenode2:9000/");
        println!("{:?}", hdfs_config);
        assert!(!hdfs_config.is_empty() && hdfs_config.len() == 3);
        assert_eq!(
            hdfs_config.get("dfs.ha.namenodes.nameservice"),
            Some(&"nn0,nn1".to_string())
        );
        assert_eq!(
            hdfs_config.get("dfs.namenode.rpc-address.nameservice.nn0"),
            Some(&"namenode1:9000".to_string())
        );
        assert_eq!(
            hdfs_config.get("dfs.namenode.rpc-address.nameservice.nn1"),
            Some(&"namenode2:9000".to_string())
        );
    }
}
