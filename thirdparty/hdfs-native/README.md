[![Build](https://img.shields.io/github/actions/workflow/status/Kimahriman/hdfs-native/rust-test.yml?branch=master)](https://github.com/Kimahriman/hdfs-native/actions/workflows/rust-test.yml?query=branch%3Amaster)
[![Crates.io](https://img.shields.io/crates/v/hdfs-native.svg)](https://crates.io/crates/hdfs-native)
[![PyPI](https://img.shields.io/pypi/v/hdfs-native.svg)](https://pypi.org/project/hdfs-native)
[![Rust Docs](https://img.shields.io/docsrs/hdfs-native?label=rust%20docs)](https://docs.rs/hdfs-native/latest/hdfs_native)
[![Python Docs](https://img.shields.io/readthedocs/hdfs-native?label=python%20docs)](https://hdfs-native.readthedocs.io)

# Native Rust HDFS client

`hdfs-native` is an HDFS client written natively in Rust. It supports nearly all major features of an HDFS client, and several key client configuration options listed below.

## Supported HDFS features

Here is a list of currently supported and unsupported but possible future features.

### HDFS Operations

- [x] Listing
- [x] Reading
- [x] Writing
- [x] Rename
- [x] Delete
- [x] Basic Permissions and ownership
- [x] ACLs
- [x] Content summary
- [x] Set replication
- [x] Set timestamps

### HDFS Features

- [x] Name Services
- [x] Observer reads
- [x] ViewFS
- [x] Router based federation
- [x] Erasure coded reads and writes
  - RS schema only, no support for RS-Legacy or XOR

### Security Features

- [x] Kerberos authentication (GSSAPI SASL support) (requires libgssapi_krb5, see below)
- [x] Token authentication (DIGEST-MD5 SASL support)
- [x] NameNode SASL connection
- [x] DataNode SASL connection
- [x] DataNode data transfer encryption
- [x] Encryption at rest (KMS support) (requires the `kms` feature, off by default — see below)

### Kerberos Support

Kerberos (SASL GSSAPI) mechanism is supported through a runtime dynamic link to `libgssapi_krb5`. This must be installed separately, but is likely already installed on your system. If not you can install it by:

Clients can supply explicit Kerberos settings with the following builder
methods:

- `with_kerberos_principal(principal)` — set the Kerberos principal (required
  when a keytab or cache is supplied).
- `with_kerberos_keytab(keytab)` — use a keytab for credential acquisition.
- `with_kerberos_cache(cache)` — use a credential cache such as
  `FILE:/run/krb5/client.ccache`.

For example:

```rust,no_run
use hdfs_native::ClientBuilder;

let client = ClientBuilder::new()
    .with_url("hdfs://namenode.example.com:9000")
    .with_kerberos_principal("client@EXAMPLE.COM")
    .with_kerberos_keytab("/run/secrets/client.keytab")
    .build()
    .unwrap();
```

These methods are available on both the asynchronous and synchronous
builders. This is native credential-store behavior rather than Hadoop Java's
Subject-based credential storage. Explicit stores require a GSSAPI
implementation exporting `gss_acquire_cred_from`.

#### Debian-based systems

```bash
apt-get install libgssapi-krb5-2
```

#### RHEL-based systems

```bash
yum install krb5-libs
```

#### MacOS

```bash
brew install krb5
```

#### Windows

Download and install the Microsoft Kerberos package from https://web.mit.edu/kerberos/dist/

Make sure `<INSTALL FOLDER>\MIT\Kerberos\bin` is in `%PATH%` so `gssapi64.dll` can be loaded.

### Encryption at rest (KMS)

Reading and writing files in an HDFS encryption zone requires the `kms` Cargo feature, which is off by default. Enable it by picking a TLS backend:

```toml
# Pure-Rust TLS (rustls)
hdfs-native = { version = "...", features = ["kms-rustls-tls"] }

# System TLS (OpenSSL / Secure Transport / SChannel)
hdfs-native = { version = "...", features = ["kms-native-tls"] }
```

The bare `kms` feature supports plaintext `http://` KMS endpoints only; use a TLS variant for `https://`. The KMS endpoint comes from `hadoop.security.key.provider.path`. The Python package enables `kms-rustls-tls` by default.

## Supported HDFS Settings

The client will attempt to read Hadoop configs `core-site.xml` and `hdfs-site.xml` in the directories `$HADOOP_CONF_DIR` or if that doesn't exist, `$HADOOP_HOME/etc/hadoop`. Passing configs in run time is supported as well via `client::ClientBuilder`. Currently the supported configs that are used are:

- `fs.defaultFS` - Client::default() support
- `hadoop.security.authentication` - Enables Kerberos
- `dfs.ha.namenodes` - name service support
- `dfs.namenode.rpc-address.*` - name service support
- `dfs.user.home.dir.prefix` - Home directory prefix for HDFS paths
- `dfs.client.failover.resolve-needed.*` - DNS based NameNode discovery
- `dfs.client.failover.resolver.useFQDN.*` - DNS based NameNode discovery
- `dfs.client.failover.random.order.*` - Randomize order of NameNodes to try
- `dfs.client.failover.proxy.provider.*` - Supports the behavior of the following proxy providers. Any other values will default back to the `ConfiguredFailoverProxyProvider` behavior:
  - `org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider`
  - `org.apache.hadoop.hdfs.server.namenode.ha.ObserverReadProxyProvider`
  - `org.apache.hadoop.hdfs.server.namenode.ha.RouterObserverReadConfiguredFailoverProxyProvider`
- `dfs.client.use.datanode.hostname` - Use DataNode hostnames instead of IP addresses for DataNode connections
- `dfs.client.socket-timeout` - Maximum duration of each DataNode packet read or final read acknowledgement, in milliseconds. Defaults to Hadoop's value of `60000`; set to `0` to disable the timeout.
- `dfs.client.max.block.acquire.failures` - Maximum number of times a read operation refreshes block locations after exhausting the available DataNodes. Defaults to `3`.
- `dfs.client.retry.window.base` - Base retry window, in milliseconds, used to back off before refreshing block locations. Defaults to `3000`.
- `dfs.client.socketcache.capacity` - Maximum number of idle DataNode connections retained by each client. Defaults to Hadoop's value of `16`; set to `0` to disable reuse.
- `dfs.client.socketcache.expiryMsec` - Maximum idle age of a cached DataNode connection in milliseconds. Defaults to Hadoop's value of `3000`.
- `dfs.client.socketcache.maxTotal` - Maximum number of live DataNode read connections per client, including checked-out and idle connections. Defaults to `0` (unlimited). Excess reads wait asynchronously and reuse returned peers; the value must be at least `dfs.client.socketcache.capacity`.
- `dfs.client.cached.conn.retry` - Maximum number of cached DataNode connections tried before opening a new connection. Defaults to Hadoop's value of `3`.
- `dfs.client.read.replica-round-robin` - Spread first-choice reads across healthy `NORMAL` replicas without requiring rack locality. Replicas with missing/lagging heartbeat metadata or a maintenance/decommission state remain ordered fallbacks. Rotation is skipped when the NameNode is configured to prioritize slow nodes, read load, or storage type because those priorities are not fully represented in the client protocol. Defaults to `false`.
- `dfs.client.read.block-locations-cache.capacity` - Maximum number of complete-file block maps cached per client. Defaults to `0` (disabled). The cache stores metadata only, coalesces concurrent opens of the same path, and invalidates entries after mutations made through the same client.
- `dfs.client.read.block-locations-cache.ttl-ms` - Block-location cache TTL in milliseconds. Defaults to `60000` when the cache is enabled. Mutations made by another HDFS client can remain invisible until this TTL expires, so enable the cache only for immutable/versioned data or choose a TTL that matches the application's consistency requirements.
- `dfs.client.block.write.replace-datanode-on-failure.enable`
- `dfs.client.block.write.replace-datanode-on-failure.policy`
- `dfs.client.block.write.replace-datanode-on-failure.best-effort`
- `dfs.data.transfer.protection` - Enables DataNode data transfer protection negotiation when set
- `hadoop.security.key.provider.path` - KMS endpoint for reading/writing files in encryption zones (requires the `kms` feature)
- `fs.viewfs.mounttable.*.homedir` - ViewFS home directory prefix
- `fs.viewfs.mounttable.*.link.*` - ViewFS links
- `fs.viewfs.mounttable.*.linkFallback` - ViewFS link fallback

Repeated positioned reads on one `FileReader` reuse its block locations without
another NameNode call. A DataNode failure refreshes that reader's block map;
normal `Client::read` calls always fetch fresh block locations.

All other settings are generally assumed to be the defaults currently. For instance, security is assumed to be enabled and SASL negotiation is always done, but on insecure clusters this will just do SIMPLE authentication. Any setups that require other customized Hadoop client configs may not work correctly.

## Building

```
cargo build
```

## Object store implementation

An object_store implementation for HDFS is provided in the [hdfs-native-object-store](https://github.com/datafusion-contrib/hdfs-native-object-store) crate.

## Running tests

The tests are mostly integration tests that utilize a small Java application in `rust/mindifs/` that runs a custom `MiniDFSCluster`. To run the tests, you need to have Java, Maven, Hadoop binaries, and Kerberos tools available and on your path. Any Java version between 8 and 17 should work.

```bash
cargo test -p hdfs-native --features integration-test
```

Add `kms-rustls-tls` to the feature list to also run the encryption-zone (KMS) tests: `--features integration-test,kms-rustls-tls`.

### Python tests

See the [Python README](./python/README.md)

## Running benchmarks

Some of the benchmarks compare performance to the JVM based client through libhdfs via the fs-hdfs3 crate. Because of that, some extra setup is required to run the benchmarks:

```bash
export HADOOP_CONF_DIR=$(pwd)/rust/target/test
export CLASSPATH=$(hadoop classpath)
```

then you can run the benchmarks with

```bash
cargo bench -p hdfs-native --features benchmark
```

The `benchmark` feature is required to expose `minidfs` and the internal erasure coding functions to benchmark.

## Running examples

The examples make use of the `minidfs` module to create a simple HDFS cluster to run the example. This requires including the `integration-test` feature to enable the `minidfs` module. Alternatively, if you want to run the example against an existing HDFS cluster you can exclude the `integration-test` feature and make sure your `HADOOP_CONF_DIR` points to a directory with HDFS configs for talking to your cluster.

```bash
cargo run --example simple --features integration-test
```
