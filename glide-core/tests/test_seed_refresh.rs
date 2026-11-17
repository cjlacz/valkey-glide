// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

mod constants;
mod utilities;

use glide_core::client::{Client, ConnectionRequest, NodeAddress, PeriodicCheck};
use redis::{AddressResolver, ProtocolVersion, Value};
use rstest::rstest;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use utilities::cluster::RedisCluster;
use utilities::*;

#[derive(Debug)]
struct SwitchingSeedResolver {
    target: Mutex<(String, u16)>,
    seed_calls: AtomicUsize,
}

impl AddressResolver for SwitchingSeedResolver {
    fn resolve(&self, host: &str, port: u16) -> (String, u16) {
        if host == "managed.invalid" {
            self.seed_calls.fetch_add(1, Ordering::SeqCst);
            self.target.lock().unwrap().clone()
        } else {
            (host.to_owned(), port)
        }
    }
}

async fn set(client: &mut Client, key: &str, value: &str) {
    client
        .send_command(redis::cmd("SET").arg(key).arg(value), None)
        .await
        .unwrap();
}

async fn get(client: &mut Client, key: &str) -> Value {
    client
        .send_command(redis::cmd("GET").arg(key), None)
        .await
        .unwrap()
}

#[rstest]
#[timeout(Duration::from_secs(120))]
fn test_seed_refresh_follows_resolver_while_old_cluster_is_healthy(
    #[values(false, true)] tls: bool,
    #[values(ProtocolVersion::RESP2, ProtocolVersion::RESP3)] protocol: ProtocolVersion,
    #[values(true, false)] refresh_from_seeds: bool,
) {
    block_on_all(async {
        let tempdir = tempfile::tempdir().unwrap();
        let tls_paths = tls.then(|| build_tls_file_paths(&tempdir));
        let clusters: Vec<_> = (0..2)
            .map(|_| {
                if tls {
                    RedisCluster::new_with_tls(3, 0, tls_paths.clone())
                } else {
                    RedisCluster::new(false, &None, Some(3), Some(0))
                }
            })
            .collect();
        let configuration = TestConfiguration {
            cluster_mode: ClusterMode::Enabled,
            use_tls: tls,
            request_timeout: Some(2000),
            root_certs: tls_paths
                .as_ref()
                .map(|paths| vec![paths.read_ca_cert_as_bytes()])
                .unwrap_or_default(),
            ..Default::default()
        };
        let mut observers = Vec::new();
        let mut seeds = Vec::new();
        for cluster in &clusters {
            let addresses = cluster.get_server_addresses();
            let request: ConnectionRequest =
                create_connection_request(&addresses, &configuration).into();
            seeds.push(request.addresses[0].clone());
            observers.push(Client::new(request, None).await.unwrap());
        }
        // Exercise hostname as well as port translation. The generated TLS
        // certificate covers localhost and loopback IPs, but not managed.invalid.
        seeds[0].host = "localhost".into();
        let resolver = Arc::new(SwitchingSeedResolver {
            target: Mutex::new((seeds[0].host.clone(), seeds[0].port)),
            seed_calls: AtomicUsize::new(0),
        });
        let mut request: ConnectionRequest =
            create_connection_request(&clusters[0].get_server_addresses(), &configuration).into();
        request.addresses = vec![NodeAddress {
            host: "managed.invalid".into(),
            port: 6379,
            unix_socket_path: None,
        }];
        request.protocol = Some(protocol);
        request.address_resolver = Some(resolver.clone());
        request.refresh_topology_from_initial_nodes = refresh_from_seeds;
        request.periodic_checks = Some(PeriodicCheck::ManualInterval(Duration::from_millis(100)));
        // With TLS the managed seed deliberately does not match any certificate SAN.
        let mut client = Client::new(request, None).await.unwrap();
        let marker = "{a}seed-refresh-marker";
        set(&mut observers[0], marker, "A").await;
        set(&mut observers[1], marker, "B").await;
        assert_eq!(
            get(&mut client, marker).await,
            Value::BulkString(b"A".to_vec().into())
        );

        let initial_seed_calls = resolver.seed_calls.load(Ordering::SeqCst);
        for (round, target) in [1, 0].into_iter().enumerate() {
            *resolver.target.lock().unwrap() = (seeds[target].host.clone(), seeds[target].port);
            let expected = if target == 0 { b"A" } else { b"B" };
            if !refresh_from_seeds {
                // Count callbacks rather than waiting for the topology throttle.
                tokio::time::sleep(Duration::from_millis(400)).await;
                assert_eq!(
                    resolver.seed_calls.load(Ordering::SeqCst),
                    initial_seed_calls
                );
                assert_eq!(
                    get(&mut client, marker).await,
                    Value::BulkString(b"A".to_vec().into())
                );
                break;
            }
            // Topology replacement retains the production 15s + jitter throttle.
            // A short periodic interval does not bypass that policy.
            tokio::time::timeout(Duration::from_secs(45), async {
                loop {
                    if get(&mut client, marker).await == Value::BulkString(expected.to_vec().into())
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
            })
            .await
            .expect("the unchanged client must follow the original seed's current target");
            assert!(resolver.seed_calls.load(Ordering::SeqCst) > initial_seed_calls);
            // These hash tags cover all three primary-slot partitions.
            for tag in ["a", "b", "c"] {
                let key = format!("{{{tag}}}seed-refresh-probe-{round}");
                set(&mut client, &key, "probe").await;
                assert_eq!(
                    get(&mut observers[target], &key).await,
                    Value::BulkString(b"probe".to_vec().into())
                );
                assert_eq!(get(&mut observers[1 - target], &key).await, Value::Nil);
            }
            for observer in &mut observers {
                set(observer, "seed-refresh-health", "healthy").await;
                assert_eq!(
                    get(observer, "seed-refresh-health").await,
                    Value::BulkString(b"healthy".to_vec().into())
                );
            }
        }
    });
}
