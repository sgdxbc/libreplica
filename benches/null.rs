use std::iter::repeat;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier};
use std::thread::{current, sleep, spawn};
use std::time::Duration;

use clap::clap_app;
use core_affinity::{get_core_ids, set_for_current};
use log::*;
use quanta::Clock;
use simple_logger::SimpleLogger;

use libreplica::app::Null;
use libreplica::engine::udp::{create_interrupt, create_upkeep, Engine};
use libreplica::recv::*;
use libreplica::*;

fn main() {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .unwrap();
    let upkeep = create_upkeep();
    let matches = clap_app!(null =>
        (@arg nb_client: -n +required +takes_value)
        (@arg duration: -d +required +takes_value)
        (@arg host: -h +required +takes_value)
        (@arg _bench: --bench)
    )
    .get_matches();
    let nb_client: usize = matches
        .value_of("nb_client")
        .unwrap()
        .parse()
        .expect("number of client");
    let duration = matches
        .value_of("duration")
        .unwrap()
        .parse()
        .expect("request duration");
    let host = matches
        .value_of("host")
        .unwrap()
        .parse()
        .expect("host ip address");

    let barrier = Arc::new(Barrier::new(nb_client + 1));
    let monitor_barrier = barrier.clone();
    let interrupt = create_interrupt();
    let monitor_interrupt = interrupt.clone();
    let monitor = spawn(move || {
        if monitor_barrier.wait().is_leader() {
            info!("all clients prepared, bench start");
        }
        sleep(Duration::from_secs(duration));
        monitor_interrupt.store(true, Ordering::Relaxed);
    });
    let mut latency_list: Vec<_> = (0..nb_client)
        .zip(
            get_core_ids()
                .unwrap()
                .into_iter()
                .map(Some)
                .chain(repeat(None)),
        )
        .zip(repeat(interrupt))
        .zip(repeat(barrier))
        .map(|(((_, core_id), interrupt), barrier)| {
            spawn(move || {
                if let Some(core_id) = core_id {
                    set_for_current(core_id);
                } else {
                    warn!(
                        "no affinity for thread {:?}, you need a machine with more cpu cores",
                        current().id()
                    );
                }

                let config = Config {
                    f: 0,
                    replica_list: vec!["0.0.0.0:3001".parse().unwrap()],
                    multicast: None,
                };
                let mut engine: Engine<Unreplicated, Null, { Role::Client as u8 }> =
                    Engine::new_client(config, host, interrupt);

                if barrier.wait().is_leader() {
                    info!("all clients prepared, bench start");
                }

                let mut latency_list = Vec::new();
                let clock = Clock::new();
                loop {
                    let start = clock.start();
                    let pending = engine.client().invoke(());
                    if let Err(_) = engine.wait(pending) {
                        return latency_list;
                    }
                    latency_list.push(clock.delta(start, clock.end()).as_micros());
                }
            })
        })
        .collect::<Vec<_>>()
        .into_iter()
        .map(|h| h.join().unwrap())
        .flatten()
        .collect();
    monitor.join().unwrap();
    drop(upkeep);

    latency_list.sort_unstable();
    info!(
        "throughput: {} ops/sec",
        latency_list.len() as u64 / duration
    );
    info!(
        "latency: {} us (medium) / {} us (99th)",
        latency_list[latency_list.len() / 2],
        latency_list[(latency_list.len() as f64 * 0.99) as usize]
    )
}
