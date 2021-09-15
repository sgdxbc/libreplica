use std::cmp::min;
use std::fmt::Debug;
use std::fs::read_to_string;
use std::iter::repeat;
use std::str::FromStr;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Barrier, Mutex};
use std::thread::{current, spawn};
use std::time::Duration;

use clap::{clap_app, ArgMatches};
use core_affinity::{get_core_ids, set_for_current};
use log::*;
use quanta::Clock;
use simple_logger::SimpleLogger;

use libreplica::app::Null;
use libreplica::engine::udp::Engine;
use libreplica::recv::*;
use libreplica::util::misc::{create_interrupt, create_upkeep};
use libreplica::*;

fn main() {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .unwrap();
    let _upkeep = create_upkeep();

    let matches = clap_app!(null =>
        (@arg nb_client: -n +required +takes_value)
        (@arg duration: -d +required +takes_value)
        (@arg host: -h +required +takes_value)
        (@arg config: -c +required +takes_value)
        (@arg _bench: --bench)
    )
    .get_matches();
    fn parse<T>(matches: &ArgMatches, key: &str, invariant: &str) -> T
    where
        T: FromStr,
        T::Err: Debug,
    {
        matches.value_of(key).unwrap().parse().expect(invariant)
    }
    let nb_client: usize = parse(&matches, "nb_client", "number of client");
    let mut duration = parse::<u64>(&matches, "duration", "request duration") * 1000;
    let host = parse(&matches, "host", "host ip address");
    let config = read_to_string(parse::<String>(&matches, "config", "path to config file"))
        .unwrap()
        .parse()
        .unwrap();

    let barrier = Arc::new(Barrier::new(nb_client + 1));
    let (signal, flag) = create_interrupt();
    let client_list: Vec<_> = (0..nb_client)
        .zip(
            get_core_ids()
                .unwrap()
                .into_iter()
                .map(Some)
                .chain(repeat(None)),
        )
        .zip(repeat(flag.clone()))
        .zip(repeat(barrier.clone()))
        .zip(repeat(config))
        .map(|((((_, core_id), interrupt), barrier), config)| {
            spawn(move || {
                if let Some(core_id) = core_id {
                    set_for_current(core_id);
                } else {
                    warn!(
                        "no affinity for {:?}, more cpu cores are needed",
                        current().id()
                    );
                }

                let mut engine: Engine<Unreplicated, Null, { Role::Client as u8 }> =
                    Engine::new_client(config, host, interrupt);

                let mut latency_list = Vec::new();
                let clock = Clock::new();
                if barrier.wait().is_leader() {
                    info!("all clients prepared, bench start");
                }
                loop {
                    let start = clock.start();
                    let pending = engine.client().invoke(());
                    if let Err(_) = engine.wait(pending) {
                        return latency_list;
                    }
                    latency_list.push(min(
                        clock.delta(start, clock.end()).as_micros(),
                        u16::MAX.into(),
                    ) as u16);
                }
            })
        })
        .collect();

    let mutex = Mutex::new(());
    let guard = mutex.lock().unwrap();
    let clock = Clock::new();

    if barrier.wait().is_leader() {
        info!("all clients (and main thread) prepared, bench start");
    }
    let start = clock.start();
    if signal
        .wait_timeout_while(guard, Duration::from_millis(duration), |_| {
            !flag.load(Ordering::Relaxed)
        })
        .unwrap()
        .1
        .timed_out()
    {
        // this is expected not to last long
        while clock.delta(start, clock.end()).as_millis() < duration.into()
            && !flag.load(Ordering::Relaxed)
        {}
        if !flag.load(Ordering::Relaxed) {
            info!("time up, main thread send interrupt");
            flag.store(true, Ordering::Relaxed);
        }
    } else {
        duration = clock.delta(start, clock.end()).as_millis() as u64;
    }
    assert!(flag.load(Ordering::Relaxed));

    let mut latency_list: Vec<_> = client_list
        .into_iter()
        .map(|h| h.join().unwrap())
        .flatten()
        .collect();
    info!(
        "throughput: {} ops/sec",
        latency_list.len() as u64 * 1000 / duration
    );
    if !latency_list.is_empty() {
        latency_list.sort_unstable();
        fn fmt_latency(latency: u16) -> String {
            if latency == u16::MAX {
                ">=65536 us".to_string()
            } else {
                format!("{} us", latency)
            }
        }
        info!(
            "latency: {} (medium) / {} (99th)",
            fmt_latency(latency_list[latency_list.len() / 2]),
            fmt_latency(latency_list[(latency_list.len() as f64 * 0.99) as usize])
        )
    }
}
