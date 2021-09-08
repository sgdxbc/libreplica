//! Run replica server with specified protocol and application.
use log::LevelFilter;
use simple_logger::SimpleLogger;

use libreplica::app::Null;
use libreplica::engine::udp::{create_upkeep, Engine};
use libreplica::recv::*;
use libreplica::*;

fn main() {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .unwrap();
    let upkeep = create_upkeep();

    let config = Config {
        f: 0,
        replica_list: vec!["0.0.0.0:3001".parse().unwrap()],
        multicast: None,
    };
    let engine: Engine<Unreplicated, Null, { Role::Server as u8 }> = Engine::new_server(
        config,
        Replica {
            index: 0,
            init: true,
            app: Null,
        },
    );
    engine.run();

    drop(upkeep);
}
