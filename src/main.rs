//! Run replica server with specified protocol and application.
use log::LevelFilter;
use simple_logger::SimpleLogger;

use libreplica::app::null::App;
use libreplica::engine::udp::Engine;
use libreplica::recv::*;
use libreplica::*;

fn main() {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .unwrap();

    let config = Config {
        f: 0,
        replica_list: vec!["0.0.0.0:3001".parse().unwrap()],
        multicast: None,
    };
    let engine: Engine<Unreplicated, App, { Role::Server as u8 }> = Engine::new_server(
        config,
        ReplicaState {
            index: 0,
            init: true,
            app: (),
        },
    );
    engine.run();
}
