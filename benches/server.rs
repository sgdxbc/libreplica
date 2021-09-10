//! Run replica server with specified protocol and application.
use std::fmt::Debug;
use std::fs::read_to_string;
use std::str::FromStr;

use clap::{clap_app, ArgMatches};
use log::LevelFilter;
use simple_logger::SimpleLogger;

use libreplica::app::Null;
use libreplica::engine::udp::Engine;
use libreplica::recv::*;
use libreplica::util::misc::create_upkeep;
use libreplica::*;

fn main() {
    SimpleLogger::new()
        .with_level(LevelFilter::Info)
        .env()
        .init()
        .unwrap();
    let _upkeep = create_upkeep();

    let matches = clap_app!(null =>
        (@arg config: -c +required +takes_value)
        (@arg index: -i +required +takes_value)
        (@arg _bench: --bench)
    )
    .get_matches();
    fn parse<T>(matches: &ArgMatches, key: &str, desc: &str) -> T
    where
        T: FromStr,
        T::Err: Debug,
    {
        matches.value_of(key).unwrap().parse().expect(desc)
    }
    let config = read_to_string(parse::<String>(&matches, "config", "path to config file"))
        .unwrap()
        .parse()
        .unwrap();
    let index = parse(&matches, "index", "replica index");

    let engine: Engine<Unreplicated, Null, { Role::Server as u8 }> = Engine::new_server(
        config,
        Replica {
            index,
            init: true,
            app: Null,
        },
    );
    engine.run();
}
