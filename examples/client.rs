use log::LevelFilter;
use simple_logger::SimpleLogger;

use libreplica::app::null::App;
use libreplica::engine::udp::{Engine, Interrupted};
use libreplica::recv::*;
use libreplica::*;

fn main() -> Result<(), Interrupted> {
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
    let mut engine: Engine<Unreplicated, App, { Role::Client as u8 }> =
        Engine::new_client(config, "127.0.0.1".parse().unwrap());
    for i in 0..100 {
        let pending = engine.client().invoke(format!("warmup {}", i));
        engine.wait(pending)?;
    }
    let pending = engine.client().invoke("Hello".to_string());
    println!("{}", engine.wait(pending)?);
    Ok(())
}
