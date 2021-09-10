use std::process::abort;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar};
use std::time::Duration;

use ctrlc::set_handler;
use log::*;
use quanta::{Clock, Handle, Upkeep};

pub fn create_interrupt() -> (Arc<Condvar>, Arc<AtomicBool>) {
    let signal = Arc::new(Condvar::new());
    let handler_signal = signal.clone();
    let flag = Arc::new(AtomicBool::new(false));
    let handler_flag = flag.clone();
    set_handler(move || {
        println!();
        if handler_flag.load(Ordering::Relaxed) {
            error!("server not respond");
            abort();
        }
        info!("interrupted");
        handler_flag.store(true, Ordering::Relaxed);
        handler_signal.notify_all();
    })
    .unwrap();
    (signal, flag)
}

pub fn create_upkeep() -> Handle {
    let upkeep = Upkeep::new(Duration::from_millis(1)).start().unwrap();
    while Clock::default().recent().as_u64() == 0 {}
    upkeep
}
