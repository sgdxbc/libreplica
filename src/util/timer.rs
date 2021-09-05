//!
use crate::*;

pub struct TimerState<T: ?Sized> {
    timeout: Option<Timeout>,
    callback: fn(&mut T),
    pub interval: Millis,
}
pub fn timer<T>(interval: Millis, callback: fn(&mut T)) -> TimerState<T> {
    TimerState {
        timeout: None,
        callback,
        interval,
    }
}
pub type Timer<T> = fn(&mut T) -> &mut TimerState<T>;

pub trait TimerKit {
    fn start(&mut self, timer: Timer<Self>);
    fn stop(&mut self, timer: Timer<Self>);
    fn reset(&mut self, timer: Timer<Self>) {
        self.stop(timer);
        self.start(timer);
    }
}

impl<T: 'static + Timing> TimerKit for T {
    fn start(&mut self, timer: Timer<Self>) {
        let TimerState {
            timeout,
            callback,
            interval,
        } = *timer(self);
        if timeout.is_some() {
            return;
        }
        let next_timeout = self.create_timeout(interval, move |this| {
            timer(this).timeout = None;
            this.reset(timer); // to allow following hanlder stop the timer immediately
            callback(this);
        });
        timer(self).timeout = Some(next_timeout);
    }
    fn stop(&mut self, timer: Timer<Self>) {
        let timeout = timer(self).timeout.take();
        if timeout.is_none() {
            return;
        }
        self.cancel_timeout(timeout.unwrap());
    }
}
