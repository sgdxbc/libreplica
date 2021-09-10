//!
use std::borrow::Borrow;
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashSet};
use std::future::Future;
use std::marker::PhantomData;
use std::net::{IpAddr, SocketAddr, UdpSocket};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use bincode::{deserialize, serialize};
use derivative::Derivative;
use futures::task::{waker_ref, ArcWake};
use log::*;
use quanta::{Clock, Instant};
use serde::{Deserialize, Serialize};

use crate::util::misc::create_interrupt;
use crate::*;

pub type Addr = SocketAddr;
pub struct Engine<P, App, const ROLE: u8>
where
    RecvT<Self, P, App>: Tag<ROLE>,
{
    _p: PhantomData<(P, App)>,
    config: Config<Addr>,

    recv: <RecvT<Self, P, App> as Tag<ROLE>>::State,
    socket: UdpSocket,
    addr: Addr,
    interrupt: Arc<AtomicBool>,

    timeout_queue: BinaryHeap<Reverse<TimeoutState<Self>>>,
    enable_set: HashSet<Timeout>,
    last_timeout: Timeout,
}
pub trait Tag<const ROLE: u8> {
    type State;
}
pub struct RecvT<E, P, A>(PhantomData<(E, P, A)>);
impl<P: Protocol<E, A>, E, A> Tag<{ Role::Client as u8 }> for RecvT<E, P, A> {
    type State = P::Client;
}
impl<P: Protocol<E, A>, E, A> Tag<{ Role::Server as u8 }> for RecvT<E, P, A> {
    type State = P::Server;
}
#[derive(Derivative)]
#[derivative(Eq, PartialEq, Ord, PartialOrd)]
struct TimeoutState<T> {
    #[derivative(PartialEq = "ignore", Ord = "ignore", PartialOrd = "ignore")]
    id: Timeout,
    expire: Instant,
    #[derivative(PartialEq = "ignore", Ord = "ignore", PartialOrd = "ignore")]
    callback: Box<dyn 'static + FnOnce(&mut T)>,
}

impl<P, A, const R: u8> Engine<P, A, R>
where
    RecvT<Self, P, A>: Tag<R>,
{
    pub fn new_client(config: Config<Addr>, host: IpAddr, interrupt: Arc<AtomicBool>) -> Self
    where
        P: Protocol<Self, A, Client = <RecvT<Self, P, A> as Tag<R>>::State>,
        P::Client: ClientState,
    {
        let socket = UdpSocket::bind(SocketAddr::new(host, 0)).unwrap();
        let client = P::Client::default();
        info!(
            "bind client, name = {:?}: {}",
            client.borrow(),
            socket.local_addr().unwrap()
        );
        Self::new(config, client, socket, interrupt)
    }
    pub fn new_server(config: Config<Addr>, replica: Replica<A>) -> Self
    where
        P: Protocol<Self, A, Server = <RecvT<Self, P, A> as Tag<R>>::State>,
        P::Server: ServerState<A>,
    {
        let socket = UdpSocket::bind(config.replica_list[replica.index as usize]).unwrap();
        info!(
            "bind server, index = {}: {}",
            replica.index,
            socket.local_addr().unwrap()
        );
        let (_, interrupt) = create_interrupt();
        Self::new(config, P::Server::from(replica), socket, interrupt)
    }
    fn new(
        config: Config<Addr>,
        recv: <RecvT<Self, P, A> as Tag<R>>::State,
        socket: UdpSocket,
        interrupt: Arc<AtomicBool>,
    ) -> Self {
        socket.set_nonblocking(true).unwrap();
        Self {
            _p: PhantomData,
            config,
            recv,
            addr: socket.local_addr().unwrap(),
            socket,
            interrupt,
            timeout_queue: BinaryHeap::new(),
            enable_set: HashSet::new(),
            last_timeout: 0,
        }
    }
}

impl<P: Protocol<Self, A, Client = <RecvT<Self, P, A> as Tag<R>>::State>, A, const R: u8>
    TaggedBorrowMut<P::Client, { Role::Client as u8 }> for Engine<P, A, R>
where
    RecvT<Self, P, A>: Tag<R>,
{
    fn borrow_mut(&mut self) -> &mut P::Client {
        &mut self.recv
    }
}
impl<P: Protocol<Self, A, Server = <RecvT<Self, P, A> as Tag<R>>::State>, A, const R: u8>
    TaggedBorrowMut<P::Server, { Role::Server as u8 }> for Engine<P, A, R>
where
    RecvT<Self, P, A>: Tag<R>,
{
    fn borrow_mut(&mut self) -> &mut P::Server {
        &mut self.recv
    }
}
impl<P, A, const R: u8> Borrow<Config<Addr>> for Engine<P, A, R>
where
    RecvT<Self, P, A>: Tag<R>,
{
    fn borrow(&self) -> &Config<Addr> {
        &self.config
    }
}
impl<P: Protocol<Self, A>, A, const R: u8> LocalAddr for Engine<P, A, R>
where
    RecvT<Self, P, A>: Tag<R>,
    P::Msg: Serialize,
{
    fn local_addr(&self) -> &Self::Addr {
        &self.addr
    }
}
impl<P: Protocol<Self, A>, A, const R: u8> TransportCore for Engine<P, A, R>
where
    RecvT<Self, P, A>: Tag<R>,
    P::Msg: Serialize,
{
    type Msg = P::Msg;
    type Addr = Addr;
    fn send_msg(&self, dst: &Self::Addr, msg: Self::Msg) {
        self.socket.send_to(&serialize(&msg).unwrap(), dst).unwrap();
    }
}
// copied from simulated engine
impl<P, A, const R: u8> Timing for Engine<P, A, R>
where
    RecvT<Self, P, A>: Tag<R>,
{
    fn create_timeout(
        &mut self,
        duration: Millis,
        callback: impl 'static + FnOnce(&mut Self),
    ) -> Timeout {
        self.last_timeout += 1;
        self.timeout_queue.push(Reverse(TimeoutState {
            id: self.last_timeout,
            expire: Instant::recent() + Duration::from_millis(duration),
            callback: Box::new(callback),
        }));
        self.enable_set.insert(self.last_timeout);
        self.last_timeout
    }
    fn cancel_timeout(&mut self, timeout: Timeout) {
        self.enable_set.remove(&timeout);
    }
}

// exclusive features
impl<P, A, const R: u8> Engine<P, A, R>
where
    RecvT<Self, P, A>: Tag<R>,
{
    pub fn client(&mut self) -> &mut impl Invoke<P, A>
    where
        Self: Invoke<P, A>,
        A: App,
    {
        self
    }
    pub fn sleep(&mut self, duration: Millis) -> impl Future<Output = ()> {
        let (out, fut) = reliable();
        self.create_timeout(duration, |_| out.send(()).unwrap());
        fut
    }

    fn internal_run(&mut self, wake: &AtomicBool)
    where
        P: Protocol<Self, A>,
        P::Msg: for<'a> Deserialize<'a>,
        Self: Recv<P, A, R, Msg = P::Msg, Addr = Addr>,
    {
        let clock = Clock::new();
        let mut next_timeout = if let Some(Reverse(timeout)) = self.timeout_queue.peek() {
            timeout.expire.as_u64()
        } else {
            u64::MAX
        };
        let mut last_timeout = self.last_timeout;
        while !(self.interrupt.load(Ordering::Relaxed)
            || (R == Role::Client as u8 && wake.load(Ordering::Relaxed)))
        {
            let mut buf = [0; 1500];
            // fast path: (may) handle received packet and do not set new timeout
            if let Ok((nb_byte, remote)) = self.socket.recv_from(&mut buf) {
                assert!(nb_byte <= 1500);
                self.recv_msg(&remote, deserialize(&buf[..nb_byte]).unwrap());

                // fast path skip this branch
                if self.last_timeout > last_timeout {
                    // can we assert that no one will ever create a new timeout
                    // then clear all timeouts immediately? :)
                    next_timeout = if let Some(Reverse(timeout)) = self.timeout_queue.peek() {
                        timeout.expire.as_u64()
                    } else {
                        u64::MAX
                    };
                    last_timeout = self.last_timeout;
                }
            }
            assert_ne!(clock.recent().as_u64(), 0);
            assert!(self.timeout_queue.is_empty() || next_timeout != u64::MAX);
            if clock.recent().as_u64() < next_timeout {
                continue; // fast path escape
            }

            next_timeout = u64::MAX;
            while let Some(Reverse(timeout)) = self.timeout_queue.peek() {
                if !self.enable_set.contains(&timeout.id) {
                    self.timeout_queue.pop();
                    continue;
                }
                if clock.recent() < timeout.expire {
                    next_timeout = timeout.expire.as_u64();
                    break;
                }
                let Reverse(timeout) = self.timeout_queue.pop().unwrap();
                self.enable_set.remove(&timeout.id);
                (timeout.callback)(self);
                next_timeout = u64::MAX;
            }
            last_timeout = self.last_timeout;
        }
    }
}
struct FlagWaker(AtomicBool);
impl ArcWake for FlagWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, Ordering::Relaxed);
    }
}
impl<P, A> Engine<P, A, { Role::Server as u8 }>
where
    RecvT<Self, P, A>: Tag<{ Role::Server as u8 }>,
{
    pub fn run(mut self)
    where
        P: Protocol<Self, A>,
        P::Msg: for<'a> Deserialize<'a>,
        Self: Recv<P, A, { Role::Server as u8 }, Msg = P::Msg, Addr = Addr>,
    {
        // the passed atomic bool is just a placeholder, should be ignored by
        // internal loop
        // so pass a positive flag to make sure that it is ignored indeed
        self.internal_run(&AtomicBool::new(true));
    }
}
#[derive(Debug)]
pub struct Interrupted;
impl<P, A> Engine<P, A, { Role::Client as u8 }>
where
    RecvT<Self, P, A>: Tag<{ Role::Client as u8 }>,
{
    pub fn wait<T>(&mut self, pending: impl Future<Output = T>) -> Result<T, Interrupted>
    where
        P: Protocol<Self, A>,
        P::Msg: for<'a> Deserialize<'a>,
        Self: Recv<P, A, { Role::Client as u8 }, Msg = P::Msg, Addr = Addr>,
    {
        let wake_flag = Arc::new(FlagWaker(AtomicBool::new(false)));
        let waker = waker_ref(&wake_flag);
        let mut pending = Box::pin(pending);
        while !self.interrupt.load(Ordering::Relaxed) {
            if let Poll::Ready(t) = pending.as_mut().poll(&mut Context::from_waker(&waker)) {
                return Ok(t);
            }
            self.internal_run(&wake_flag.0);
        }
        Err(Interrupted)
    }
}
