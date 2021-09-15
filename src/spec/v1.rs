//! `spec` module defines important abstractions and traits, which compose the
//! foundation.
//!
//! The system follows a 3-layer design: from bottom to top is engine, protocol,
//! and application. Engines and applications have their own struct types, but
//! protocols are **contained** by engines. Specifically, protocols are implemented
//! as traits, which borrow required states from engine objects, and provide
//! blanket implementation to engine objects. This design enables protocols to
//! call engine's methods with maximum flexibility.
//!
//! More detailed, each protocol is composed with serveral roles, normally client
//! and server. Each role can behave differently when reacting to events. Related
//! traits are *tagged* with generic role parameter to allow multiple implementation
//! of same protocol.
//!
//! To extend...
//! * A custom application struct needs to implement `App`.
//! * A custom engine struct needs to implement:
//!   * `TransportCore`, where `Msg` should match the one from `Recv` from protocol
//!   * `Timing`
//!   * `LocalAddr`
//!   * `TaggedBorrowMut<RecvState, ROLE>`, where `RecvState` could be state struct
//!     of receiver of any role corresponding to `ROLE`. For engine works with
//!     multiple roles like simulated engine, it should provide multiple `impl`
//!     as well
//!   * `Borrow<Config<Addr>>`, where `Addr` is from `TransportCore`
//!   * Other exclusive features, i.e. invoking for engine contains client
//! * A custom protocol trait needs to provide:
//!   * State structs for each involved role, and client state and server state
//!     should implement `ClientState` and `ServerState<App>` respectively
//!   * A ghost meta-type which implements `Protocol<Engine, App>`, works as the
//!     globally unique tag for the protocol
//!   * One trait per involved role, may inherit any engine trait it needs, and
//!     provides blanket implementation of `Recv<Protocol, App, ROLE>`. Client
//!     should also implement `Invoke<Protocol, App>`
//!
//! All items in top level module are public, to allow foreign extension to work
//! with implementation from this crate.
use std::borrow::{Borrow, BorrowMut};
use std::convert::TryInto;
use std::fmt::{self, Debug, Formatter};
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;
use std::task::{Context, Poll};

pub use futures::channel::oneshot::Canceled;
pub use futures::channel::oneshot::Receiver as Unreliable;
pub use futures::channel::oneshot::{channel as unreliable, Sender as Out};
use rand::distributions::Alphanumeric;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use serde_derive::{Deserialize, Serialize};
use toml::Value;

pub type OpNum = u64;

pub trait App: 'static {
    type Op: Debug + Clone + for<'a> Deserialize<'a> + Serialize;
    type Res: Debug + Clone + Default + for<'a> Deserialize<'a> + Serialize;
    #[allow(unused_variables)]
    fn leader_upcall(&mut self, op: &Self::Op) -> Option<Self::Res> {
        None
    }
    fn replica_upcall(&mut self, op_num: OpNum, op: Self::Op) -> Self::Res;
    fn unlogged_upcall(&mut self, op: Self::Op) -> Self::Res;
}
pub trait RollbackApp: App {
    fn rollback_upcall(&mut self, to: OpNum, op_iter: impl Iterator<Item = (OpNum, Self::Op)>);
    fn commit_upcall(&mut self, to: OpNum);
}
/// The "ghost" meta-type for protocol implementation, i.e. a group of receivers
/// including client, server, etc. Engines are generic over `Protocol` instead
/// of contained receiver states, which normally take the engine type as generic
/// parameter, to prevent cyclic type.
pub trait Protocol<Engine, App> {
    type Client;
    type Server;
    type Msg;
}
/// Some structs need to implement `Borrow` on non-specific types, e.g. generic
/// parameters or trait's associate type. Include a tag to prevent these
/// implementations collide with each other.
///
/// The value of tag is unlimited. Currently only roles are used as tags, but it
/// could be extended some time.
pub trait TaggedBorrowMut<T, const TAG: u8> {
    fn borrow_mut(&mut self) -> &mut T;
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
pub struct ClientName([char; 4]);
impl Debug for ClientName {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.iter().collect::<String>())
    }
}
impl Default for ClientName {
    fn default() -> Self {
        // colision possible for 10 clients (enough number for most protocols): 0.000061%
        Self(
            thread_rng()
                .sample_iter(&Alphanumeric)
                .take(4)
                .map(char::from)
                .collect::<Vec<_>>()
                .try_into()
                .unwrap(),
        )
    }
}
pub trait ClientState: Default + Borrow<ClientName> {}
impl<T: Default + Borrow<ClientName>> ClientState for T {}

pub type ReplicaIndex = i32;
pub struct Replica<App> {
    pub index: ReplicaIndex,
    pub init: bool,
    pub app: App,
}
pub trait ServerState<App>: From<Replica<App>> + BorrowMut<Replica<App>> {}
impl<T: From<Replica<App>> + BorrowMut<Replica<App>>, App> ServerState<App> for T {}

#[derive(Debug, Clone)]
pub struct Config<Addr> {
    pub f: usize,
    pub replica_list: Vec<Addr>,
    pub multicast: Option<Addr>,
}
impl<Addr: FromStr> FromStr for Config<Addr>
where
    Addr::Err: Debug,
{
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let value: Value = s.parse().unwrap();
        Ok(Self {
            f: value["f"].as_integer().unwrap() as usize,
            replica_list: value["replica"]
                .as_array()
                .unwrap()
                .iter()
                .map(|addr| addr.as_str().unwrap().parse().unwrap())
                .collect(),
            multicast: value
                .get("multicast")
                .map(|multicast| multicast.as_str().unwrap().parse().unwrap()),
        })
    }
}

#[derive(PartialEq, Eq)]
pub enum Role {
    Server,
    Client,
}
// FIXME(const_generic)
pub trait Recv<Protocol, App, const ROLE: u8> {
    type Msg;
    type Addr;
    fn recv_msg(&mut self, remote: &Self::Addr, msg: Self::Msg);
}

pub type Millis = u64;

pub struct Reliable<T>(Unreliable<T>);
impl<T> Future for Reliable<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0)
            .poll(cx)
            .map(|x| x.expect("unexpected cancel reliable pending"))
    }
}
pub fn reliable<T>() -> (Out<T>, Reliable<T>) {
    let (out, fut) = unreliable();
    (out, Reliable(fut))
}

pub trait Invoke<Protocol, A: App> {
    fn invoke(&mut self, op: A::Op) -> Reliable<A::Res>;
    fn invoke_unlogged(&mut self, op: A::Op, timeout: Millis) -> Unreliable<A::Res>;
    const DEFAULT_UNLOGGED_TIMEOUT: Millis = 1000;
}

pub trait TransportCore {
    type Msg;
    type Addr;
    fn send_msg(&self, dst: &Self::Addr, msg: Self::Msg);
}
pub trait Transport: TransportCore {
    fn send_msg_to_replica(&self, index: ReplicaIndex, msg: Self::Msg);
    fn send_msg_to_all(&self, msg: Self::Msg);
}
pub trait LocalAddr: TransportCore {
    fn local_addr(&self) -> &Self::Addr;
}
impl<T: TransportCore<Msg = Msg> + Borrow<Config<T::Addr>> + LocalAddr, Msg: Clone> Transport for T
where
    T::Addr: Eq,
{
    fn send_msg_to_replica(&self, index: ReplicaIndex, msg: Self::Msg) {
        let dst = &<Self as Borrow<Config<T::Addr>>>::borrow(self).replica_list[index as usize];
        self.send_msg(dst, msg);
    }
    fn send_msg_to_all(&self, msg: Self::Msg) {
        for i in 0..<Self as Borrow<Config<T::Addr>>>::borrow(self)
            .replica_list
            .len()
        {
            if *self.local_addr() == <Self as Borrow<Config<T::Addr>>>::borrow(self).replica_list[i]
            {
                continue;
            }
            self.send_msg_to_replica(i as ReplicaIndex, msg.clone());
        }
    }
}

pub type Timeout = i32;
pub trait Timing {
    fn create_timeout(
        &mut self,
        duration: Millis,
        callback: impl 'static + FnOnce(&mut Self),
    ) -> Timeout;
    fn cancel_timeout(&mut self, timeout: Timeout);
}
