//!
pub mod recv {
    pub mod unreplicated;
    pub use unreplicated::Unreplicated;
}
pub mod engine {
    pub mod sim;
}
pub mod app {
    pub(crate) mod mock;
}
pub(crate) mod util {
    pub mod timer;
    pub use timer::*;
    pub mod replica;
    pub use replica::*;
}

use std::borrow::{Borrow, BorrowMut};
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::channel::oneshot::{
    channel as unreliable, Canceled, Receiver as Unreliable, Sender as Out,
};

pub type OpNum = u64;

pub trait App<A> {
    #[allow(unused_variables)]
    fn leader_upcall(&mut self, op: String) -> Option<String> {
        None
    }
    fn replica_upcall(&mut self, op_num: OpNum, op: String) -> String;
    fn unlogged_upcall(&mut self, op: String) -> String;
}
pub trait AppMeta {
    type State;
}
/// Grouped meta for `ClientState` and `ServerState`, include useful `Msg`.
pub trait Protocol<Engine, App> {
    type Client;
    type Server;
    type Msg;
}
/// Some structs need to implement `Borrow` on non-specific types, e.g. generic
/// parameters or trait's associate type. Include a tag to prevent these
/// implementations collide with each other.
///
/// The tag is not contraint. Currently only roles are used as tags, but it could
/// be extended some time.
pub trait TaggedBorrowMut<T, const TAG: u8> {
    fn get_mut(&mut self) -> &mut T;
}

pub type ReplicaIndex = i32;
pub struct ReplicaState<App: AppMeta> {
    pub index: ReplicaIndex,
    pub init: bool,
    pub app: App::State,
}

pub trait ClientState: Default {}
pub trait ServerState<App: AppMeta>: From<ReplicaState<App>> {}

pub struct Config {
    pub f: usize,
    pub replica_list: Vec<SocketAddr>,
    pub multicast: Option<SocketAddr>,
}

#[derive(PartialEq, Eq)]
pub enum Role {
    Server,
    Client,
}
// FIXME(const_generic)
pub trait Recv<Protocol, App, const ROLE: u8> {
    type Msg;
    fn recv_msg(&mut self, remote: &SocketAddr, msg: Self::Msg);
}

pub type Millis = u64;

pub struct Reliable<T>(Unreliable<T>);
impl<T> Future for Reliable<T> {
    type Output = T;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.0).poll(cx).map(Result::unwrap)
    }
}
pub(crate) fn reliable<T>() -> (Out<T>, Reliable<T>) {
    let (out, fut) = unreliable();
    (out, Reliable(fut))
}

pub trait Invoke<Protocol> {
    fn invoke(&mut self, op: String) -> Reliable<String>;
    fn invoke_unlogged(&mut self, op: String, timeout: Millis) -> Unreliable<String>;
    const DEFAULT_UNLOGGED_TIMEOUT: Millis = 1000;
}

pub trait TransportCore {
    type Msg;
    fn send_msg(&self, dst: &SocketAddr, msg: Self::Msg);
}
pub trait Transport: TransportCore {
    fn send_msg_to_replica(&self, index: ReplicaIndex, msg: Self::Msg);
    fn send_msg_to_all(&self, msg: Self::Msg);
}
// The usage of `Borrow<SocketAddr>` here is a little bit confusing. The "borrowed"
// `SocketAddr` is actually treated as sender's address.
// Make a dedicated trait if `Borrow<SocketAddr>` appears anywhere else.
impl<T: TransportCore<Msg = Msg> + Borrow<Config> + Borrow<SocketAddr>, Msg: Clone> Transport
    for T
{
    fn send_msg_to_replica(&self, index: ReplicaIndex, msg: Self::Msg) {
        let dst = &<Self as Borrow<Config>>::borrow(self).replica_list[index as usize];
        self.send_msg(dst, msg);
    }
    fn send_msg_to_all(&self, msg: Self::Msg) {
        for i in 0..<Self as Borrow<Config>>::borrow(self).replica_list.len() {
            if *<Self as Borrow<SocketAddr>>::borrow(self)
                == <Self as Borrow<Config>>::borrow(self).replica_list[i]
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
