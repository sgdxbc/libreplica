//!
use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::fmt::Debug;
use std::future::Future;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Arc;
use std::task::Context;

use derivative::Derivative;
use futures::future::LocalBoxFuture;
use futures::task::{waker_ref, ArcWake};
use log::*;

use crate::*;

pub struct Engine<P: Protocol<Self, App>, App> {
    _p: PhantomData<P>,
    recv_map: HashMap<SocketAddr, RecvState<Self, P, App>>,
    whoami: Option<SocketAddr>,

    msg_in: Sender<MsgEnvelop<P::Msg>>,
    msg_out: Receiver<MsgEnvelop<P::Msg>>,
    filter_map: HashMap<u64, Box<dyn Fn(&mut MsgEnvelop<P::Msg>) -> bool>>,

    now: Millis,
    timeout_queue: BinaryHeap<Reverse<TimeoutState<Self>>>,
    enable_set: HashSet<Timeout>,
    last_timeout: Timeout,

    config: Config,
    step_in: Sender<Step<Self>>,
    step_out: Receiver<Step<Self>>,
    task_list: Vec<LocalBoxFuture<'static, ()>>,
}
enum RecvState<E, P: Protocol<E, A>, A> {
    Client(P::Client),
    Server(P::Server),
}
type Step<E> = Box<dyn FnOnce(&mut E) -> LocalBoxFuture<'static, ()>>;
#[derive(Derivative)]
#[derivative(Eq, PartialEq, Ord, PartialOrd)]
pub struct MsgEnvelop<Msg> {
    #[derivative(PartialEq = "ignore", Ord = "ignore", PartialOrd = "ignore")]
    pub src: SocketAddr,
    #[derivative(PartialEq = "ignore", Ord = "ignore", PartialOrd = "ignore")]
    pub dst: SocketAddr,
    #[derivative(PartialEq = "ignore", Ord = "ignore", PartialOrd = "ignore")]
    pub msg: Msg,
    pub delivered: Millis,
}
#[derive(Derivative)]
#[derivative(Eq, PartialEq, Ord, PartialOrd)]
struct TimeoutState<T> {
    #[derivative(PartialEq = "ignore", Ord = "ignore", PartialOrd = "ignore")]
    id: Timeout,
    #[derivative(PartialEq = "ignore", Ord = "ignore", PartialOrd = "ignore")]
    notified: Option<SocketAddr>,
    expire: Millis,
    #[derivative(PartialEq = "ignore", Ord = "ignore", PartialOrd = "ignore")]
    callback: Box<dyn 'static + FnOnce(&mut T)>,
}

impl<P: Protocol<Self, A>, A> Engine<P, A> {
    pub fn new(config: Config) -> Self {
        let (msg_in, msg_out) = channel();
        let (step_in, step_out) = channel();
        Self {
            _p: PhantomData,
            recv_map: HashMap::new(),
            whoami: None,
            msg_in,
            msg_out,
            filter_map: HashMap::new(),
            now: 0,
            timeout_queue: BinaryHeap::new(),
            enable_set: HashSet::new(),
            last_timeout: 0,
            config,
            step_in,
            step_out,
            task_list: Vec::new(),
        }
    }
    pub fn spawn_client(&mut self, host: IpAddr) -> SocketAddr
    where
        P::Client: ClientState,
    {
        let addr = SocketAddr::new(host, self.recv_map.len() as u16 + 10001);
        self.recv_map
            .insert(addr, RecvState::Client(P::Client::default()));
        addr
    }
    pub fn spawn_server(&mut self, replica: ReplicaState<A>)
    where
        P::Server: ServerState<A>,
        A: AppMeta,
    {
        let addr = self.config.replica_list[replica.index as usize];
        self.recv_map
            .insert(addr, RecvState::Server(P::Server::from(replica)));
    }
}

impl<P: Protocol<Self, A>, A> TaggedBorrowMut<P::Client, { Role::Client as u8 }> for Engine<P, A> {
    fn get_mut(&mut self) -> &mut P::Client {
        if let RecvState::Client(state) = self.recv_map.get_mut(&self.whoami.unwrap()).unwrap() {
            state
        } else {
            unreachable!()
        }
    }
}
impl<P: Protocol<Self, A>, A> TaggedBorrowMut<P::Server, { Role::Server as u8 }> for Engine<P, A> {
    fn get_mut(&mut self) -> &mut P::Server {
        if let RecvState::Server(state) = self.recv_map.get_mut(&self.whoami.unwrap()).unwrap() {
            state
        } else {
            unreachable!()
        }
    }
}
impl<P: Protocol<Self, A>, A> Borrow<Config> for Engine<P, A> {
    fn borrow(&self) -> &Config {
        &self.config
    }
}
impl<P: Protocol<Self, A>, A> Borrow<SocketAddr> for Engine<P, A> {
    fn borrow(&self) -> &SocketAddr {
        self.whoami.as_ref().unwrap()
    }
}

impl<P: Protocol<Self, A>, A> TransportCore for Engine<P, A>
where
    P::Msg: Debug,
{
    type Msg = P::Msg;
    fn send_msg(&self, dst: &SocketAddr, msg: Self::Msg) {
        let mut envelop = MsgEnvelop {
            src: self.whoami.unwrap(),
            dst: *dst,
            delivered: self.now,
            msg,
        };
        for (id, filter) in self.filter_map.iter() {
            if !filter(&mut envelop) {
                info!(
                    "[now = {}] msg dropped (id = {}): {} -> {}, {:?}",
                    self.now, id, envelop.src, envelop.dst, envelop.msg
                );
                return;
            }
        }
        self.msg_in.send(envelop).unwrap();
    }
}
impl<P: Protocol<Self, A>, A> Timing for Engine<P, A> {
    fn create_timeout(
        &mut self,
        duration: Millis,
        callback: impl 'static + FnOnce(&mut Self),
    ) -> Timeout {
        self.last_timeout += 1;
        self.timeout_queue.push(Reverse(TimeoutState {
            id: self.last_timeout,
            notified: self.whoami,
            expire: self.now + duration,
            callback: Box::new(callback),
        }));
        // FIXME(binary_heap_retain)
        // use a positive set instead a negative set, so no existance checking
        // upon cancellation
        self.enable_set.insert(self.last_timeout);
        self.last_timeout
    }
    fn cancel_timeout(&mut self, timeout: Timeout) {
        self.enable_set.remove(&timeout);
    }
}

impl<P: Protocol<Self, A>, A> Engine<P, A> {
    pub fn client(&mut self, addr: &SocketAddr) -> &mut impl Invoke<P>
    where
        Self: Invoke<P>,
    {
        self.whoami = Some(*addr);
        self
    }
    pub fn sleep(&mut self, duration: Millis) -> Reliable<()> {
        let (out, fut) = reliable();
        self.whoami = None;
        self.create_timeout(duration, |_| out.send(()).unwrap());
        fut
    }
    pub fn cancel_all_timeout(&mut self) {
        self.timeout_queue.clear();
        self.enable_set.clear();
    }
}
pub enum Filter<Msg> {
    Native(Box<dyn Fn(&mut MsgEnvelop<Msg>) -> bool>),
    All,
}
impl<Msg, F: 'static + Fn(&mut MsgEnvelop<Msg>) -> bool> From<F> for Filter<Msg> {
    fn from(native: F) -> Self {
        Self::Native(Box::new(native))
    }
}
impl<P: Protocol<Self, A>, A> Engine<P, A> {
    pub fn add_filter(&mut self, id: u64, filter: Filter<P::Msg>) {
        if let Filter::Native(filter) = filter {
            self.filter_map.insert(id, filter);
        } else {
            unimplemented!();
        }
    }
    pub fn remove_filter(&mut self, id: u64) {
        self.filter_map.remove(&id);
    }
}

pub struct Spawner<Engine>(Sender<Step<Engine>>);
impl<E> Clone for Spawner<E> {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}
impl<E> Spawner<E> {
    // FIXME(generators)
    // although `yield` probably cannot do a in-place replacing
    pub async fn spawn<T: 'static, Fut: 'static + Future<Output = T>>(
        self,
        step: impl 'static + FnOnce(&mut E) -> Fut,
    ) -> T {
        let (t_in, t_out) = reliable();
        let step = Box::new(|this: &mut E| -> LocalBoxFuture<'static, ()> {
            let pending = step(this);
            Box::pin(async {
                t_in.send(pending.await).ok().unwrap();
            })
        });
        self.0.send(step).unwrap();
        t_out.await
    }
}
impl<P: Protocol<Engine<P, A>, A>, A> Spawner<Engine<P, A>> {
    pub async fn invoke(&self, addr: &SocketAddr, op: String) -> String
    where
        Engine<P, A>: Invoke<P>,
    {
        let addr = *addr;
        self.clone()
            .spawn(move |engine| engine.client(&addr).invoke(op))
            .await
    }
    pub async fn invoke_unlogged(
        &self,
        addr: &SocketAddr,
        op: String,
        timeout: Millis,
    ) -> Result<String, Canceled>
    where
        Engine<P, A>: Invoke<P>,
    {
        let addr = *addr;
        self.clone()
            .spawn(move |engine| engine.client(&addr).invoke_unlogged(op, timeout))
            .await
    }
    pub async fn sleep(&self, duration: Millis) {
        self.clone()
            .spawn(move |engine| engine.sleep(duration))
            .await
    }
    pub async fn cancel_all_timeout(&self, after: Millis) {
        self.sleep(after).await;
        self.clone()
            .spawn(|engine| {
                engine.cancel_all_timeout();
                async {}
            })
            .await;
    }
}
impl<P: Protocol<Self, A>, A> Engine<P, A> {
    pub fn sched<Fut: 'static + Future<Output = ()>>(
        &mut self,
        task: impl FnOnce(Spawner<Self>) -> Fut,
    ) {
        self.task_list
            .push(Box::pin(task(Spawner(self.step_in.clone()))));
    }
}
struct FlagWaker(AtomicBool);
impl ArcWake for FlagWaker {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        arc_self.0.store(true, Ordering::Relaxed);
    }
}
impl<P: Protocol<Self, A>, A> Engine<P, A> {
    pub fn run(mut self) -> bool
    where
        Self: Recv<P, A, { Role::Client as u8 }, Msg = P::Msg>
            + Recv<P, A, { Role::Server as u8 }, Msg = P::Msg>,
        P::Msg: Debug,
    {
        let mut msg_queue = BinaryHeap::new();
        loop {
            let flag = Arc::new(FlagWaker(AtomicBool::new(false)));
            let waker = waker_ref(&flag);
            // step 1, poll tasks and collect steps, messages and timeouts
            self.task_list = self
                .task_list
                .into_iter()
                .filter_map(|mut task| {
                    if task
                        .as_mut()
                        .poll(&mut Context::from_waker(&waker))
                        .is_pending()
                    {
                        Some(task)
                    } else {
                        None
                    }
                })
                .collect();
            // positive exit: all tasks done
            if self.task_list.is_empty() {
                assert!(self.step_out.try_recv().is_err()); // task should wait on step
                return true;
            }
            // step 2, extend task list with collected steps
            let step_list: Vec<_> = self.step_out.try_iter().collect();
            let step_task: Vec<_> = step_list.into_iter().map(|step| step(&mut self)).collect();
            if !step_task.is_empty() || flag.0.load(Ordering::Relaxed) {
                self.task_list.extend(step_task);
                continue; // shortcut, advance tasks as soon as possible
            }
            // step 3, deliver all "arrived" messages
            let flush = |msg_queue: &mut BinaryHeap<_>, this: &mut Self| {
                msg_queue.extend(this.msg_out.try_iter().map(Reverse));
                while this
                    .timeout_queue
                    .peek()
                    .map(|Reverse(timeout)| !this.enable_set.contains(&timeout.id))
                    .unwrap_or(false)
                {
                    this.timeout_queue.pop();
                }
            };
            flush(&mut msg_queue, &mut self);
            while !msg_queue.is_empty()
                && (self.timeout_queue.is_empty()
                    || msg_queue.peek().unwrap().0.delivered
                        < self.timeout_queue.peek().unwrap().0.expire)
            {
                let Reverse(envelop) = msg_queue.pop().unwrap();
                assert!(self.now <= envelop.delivered);
                debug!(
                    "[now = {}] {} -> {}: {:?}",
                    self.now, &envelop.src, &envelop.dst, &envelop.msg
                );
                self.now = envelop.delivered;
                self.whoami = Some(envelop.dst);
                // maybe i am the only one on this planet
                // to be such a crazy guy:)
                (match self.recv_map.get(self.whoami.as_ref().unwrap()).unwrap() {
                    RecvState::Client(_) => <Self as Recv<P, A, { Role::Client as u8 }>>::recv_msg,
                    RecvState::Server(_) => <Self as Recv<P, A, { Role::Server as u8 }>>::recv_msg,
                })(&mut self, &envelop.src, envelop.msg);
                msg_queue.extend(self.msg_out.try_iter().map(Reverse));
                flush(&mut msg_queue, &mut self);
            }
            if flag.0.load(Ordering::Relaxed) {
                continue; // same shortcut
            }
            // step 4, deliver next timeout
            if let Some(Reverse(timeout)) = self.timeout_queue.pop() {
                assert!(self.enable_set.contains(&timeout.id));
                self.enable_set.remove(&timeout.id);
                assert!(self.now <= timeout.expire);
                self.now = timeout.expire;
                self.whoami = timeout.notified;
                (timeout.callback)(&mut self);
            } else {
                // negative exit: no pending message, no enabled timeout, has task but cannot advance
                return false;
            }
        }
    }
}
