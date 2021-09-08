//!
use std::borrow::{Borrow, BorrowMut};
use std::collections::HashMap;
use std::marker::PhantomData;

use derivative::Derivative;
use log::*;
use serde_derive::{Deserialize, Serialize};

use crate::util::*;
use crate::*;

mod msg {
    use std::mem::take;

    use serde_derive::{Deserialize, Serialize};

    use crate::util;
    use crate::*;

    #[derive(Debug, Clone, Deserialize, Serialize)]
    pub struct Req<Op> {
        pub op: Op,
        pub name: ClientName,
        pub seq: u64,
    }
    impl<A: App> util::Req<A> for Req<A::Op> {
        fn get_op(&self) -> &A::Op {
            &self.op
        }
        fn take_op(&mut self) -> A::Op
        where
            A::Op: Default,
        {
            take(&mut self.op)
        }
    }
    #[derive(Debug, Clone, Default, Deserialize, Serialize)]
    pub struct Reply<Res> {
        pub result: Res,
        pub seq: u64,
    }
    impl<A: App> util::Reply<A> for Reply<A::Res> {
        fn set_result(&mut self, result: A::Res) {
            self.result = result;
        }
    }
    #[derive(Debug, Clone, Deserialize, Serialize)]
    pub struct UnloggedReq<Op> {
        pub op: Op,
    }
    impl<A: App> util::Req<A> for UnloggedReq<A::Op> {
        fn get_op(&self) -> &A::Op {
            &self.op
        }
        fn take_op(&mut self) -> A::Op
        where
            A::Op: Default,
        {
            take(&mut self.op)
        }
    }
    #[derive(Debug, Clone, Default, Deserialize, Serialize)]
    pub struct UnloggedReply<Res> {
        pub result: Res,
    }
    impl<A: App> util::Reply<A> for UnloggedReply<A::Res> {
        fn set_result(&mut self, result: A::Res) {
            self.result = result;
        }
    }
}
#[derive(Derivative, Deserialize, Serialize)]
#[derivative(Debug, Clone(bound = ""))]
pub enum Msg<A: App> {
    #[derivative(Debug = "transparent")]
    Req(msg::Req<A::Op>),
    #[derivative(Debug = "transparent")]
    Reply(msg::Reply<A::Res>),
    #[derivative(Debug = "transparent")]
    UnloggedReq(msg::UnloggedReq<A::Op>),
    #[derivative(Debug = "transparent")]
    UnloggedReply(msg::UnloggedReply<A::Res>),
}

pub struct ClientState<T: ?Sized, A: App> {
    name: ClientName,
    seq: u64,
    req: Option<PendingReq<A>>,
    unlogged_req: Option<PendingUnlogged<A>>,
    req_timer: TimerState<T>,
    unlogged_timer: TimerState<T>,
}
struct PendingReq<A: App> {
    op: A::Op,
    seq: u64,
    out: Out<A::Res>,
}
struct PendingUnlogged<A: App>(Out<A::Res>);
impl<T: Client<A>, A: App> Default for ClientState<T, A> {
    fn default() -> Self {
        Self {
            name: ClientName::default(),
            seq: 0,
            req: None,
            unlogged_req: None,
            req_timer: timer(1000, T::on_req),
            unlogged_timer: timer(
                <T as Invoke<Unreplicated, A>>::DEFAULT_UNLOGGED_TIMEOUT,
                T::on_unlogged,
            ),
        }
    }
}
impl<T: Client<A>, A: App> Borrow<ClientName> for ClientState<T, A> {
    fn borrow(&self) -> &ClientName {
        &self.name
    }
}
impl<T: Client<A>, A: App> Invoke<Unreplicated, A> for T {
    fn invoke(&mut self, op: A::Op) -> Reliable<A::Res> {
        assert!(self.borrow_mut().req.is_none(), "one request at a time");
        let (out, fut) = reliable();
        self.borrow_mut().seq += 1;
        self.borrow_mut().req = Some(PendingReq {
            op,
            seq: self.borrow_mut().seq,
            out,
        });
        self.send_req();
        fut
    }
    fn invoke_unlogged(&mut self, op: A::Op, timeout: Millis) -> Unreliable<A::Res> {
        assert!(
            self.borrow_mut().unlogged_req.is_none(),
            "one unlogged request at a time"
        );
        let (out, fut) = unreliable();
        self.borrow_mut().unlogged_req = Some(PendingUnlogged(out));
        let msg = msg::UnloggedReq { op };
        self.send_msg_to_replica(0, Msg::UnloggedReq(msg));
        self.unlogged_timer().interval = timeout;
        self.start(Self::unlogged_timer);
        fut
    }
}
impl<T: Client<A>, A: App> Recv<Unreplicated, A, { Role::Client as u8 }> for T {
    type Msg = Msg<A>;
    type Addr = T::Addr;
    fn recv_msg(&mut self, remote: &Self::Addr, msg: Self::Msg) {
        match msg {
            Msg::Reply(msg) => self.handle_reply(remote, msg),
            Msg::UnloggedReply(msg) => self.handle_unlogged_reply(remote, msg),
            _ => unreachable!(),
        }
    }
}
impl<
        T: Transport<Msg = Msg<A>>
            + TimerKit
            + TaggedBorrowMut<ClientState<Self, A>, { Role::Client as u8 }>,
        A: App,
    > Client<A> for T
{
}
pub trait Client<A: App>:
    Transport<Msg = Msg<A>> + TimerKit + TaggedBorrowMut<ClientState<Self, A>, { Role::Client as u8 }>
{
    fn req_timer(&mut self) -> &mut TimerState<Self> {
        &mut self.borrow_mut().req_timer
    }
    fn unlogged_timer(&mut self) -> &mut TimerState<Self> {
        &mut self.borrow_mut().unlogged_timer
    }

    fn on_req(&mut self) {
        warn!("resend request: seq = {}", self.borrow_mut().seq);
        self.send_req();
    }
    fn on_unlogged(&mut self) {
        warn!("unlogged request timeout");
        self.borrow_mut().unlogged_req.take(); // drop the pipe to cancel
        self.stop(Self::unlogged_timer);
    }

    fn handle_reply(&mut self, _remote: &Self::Addr, msg: msg::Reply<A::Res>) {
        if self
            .borrow_mut()
            .req
            .as_ref()
            .map(|req| req.seq != msg.seq)
            .unwrap_or(true)
        {
            return;
        }
        self.stop(Self::req_timer);
        let req = self.borrow_mut().req.take().unwrap();
        req.out.send(msg.result).ok().unwrap();
    }
    fn handle_unlogged_reply(&mut self, _remote: &Self::Addr, msg: msg::UnloggedReply<A::Res>) {
        let req = self.borrow_mut().unlogged_req.take();
        if req.is_none() {
            return;
        }
        req.unwrap().0.send(msg.result).ok().unwrap();
    }

    fn send_req(&mut self) {
        let msg = msg::Req {
            name: self.borrow_mut().name,
            op: self.borrow_mut().req.as_ref().unwrap().op.clone(),
            seq: self.borrow_mut().seq,
        };
        self.send_msg_to_replica(0, Msg::Req(msg));
        self.reset(Self::req_timer);
    }
}

pub struct ServerState<T: ?Sized, A: App> {
    op_num: OpNum,
    replica: Replica<A>,
    client_table: HashMap<ClientName, msg::Reply<A::Res>>,
    _this: PhantomData<T>,
}
impl<T, A: App> From<Replica<A>> for ServerState<T, A> {
    fn from(replica: Replica<A>) -> Self {
        Self {
            op_num: 0,
            replica,
            client_table: HashMap::new(),
            _this: PhantomData,
        }
    }
}
impl<T, A: App> Borrow<Replica<A>> for ServerState<T, A> {
    fn borrow(&self) -> &Replica<A> {
        &self.replica
    }
}
impl<T, A: App> BorrowMut<Replica<A>> for ServerState<T, A> {
    fn borrow_mut(&mut self) -> &mut Replica<A> {
        &mut self.replica
    }
}
impl<T: Server<A>, A: App> Recv<Unreplicated, A, { Role::Server as u8 }> for T
where
    ServerState<Self, A>: Execute<A>,
{
    type Msg = Msg<A>;
    type Addr = T::Addr;
    fn recv_msg(&mut self, remote: &Self::Addr, msg: Self::Msg) {
        match msg {
            Msg::Req(msg) => self.handle_req(remote, msg),
            Msg::UnloggedReq(msg) => self.handle_unlogged_req(remote, msg),
            _ => unreachable!(),
        }
    }
}
impl<
        T: TransportCore<Msg = Msg<A>> + TaggedBorrowMut<ServerState<T, A>, { Role::Server as u8 }>,
        A: App,
    > Server<A> for T
{
}
pub trait Server<A: App>:
    TransportCore<Msg = Msg<A>> + TaggedBorrowMut<ServerState<Self, A>, { Role::Server as u8 }>
{
    fn handle_req(&mut self, remote: &Self::Addr, msg: msg::Req<A::Op>)
    where
        ServerState<Self, A>: Execute<A>,
    {
        if let Some(reply_msg) = self.borrow_mut().client_table.get(&msg.name) {
            let last_seq = reply_msg.seq;
            if last_seq == msg.seq {
                info!("resend last request");
                let reply_msg = reply_msg.clone();
                self.send_msg(remote, Msg::Reply(reply_msg));
            }
            if last_seq >= msg.seq {
                info!("skip duplicated request");
                return;
            }
        }
        self.borrow_mut().op_num += 1;
        let mut reply_msg = msg::Reply {
            seq: msg.seq,
            ..Default::default()
        };
        let op_num = self.borrow_mut().op_num;
        let name = msg.name;
        self.borrow_mut().execute(op_num, msg, &mut reply_msg);
        self.borrow_mut()
            .client_table
            .insert(name, reply_msg.clone());
        self.send_msg(remote, Msg::Reply(reply_msg));
    }
    fn handle_unlogged_req(&mut self, remote: &Self::Addr, msg: msg::UnloggedReq<A::Op>)
    where
        ServerState<Self, A>: Execute<A>,
    {
        let mut reply_msg = msg::UnloggedReply::default();
        self.borrow_mut().execute_unlogged(msg, &mut reply_msg);
        self.send_msg(remote, Msg::UnloggedReply(reply_msg));
    }
}

pub struct Unreplicated;
impl<T, A: App> Protocol<T, A> for Unreplicated {
    type Client = ClientState<T, A>;
    type Server = ServerState<T, A>;
    type Msg = Msg<A>;
}

#[cfg(test)]
mod tests {
    use std::iter::repeat;

    use futures::future::join_all;
    use log::LevelFilter;
    use rand::random;
    use simple_logger::SimpleLogger;

    use super::*;
    use crate::app::{Mock, Upcall};
    use crate::engine::sim::*;
    fn setup(nb_client: u64) -> Engine<Unreplicated, Mock> {
        let _ = SimpleLogger::new().with_level(LevelFilter::Debug).init();
        let mut engine = Engine::new(Config {
            f: 0,
            replica_list: vec!["server1".to_string()],
            multicast: None,
        });
        engine.spawn_server(Replica {
            index: 0,
            init: true,
            app: Mock(Vec::new()),
        });
        for i in 0..nb_client {
            engine.spawn_client(format!("client{}", i + 1));
        }
        engine
    }

    #[test]
    fn bootstrap() {
        let _ = setup(1);
    }

    #[test]
    fn two_op() {
        let mut engine = setup(1);
        engine.sched(|task| async move {
            task.cancel_all_timeout(0).await;
        });
        engine.sched(|task| async move {
            assert_eq!(
                task.invoke("client1", "Hello".to_string()).await,
                "Reply: Hello"
            );
            assert_eq!(
                task.invoke("client1", "Hello (again)".to_string()).await,
                "Reply: Hello (again)"
            );

            let step = task.spawn(|engine| {
                assert_eq!(
                    *engine.app(0),
                    Mock(vec![
                        Upcall::ReplicaUpcall(1, "Hello".to_string()),
                        Upcall::ReplicaUpcall(2, "Hello (again)".to_string()),
                    ])
                );
                async {}
            });
            step.await;
        });
        assert!(engine.run());
    }

    #[test]
    fn unlogged_op() {
        let mut engine = setup(1);
        engine.sched(|task| async move {
            task.cancel_all_timeout(0).await;
        });
        engine.sched(|task| async move {
            assert_eq!(
                task.invoke_unlogged(
                    "client1",
                    "Hello".to_string(),
                    <Engine<Unreplicated, Mock> as Invoke<_, _>>::DEFAULT_UNLOGGED_TIMEOUT
                )
                .await,
                Ok("Unlogged reply: Hello".to_string())
            );
            task.spawn(|engine| {
                assert_eq!(
                    *engine.app(0),
                    Mock(vec![Upcall::UnloggedUpcall("Hello".to_string())])
                );
                async {}
            })
            .await;
        });
        assert!(engine.run());
    }

    #[test]
    fn cannot_finish() {
        let mut engine = setup(1);
        engine.sched(|task| async move {
            task.cancel_all_timeout(0).await;
        });
        engine.sched(|task| async move {
            task.spawn(move |engine| {
                engine.add_filter(1, Filter::from(|_: &mut MsgEnvelop<_>| false));
                engine.client("client1").invoke("Never done".to_string())
            })
            .await;
        });
        assert!(!engine.run());
    }

    #[test]
    fn unlogged_timeout() {
        let mut engine = setup(1);
        engine.sched(|task| async move {
            task.cancel_all_timeout(10).await;
        });
        engine.sched(|task| async move {
            let step = task.clone().spawn(move |engine| {
                engine.add_filter(1, Filter::from(|_: &mut MsgEnvelop<_>| false));
                engine
                    .client("client1")
                    .invoke_unlogged("Never done".to_string(), 3)
            });
            assert_eq!(step.await, Err(Canceled));
        });
        assert!(engine.run());
    }

    #[test]
    fn resend() {
        let mut engine = setup(1);
        engine.sched(|task| async move {
            task.spawn(move |engine| {
                engine.add_filter(1, Filter::from(|_: &mut MsgEnvelop<_>| false));
                engine.sleep(1)
            })
            .await;
            task.spawn(move |engine| {
                engine.remove_filter(1);
                async {}
            })
            .await;
            task.cancel_all_timeout(1000).await;
        });
        engine.sched(|task| async move {
            task.spawn(move |engine| {
                engine.add_filter(1, Filter::from(|_: &mut MsgEnvelop<_>| false));
                engine
                    .client("client1")
                    .invoke("Hello (resend)".to_string())
            })
            .await;
        });
        assert!(engine.run());
    }

    #[test]
    fn duplicated() {
        let mut engine = setup(1);
        engine.sched(|task| async move {
            task.spawn(move |engine| {
                engine.add_filter(
                    1,
                    Filter::from(move |envelop: &mut MsgEnvelop<_>| envelop.dst != "client1"),
                );
                engine.sleep(1)
            })
            .await;
            task.spawn(move |engine| {
                engine.remove_filter(1);
                async {}
            })
            .await;
            task.cancel_all_timeout(1000).await;
        });
        engine.sched(|task| async move {
            assert_eq!(
                task.invoke("client1", "Hello (duplicated)".to_string())
                    .await,
                "Reply: Hello (duplicated)"
            );
            task.spawn(|engine| {
                assert_eq!(engine.app(0).0.len(), 1);
                async {}
            })
            .await;
        });
        assert!(engine.run());
    }

    #[test]
    fn client_party() {
        let mut engine = setup(10);
        engine.sched(|task| async move {
            join_all(
                repeat(task.clone())
                    .take(10)
                    .enumerate()
                    .map(|(i, task)| async move {
                        for n in 0..10 {
                            task.sleep(random::<u64>() % 100).await;
                            assert_eq!(
                                task.invoke(&format!("client{}", i + 1), format!("Ping {}", n))
                                    .await,
                                format!("Reply: Ping {}", n)
                            );
                        }
                    }),
            )
            .await;
            task.spawn(|engine| {
                assert_eq!(engine.app(0).0.len(), 100);
                let mut prev = 0;
                for upcall in &engine.app(0).0 {
                    if let Upcall::ReplicaUpcall(op_num, _) = upcall {
                        assert!(*op_num > prev);
                        prev = *op_num;
                    } else {
                        unreachable!();
                    }
                }
                async {}
            })
            .await;
        });
        assert!(engine.run());
    }
}
