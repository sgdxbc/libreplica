//!
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

    #[derive(Debug, Clone, Deserialize, Serialize)]
    pub struct Req {
        pub op: String,
        pub seq: u64,
    }
    impl util::Req for Req {
        fn get_op(&self) -> &str {
            &self.op
        }
        fn take_op(&mut self) -> String {
            take(&mut self.op)
        }
    }
    #[derive(Debug, Clone, Default, Deserialize, Serialize)]
    pub struct Reply {
        pub result: String,
        pub seq: u64,
    }
    impl util::Reply for Reply {
        fn set_result(&mut self, result: String) {
            self.result = result;
        }
    }
    #[derive(Debug, Clone, Deserialize, Serialize)]
    pub struct UnloggedReq {
        pub op: String,
    }
    impl util::Req for UnloggedReq {
        fn get_op(&self) -> &str {
            &self.op
        }
        fn take_op(&mut self) -> String {
            take(&mut self.op)
        }
    }
    #[derive(Debug, Clone, Default, Deserialize, Serialize)]
    pub struct UnloggedReply {
        pub result: String,
    }
    impl util::Reply for UnloggedReply {
        fn set_result(&mut self, result: String) {
            self.result = result;
        }
    }
}
#[derive(Clone, Derivative, Deserialize, Serialize)]
#[derivative(Debug)]
pub enum Msg {
    #[derivative(Debug = "transparent")]
    Req(msg::Req),
    #[derivative(Debug = "transparent")]
    Reply(msg::Reply),
    #[derivative(Debug = "transparent")]
    UnloggedReq(msg::UnloggedReq),
    #[derivative(Debug = "transparent")]
    UnloggedReply(msg::UnloggedReply),
}

pub struct ClientState<T: ?Sized> {
    seq: u64,
    req: Option<PendingReq>,
    unlogged_req: Option<PendingUnlogged>,
    req_timer: TimerState<T>,
    unlogged_timer: TimerState<T>,
}
struct PendingReq {
    op: String,
    seq: u64,
    out: Out<String>,
}
struct PendingUnlogged(Out<String>);
impl<T: Client> Default for ClientState<T> {
    fn default() -> Self {
        Self {
            seq: 0,
            req: None,
            unlogged_req: None,
            req_timer: timer(1000, T::on_req),
            unlogged_timer: timer(T::DEFAULT_UNLOGGED_TIMEOUT, T::on_unlogged),
        }
    }
}
impl<T: Client> crate::ClientState for ClientState<T> {}
impl<T: Client> Invoke<Unreplicated> for T {
    fn invoke(&mut self, op: String) -> Reliable<String> {
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
    fn invoke_unlogged(&mut self, op: String, timeout: Millis) -> Unreliable<String> {
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
impl<T: Client, A> Recv<Unreplicated, A, { Role::Client as u8 }> for T {
    type Msg = Msg;
    fn recv_msg(&mut self, remote: &SocketAddr, msg: Self::Msg) {
        match msg {
            Msg::Reply(msg) => self.handle_reply(remote, msg),
            Msg::UnloggedReply(msg) => self.handle_unlogged_reply(remote, msg),
            _ => unreachable!(),
        }
    }
}
impl<
        T: Transport<Msg = Msg>
            + TimerKit
            + TaggedBorrowMut<ClientState<Self>, { Role::Client as u8 }>,
    > Client for T
{
}
pub trait Client:
    Transport<Msg = Msg> + TimerKit + TaggedBorrowMut<ClientState<Self>, { Role::Client as u8 }>
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

    fn handle_reply(&mut self, _remote: &SocketAddr, msg: msg::Reply) {
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
        req.out.send(msg.result).unwrap();
    }
    fn handle_unlogged_reply(&mut self, _remote: &SocketAddr, msg: msg::UnloggedReply) {
        let req = self.borrow_mut().unlogged_req.take();
        if req.is_none() {
            return;
        }
        req.unwrap().0.send(msg.result).unwrap();
    }

    fn send_req(&mut self) {
        let msg = msg::Req {
            op: self.borrow_mut().req.as_ref().unwrap().op.to_string(),
            seq: self.borrow_mut().seq,
        };
        self.send_msg_to_replica(0, Msg::Req(msg));
        self.reset(Self::req_timer);
    }
}

pub struct ServerState<T: ?Sized, App: AppMeta> {
    op_num: OpNum,
    replica: ReplicaState<App>,
    client_table: HashMap<SocketAddr, msg::Reply>,
    _this: PhantomData<T>,
}
impl<T, A: AppMeta> From<ReplicaState<A>> for ServerState<T, A> {
    fn from(replica: ReplicaState<A>) -> Self {
        Self {
            op_num: 0,
            replica,
            client_table: HashMap::new(),
            _this: PhantomData,
        }
    }
}
impl<T, A: AppMeta> Borrow<ReplicaState<A>> for ServerState<T, A> {
    fn borrow(&self) -> &ReplicaState<A> {
        &self.replica
    }
}
impl<T, A: AppMeta> BorrowMut<ReplicaState<A>> for ServerState<T, A> {
    fn borrow_mut(&mut self) -> &mut ReplicaState<A> {
        &mut self.replica
    }
}
impl<T, A: AppMeta> crate::ServerState<A> for ServerState<T, A> {}
impl<T: Server<A>, A: AppMeta> Recv<Unreplicated, A, { Role::Server as u8 }> for T
where
    ServerState<T, A>: App<A>,
{
    type Msg = Msg;
    fn recv_msg(&mut self, remote: &SocketAddr, msg: Self::Msg) {
        match msg {
            Msg::Req(msg) => self.handle_req(remote, msg),
            Msg::UnloggedReq(msg) => self.handle_unlogged_req(remote, msg),
            _ => unreachable!(),
        }
    }
}
impl<
        T: TransportCore<Msg = Msg> + TaggedBorrowMut<ServerState<T, A>, { Role::Server as u8 }>,
        A: AppMeta,
    > Server<A> for T
where
    ServerState<Self, A>: App<A>,
{
}
pub trait Server<A: AppMeta>:
    TransportCore<Msg = Msg> + TaggedBorrowMut<ServerState<Self, A>, { Role::Server as u8 }>
where
    ServerState<Self, A>: App<A>,
{
    fn handle_req(&mut self, remote: &SocketAddr, msg: msg::Req) {
        if let Some(reply_msg) = self.borrow_mut().client_table.get(remote) {
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
        self.borrow_mut().execute(op_num, msg, &mut reply_msg);
        self.borrow_mut()
            .client_table
            .insert(*remote, reply_msg.clone());
        self.send_msg(remote, Msg::Reply(reply_msg));
    }
    fn handle_unlogged_req(&mut self, remote: &SocketAddr, msg: msg::UnloggedReq) {
        let mut reply_msg = msg::UnloggedReply::default();
        self.borrow_mut().execute_unlogged(msg, &mut reply_msg);
        self.send_msg(remote, Msg::UnloggedReply(reply_msg));
    }
}

pub struct Unreplicated;
impl<T, A: AppMeta> Protocol<T, A> for Unreplicated {
    type Client = ClientState<T>;
    type Server = ServerState<T, A>;
    type Msg = Msg;
}

#[cfg(test)]
mod tests {
    use log::LevelFilter;
    use simple_logger::SimpleLogger;

    use super::*;
    use crate::app::mock::{App as Mock, AppState, Upcall};
    use crate::engine::sim::*;
    fn setup(nb_client: u64) -> (Engine<Unreplicated, Mock>, Vec<SocketAddr>) {
        let _ = SimpleLogger::new().with_level(LevelFilter::Debug).init();
        let mut engine = Engine::new(Config {
            f: 0,
            replica_list: vec!["10.0.0.1:3001".parse().unwrap()],
            multicast: None,
        });
        engine.spawn_server(ReplicaState {
            index: 0,
            init: true,
            app: AppState(Vec::new()),
        });
        let client_list = (0..nb_client)
            .map(|_| engine.spawn_client("10.0.0.101".parse().unwrap()))
            .collect();
        (engine, client_list)
    }

    #[test]
    fn bootstrap() {
        let (_engine, client_list) = setup(1);
        assert_eq!(client_list.len(), 1);
    }

    #[test]
    fn two_op() {
        let (mut engine, client_list) = setup(1);
        engine.sched(|task| async move {
            task.cancel_all_timeout(0).await;
        });
        engine.sched(|task| async move {
            let client = &client_list[0];
            assert_eq!(
                task.invoke(client, "Hello".to_string()).await,
                "Reply: Hello"
            );
            assert_eq!(
                task.invoke(client, "Hello (again)".to_string()).await,
                "Reply: Hello (again)"
            );

            let step = task.spawn(|engine| {
                assert_eq!(
                    *engine.app(0),
                    AppState(vec![
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
        let (mut engine, client_list) = setup(1);
        engine.sched(|task| async move {
            task.cancel_all_timeout(0).await;
        });
        engine.sched(|task| async move {
            let client = &client_list[0];
            assert_eq!(
                task.invoke_unlogged(
                    client,
                    "Hello".to_string(),
                    <Engine<Unreplicated, Mock> as Invoke<_>>::DEFAULT_UNLOGGED_TIMEOUT
                )
                .await,
                Ok("Unlogged reply: Hello".to_string())
            );
            task.spawn(|engine| {
                assert_eq!(
                    *engine.app(0),
                    AppState(vec![Upcall::UnloggedUpcall("Hello".to_string())])
                );
                async {}
            })
            .await;
        });
        assert!(engine.run());
    }

    #[test]
    fn cannot_finish() {
        let (mut engine, client_list) = setup(1);
        engine.sched(|task| async move {
            task.cancel_all_timeout(0).await;
        });
        engine.sched(|task| async move {
            let addr = client_list[0];
            task.spawn(move |engine| {
                engine.add_filter(1, Filter::from(|_: &mut MsgEnvelop<_>| false));
                engine.client(&addr).invoke("Never done".to_string())
            })
            .await;
        });
        assert!(!engine.run());
    }

    #[test]
    fn unlogged_timeout() {
        let (mut engine, client_list) = setup(1);
        engine.sched(|task| async move {
            task.cancel_all_timeout(10).await;
        });
        engine.sched(|task| async move {
            let addr = client_list[0];
            let step = task.clone().spawn(move |engine| {
                engine.add_filter(1, Filter::from(|_: &mut MsgEnvelop<_>| false));
                engine
                    .client(&addr)
                    .invoke_unlogged("Never done".to_string(), 3)
            });
            assert_eq!(step.await, Err(Canceled));
        });
        assert!(engine.run());
    }

    #[test]
    fn resend() {
        let (mut engine, client_list) = setup(1);
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
            let addr = client_list[0];
            task.spawn(move |engine| {
                engine.add_filter(1, Filter::from(|_: &mut MsgEnvelop<_>| false));
                engine.client(&addr).invoke("Hello (resend)".to_string())
            })
            .await;
        });
        assert!(engine.run());
    }

    #[test]
    fn duplicated() {
        let (mut engine, client_list) = setup(1);
        let client = client_list[0].clone();
        engine.sched(|task| async move {
            task.spawn(move |engine| {
                engine.add_filter(
                    1,
                    Filter::from(move |envelop: &mut MsgEnvelop<_>| envelop.dst != client),
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
                task.invoke(&client_list[0], "Hello (duplicated)".to_string())
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
}
