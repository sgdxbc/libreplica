use std::fmt::Debug;

use log::*;

use crate::*;

pub trait Req<A: App> {
    fn get_op(&self) -> &A::Op;
    fn take_op(&mut self) -> A::Op
    where
        A::Op: Default;
}
pub trait Reply<A: App> {
    fn set_result(&mut self, result: A::Res);
}
pub trait Execute<A: App> {
    fn execute(&mut self, op_num: OpNum, req: impl Req<A>, reply: &mut impl Reply<A>);
    fn execute_unlogged(&mut self, req: impl Req<A>, reply: &mut impl Reply<A>);
}
impl<A: App, T: ServerState<A>> Execute<A> for T
where
    A::Op: Debug + Default,
{
    fn execute(&mut self, op_num: OpNum, mut req: impl Req<A>, reply: &mut impl Reply<A>) {
        debug!("replica upcall: num = {}, op = {:?}", op_num, req.get_op());
        reply.set_result(self.borrow_mut().app.replica_upcall(op_num, req.take_op()));
    }
    fn execute_unlogged(&mut self, mut req: impl Req<A>, reply: &mut impl Reply<A>) {
        debug!("unlogged upcall: {:?}", req.get_op());
        reply.set_result(self.borrow_mut().app.unlogged_upcall(req.take_op()));
    }
}
