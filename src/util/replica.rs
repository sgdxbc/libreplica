use log::*;

use crate::*;

pub trait Req {
    fn get_op(&self) -> &str;
    fn take_op(&mut self) -> String;
}
pub trait Reply {
    fn set_result(&mut self, result: String);
}
pub trait Replica<App> {
    fn execute(&mut self, op_num: OpNum, req: impl Req, reply: &mut impl Reply);
    fn execute_unlogged(&mut self, req: impl Req, reply: &mut impl Reply);
}
/// `Replica` function is decoupled with its state. Funny.
impl<T: App<A>, A> Replica<A> for T {
    fn execute(&mut self, op_num: OpNum, mut req: impl Req, reply: &mut impl Reply) {
        debug!("replica upcall: num = {}, op = {}", op_num, req.get_op());
        reply.set_result(self.replica_upcall(op_num, req.take_op()));
    }
    fn execute_unlogged(&mut self, mut req: impl Req, reply: &mut impl Reply) {
        debug!("unlogged upcall: {}", req.get_op());
        reply.set_result(self.unlogged_upcall(req.take_op()));
    }
}
