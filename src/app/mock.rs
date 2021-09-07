//!

use crate::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppState(pub Vec<Upcall>);
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Upcall {
    ReplicaUpcall(OpNum, String),
    UnloggedUpcall(String),
}
pub struct App;
impl AppMeta for App {
    type State = AppState;
}
impl<T: BorrowMut<ReplicaState<App>>> crate::App<App> for T {
    fn replica_upcall(&mut self, op_num: OpNum, op: String) -> String {
        self.borrow_mut()
            .app
            .0
            .push(Upcall::ReplicaUpcall(op_num, op.clone()));
        format!("Reply: {}", op)
    }
    fn unlogged_upcall(&mut self, op: String) -> String {
        self.borrow_mut()
            .app
            .0
            .push(Upcall::UnloggedUpcall(op.clone()));
        format!("Unlogged reply: {}", op)
    }
}
