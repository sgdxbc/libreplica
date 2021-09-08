//!

use crate::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mock(pub Vec<Upcall>);
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Upcall {
    ReplicaUpcall(OpNum, String),
    UnloggedUpcall(String),
}
impl App for Mock {
    type Op = String;
    type Res = String;
    fn replica_upcall(&mut self, op_num: OpNum, op: Self::Op) -> Self::Res {
        self.0.push(Upcall::ReplicaUpcall(op_num, op.clone()));
        format!("Reply: {}", op)
    }
    fn unlogged_upcall(&mut self, op: Self::Op) -> Self::Res {
        self.0.push(Upcall::UnloggedUpcall(op.clone()));
        format!("Unlogged reply: {}", op)
    }
}
