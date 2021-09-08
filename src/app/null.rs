use crate::*;

pub struct Null;
impl App for Null {
    type Op = ();
    type Res = ();
    fn replica_upcall(&mut self, _op_num: OpNum, _op: Self::Op) -> Self::Res {}
    fn unlogged_upcall(&mut self, _op: Self::Op) -> Self::Res {}
}
