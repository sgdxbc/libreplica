use crate::*;

pub struct App;
impl AppMeta for App {
    type State = ();
}
impl<T> crate::App<App> for T {
    fn replica_upcall(&mut self, _op_num: OpNum, _op: String) -> String {
        String::new()
    }
    fn unlogged_upcall(&mut self, _op: String) -> String {
        String::new()
    }
}
