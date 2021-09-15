//! A collection of common replica operations, most importantly at-most-once
//! execution, and rollback, etc.
//!
//! Basically the `Replica` class of specpaxos and something more. Through
//! `Replica` may be a better name, I don't want to make it confusing with name
//! of the crate.
use std::borrow::Borrow;
use std::collections::HashMap;

use log::*;
use serde_derive::{Deserialize, Serialize};

use crate::*;

pub struct Execute<A: App, Entry> {
    pub index: ReplicaIndex,
    pub app: A,
    client_table: HashMap<ClientName, AMORes<A>>,
    log: Vec<Entry>,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AMOOp<A: App> {
    pub op: A::Op,
    pub name: ClientName,
    pub seq: u64,
}
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct AMORes<A: App> {
    pub res: A::Res,
    pub seq: u64,
}
pub trait LogEntry<A: App>: Borrow<AMOOp<A>> {
    fn op_num(&self) -> OpNum;
    fn is_noop(&self) -> bool;
    fn set_noop(&mut self);
    fn set_res(&mut self, amo_res: AMORes<A>);
}
impl<A: App, E> Execute<A, E> {
    pub fn new(index: ReplicaIndex, app: A) -> Self {
        Self {
            index,
            app,
            client_table: HashMap::new(),
            log: Vec::new(),
        }
    }
    pub fn execute(&mut self, op_num: OpNum) -> Option<AMORes<A>>
    where
        AMORes<A>: Clone,
    {
        if let Some(amo_res) = self.client_table.get(&amo_op.name) {
            if amo_res.seq > amo_op.seq {
                return None;
            }
            if amo_res.seq == amo_op.seq {
                return Some(amo_res.clone());
            }
            assert_eq!(amo_op.seq, amo_res.seq + 1); // a sound client should send on every seq
        }
        debug!("replica upcall: num = {}, op = {:?}", op_num, amo_op.op);
        let amo_res = AMORes {
            res: self.app.replica_upcall(op_num, amo_op.op),
            seq: amo_op.seq,
        };
        self.client_table.insert(amo_op.name, amo_res.clone());
        Some(amo_res)
    }
    pub fn execute_unlogged(&mut self, op: A::Op) -> A::Res {
        debug!("unlogged upcall: {:?}", op);
        self.app.unlogged_upcall(op)
    }
}
impl<E: LogEntry<A>, A: App> Execute<A, E> {
    pub fn append(&mut self, entry: E) {
        assert_ne!(entry.op_num(), 0);
        assert!(self.log.is_empty() || entry.op_num() == self.log.last().unwrap().op_num() + 1);
        self.log.push(entry);
    }
    pub fn first_op_num(&self) -> OpNum {
        self.log.first().map(|entry| entry.op_num()).unwrap_or(0)
    }
    pub fn last_op_num(&self) -> OpNum {
        self.log.last().map(|entry| entry.op_num()).unwrap_or(0)
    }
    pub fn index(&self, op_num: OpNum) -> Option<E> {
        if let Some(first) = self.first_op_num() {
            self.log.get(op_num - first)
        } else {
            None
        }
    }
    pub fn rollback(&mut self, to: OpNum) {
        //
    }
}
