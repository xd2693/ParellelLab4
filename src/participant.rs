//!
//! participant.rs
//! Implementation of 2PC participant
//!
extern crate ipc_channel;
extern crate log;
extern crate rand;
extern crate stderrlog;

use std::collections::HashMap;
use std::ptr::null;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use std::thread;

use ipc_channel::ipc::IpcError;
use participant::rand::prelude::*;
use participant::ipc_channel::ipc::IpcReceiver as Receiver;
use participant::ipc_channel::ipc::TryRecvError;
use participant::ipc_channel::ipc::IpcSender as Sender;

use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

use crate::message;

///
/// ParticipantState
/// enum for Participant 2PC state machine
///
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {
    Quiescent,
    ReceivedP1,
    VotedAbort,
    VotedP1Commit,
    SentP1Commit,
    VotedP2Commit,
    AwaitingGlobalDecision,
}

///
/// Participant
/// Structure for maintaining per-participant state and communication/synchronization objects to/from coordinator
///
#[derive(Debug)]
pub struct Participant {
    id_str: String,
    state: ParticipantState,
    log: oplog::OpLog,
    running: Arc<AtomicBool>,
    send_success_prob: f64,
    operation_success_prob: f64,
    tx: Sender<ProtocolMessage>,
    rx: Receiver<ProtocolMessage>,
    successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,
    request_status: RequestStatus,
}

///
/// Participant
/// Implementation of participant for the 2PC protocol
/// Required:
/// 1. new -- Constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- Implements participant side protocol for 2PC
///
impl Participant {

    ///
    /// new()
    ///
    /// Return a new participant, ready to run the 2PC protocol with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course.
    ///
    pub fn new(
        id_str: String,
        log_path: String,
        r: Arc<AtomicBool>,
        send_success_prob: f64,
        operation_success_prob: f64,
        tx: Sender<ProtocolMessage>,
        rx: Receiver<ProtocolMessage>) -> Participant {

        Participant {
            id_str: id_str,
            state: ParticipantState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r,
            send_success_prob: send_success_prob,
            operation_success_prob: operation_success_prob,
            // TODO
            tx: tx,
            rx: rx,
            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,
            request_status: RequestStatus::Committed,
        }
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator. This can fail depending on
    /// the success probability. For testing purposes, make sure to not specify
    /// the -S flag so the default value of 1 is used for failproof sending.
    ///
    /// HINT: You will need to implement the actual sending
    ///
    pub fn send(&mut self, pm: ProtocolMessage) {
        let x: f64 = random();
        //trace!("random number is {}", x.to_string());
        if x <= self.send_success_prob  {
            // TODO: Send success
            let result = self.tx.send(pm);
            if result.is_err(){
                info!("send err {}", self.id_str);
            }
        } else {
            // TODO: Send fail
            return;
        }
    }

    ///
    /// perform_operation
    /// Perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the
    /// command-line option success_probability.
    ///
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic.
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than
    ///       bool if it's more convenient for your design).
    ///
    pub fn perform_operation(&mut self, request_option: ProtocolMessage) -> bool {

        //trace!("{}::Performing operation", self.id_str.clone());
        let x: f64 = random();
        if x <= self.operation_success_prob {
            // TODO: Successful operation
            return true;
        } else {
            // TODO: Failed operation
            return false;
        }

        
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        let successful_ops: u64 = 0;
        let failed_ops: u64 = 0;
        let unknown_ops: u64 = 0;

        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.id_str.clone(), self.successful_ops, self.failed_ops, self.unknown_ops);
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        // TODO
        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            let result = self.rx.recv();

        }

        trace!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {
        //trace!("{}::Beginning protocol", self.id_str.clone());
        //let mut phase1 = false;
        //let mut phase2 = false;
        let timeout_duration = Duration::from_millis(100);
        let mut txid= String::from("") ;
        let mut uid = 0;
        let binding = self.id_str.clone();
        let mut sid = binding.as_str();
        let mut opid = 0;
        let mut result : Result<ProtocolMessage, IpcError> ;
        let mut msg : message::ProtocolMessage ;
        // TODO
        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            match self.state{
                ParticipantState::Quiescent => {
                    result = self.rx.recv();
                    if result.is_ok(){
                        msg = result.unwrap();
                        if msg.mtype == message::MessageType::CoordinatorPropose{
                            //trace!("{} received proposal", self.id_str.clone());
                            
                            txid = msg.txid.clone();
                            uid = msg.uid;                            
                            opid = msg.opid;
                            self.state = ParticipantState::ReceivedP1;
                            self.request_status = RequestStatus::Unknown;
                            
                        }else if msg.mtype == message::MessageType::CoordinatorExit {
                            trace!("receive coordinator exit {}", self.id_str.to_owned());
                            break;
                        }
                    }else{
                        warn!("receive proposal fail in {}", self.id_str.to_owned());
                        break;
                    }
                }
                ParticipantState::ReceivedP1 =>{
                    msg = message::ProtocolMessage::instantiate(message::MessageType::CoordinatorPropose, 
                                                                                    uid,
                                                                                    txid.clone(), 
                                                                                    String::from(sid), 
                                                                                    opid);
                    let op = self.perform_operation(msg.clone());
                    
                    if !op {
                        //my_msg.mtype = message::MessageType::ParticipantVoteAbort;
                        self.state = ParticipantState::VotedAbort;
                        self.log.append(message::MessageType::ParticipantVoteAbort, txid.clone(), String::from(sid), opid);
                    }else{
                        self.state = ParticipantState::VotedP1Commit;
                    }                                                          
                    
                    
                }
                ParticipantState::VotedP1Commit =>{
                    let my_msg = message::ProtocolMessage::instantiate(message::MessageType::ParticipantVoteCommit, 
                                                                                        uid,
                                                                                        txid.clone(), 
                                                                                        String::from(sid), 
                                                                                        opid);
                    self.send(my_msg);
                    self.state = ParticipantState::SentP1Commit;
                }
                ParticipantState::SentP1Commit =>{
                    result = self.rx.recv();
                    if result.is_ok(){
                        msg = result.unwrap();
                        //phase 1 committed
                        if msg.mtype == message::MessageType::CoordinatorCommit{
                            //trace!("{} received commit, start phase 2", self.id_str.clone());
                            let op = self.perform_operation(msg.clone());
                            
                            if !op {
                                //my_msg.mtype = message::MessageType::ParticipantVoteAbort;
                                self.state = ParticipantState::VotedAbort;
                                self.log.append(message::MessageType::ParticipantVoteAbort, txid.clone(), String::from(sid), opid);
                            }else{
                                self.state = ParticipantState::VotedP2Commit;
                                self.log.append(message::MessageType::ParticipantVoteCommit, txid.clone(), String::from(sid), opid); 
                            }  
                            
                        }
                        //phase 1 aborted
                        else if msg.mtype == message::MessageType::CoordinatorAbort{
                            //trace!("{} received abort in phase 1", self.id_str.clone());
                            self.failed_ops += 1;
                            let mtype = message::MessageType::CoordinatorAbort;
                            self.log.append(mtype, txid.clone(), String::from(sid), opid);
                            self.request_status = RequestStatus::Aborted;
                            self.state = ParticipantState::Quiescent;
                            continue;
                        }
                        //unknown exit
                        else{
                            trace!("receive coordinator exit {}", self.id_str.to_owned());
                            self.unknown_ops += 1;
                            break;
                        }
                        
                    }else{
                        warn!("receive phase 2 fail in {}", self.id_str.to_owned());
                        self.unknown_ops += 1;
                        break;
                    }
                }
                ParticipantState::VotedP2Commit =>{
                    let my_msg = message::ProtocolMessage::instantiate(message::MessageType::ParticipantVoteCommit, 
                                                                                        uid,
                                                                                        txid.clone(), 
                                                                                        String::from(sid), 
                                                                                        opid);
                    self.send(my_msg);
                    self.state = ParticipantState::AwaitingGlobalDecision;
                }
                ParticipantState::AwaitingGlobalDecision =>{
                    result = self.rx.recv();
                    if result.is_ok(){
                        msg = result.unwrap();
                        //phase 2 committed
                        if msg.mtype == message::MessageType::CoordinatorCommit{
                            //trace!("{} received result commit", self.id_str.clone());
                            self.successful_ops += 1;
                            self.request_status = RequestStatus::Committed;
                        }
                        //phase 2 aborted
                        else if msg.mtype == message::MessageType::CoordinatorAbort{
                            //trace!("{} received result abort", self.id_str.clone());
                            self.failed_ops += 1; 
                            self.request_status = RequestStatus::Aborted;                   
                        }
                        //unknown exit 
                        else{
                            trace!("receive coordinator exit {}", self.id_str.to_owned());
                            self.unknown_ops += 1;
                            break;
                        }
                        self.log.append(msg.mtype, txid.clone(), String::from(sid), opid);
                    }else {
                        warn!("receive phase 2 fail in {}", self.id_str.to_owned());
                        self.unknown_ops += 1;
                        break;
                    }
                    self.state = ParticipantState::Quiescent;
                }
                ParticipantState::VotedAbort =>{
                    let my_msg = message::ProtocolMessage::instantiate(message::MessageType::ParticipantVoteAbort, 
                                                                                        uid,
                                                                                        txid.clone(), 
                                                                                        String::from(sid), 
                                                                                        opid);
                    self.send(my_msg);
                    self.state = ParticipantState::AwaitingGlobalDecision;
                }
            }
            //let result = self.rx.try_recv_timeout(timeout_duration);
            //phase 1: receive proposal from coordinator
            /*let result = self.rx.recv();
            if result.is_ok(){
                let msg = result.unwrap();
                if msg.mtype == message::MessageType::CoordinatorPropose{
                    trace!("{} received proposal", self.id_str.clone());
                    let op = self.perform_operation(msg.clone());
                    let mut my_msg = message::ProtocolMessage::generate(message::MessageType::ParticipantVoteCommit, 
                                                                                    msg.txid, 
                                                                                    msg.senderid, 
                                                                                    msg.opid);
                    if !op {
                        my_msg.mtype = message::MessageType::ParticipantVoteAbort;
                        
                        self.log.append(my_msg.mtype.clone(), my_msg.txid.clone(), my_msg.senderid.clone(), my_msg.opid);
                    }                                                          
                    self.send(my_msg.clone()) ;
                    //trace!("{} phase 1 sent", self.id_str);
                    
                }else if msg.mtype == message::MessageType::CoordinatorExit {
                    trace!("receive coordinator exit {}", self.id_str.to_owned());
                    break;
                }
            }else{
                warn!("receive proposal fail in {}", self.id_str.to_owned());
                break;
            }
            //phase 2: receive from coordinator
            let result = self.rx.recv();
            if result.is_ok(){
                let msg = result.unwrap();
                //phase 1 committed
                if msg.mtype == message::MessageType::CoordinatorCommit{
                    trace!("{} received commit, start phase 2", self.id_str.clone());
                    let op = self.perform_operation(msg.clone());
                    let mut my_msg = message::ProtocolMessage::generate(message::MessageType::ParticipantVoteCommit, 
                                                                                    msg.txid, 
                                                                                    msg.senderid, 
                                                                                    msg.opid);
                    if !op {
                        my_msg.mtype = message::MessageType::ParticipantVoteAbort;
                        //self.log.append(my_msg.mtype.clone(), my_msg.txid.clone(), my_msg.senderid.clone(), my_msg.opid); 
                    }
                                                                             
                    self.send(my_msg.clone()) ;
                    self.log.append(my_msg.mtype, my_msg.txid, my_msg.senderid, my_msg.opid);
                }
                //phase 1 aborted
                else if msg.mtype == message::MessageType::CoordinatorAbort{
                    trace!("{} received abort in phase 1", self.id_str.clone());
                    self.failed_ops += 1;
                    let mtype = message::MessageType::CoordinatorAbort;
                    self.log.append(mtype, msg.txid, msg.senderid, msg.opid);
                    continue;
                }
                //unknown exit
                else{
                    trace!("receive coordinator exit {}", self.id_str.to_owned());
                    self.unknown_ops += 1;
                    break;
                }
                
            }else{
                warn!("receive phase 2 fail in {}", self.id_str.to_owned());
                self.unknown_ops += 1;
                break;
            }
            //receive final result from coordinator
            let result = self.rx.recv();
            if result.is_ok(){
                let msg = result.unwrap();
                //phase 2 committed
                if msg.mtype == message::MessageType::CoordinatorCommit{
                    trace!("{} received result commit", self.id_str.clone());
                    self.successful_ops += 1;

                }
                //phase 2 aborted
                else if msg.mtype == message::MessageType::CoordinatorAbort{
                    trace!("{} received result abort", self.id_str.clone());
                    self.failed_ops += 1;                    
                }
                //unknown exit 
                else{
                    trace!("receive coordinator exit {}", self.id_str.to_owned());
                    self.unknown_ops += 1;
                    break;
                }
                self.log.append(msg.mtype, msg.txid, msg.senderid, msg.opid);
            }else {
                warn!("receive phase 2 fail in {}", self.id_str.to_owned());
                self.unknown_ops += 1;
                break;
            }*/

        }

        //self.wait_for_exit_signal();
        self.report_status();
    }
}
