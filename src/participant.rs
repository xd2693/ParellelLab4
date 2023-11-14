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
    Fail,
}

///
/// Participant
/// Structure for maintaining per-participant state and communication/synchronization objects to/from coordinator
///
//#[derive(Debug)]
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
    log_index : u32,
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
            log_index: 0,
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
    pub fn send(&mut self, pm: ProtocolMessage) -> bool{
        let x: f64 = random();
        //trace!("random number is {}", x.to_string());
        if x <= self.send_success_prob  {
            // TODO: Send success
            let result = self.tx.send(pm);
            if result.is_err(){
                warn!("send err {}", self.id_str);
            }
            return true;
        } else {
            // TODO: Send fail
            // Sleep for 20 milliseconds to simulator participant fail
            thread::sleep(Duration::from_millis(20));
            return false;
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
    pub fn perform_operation(&mut self, _: ProtocolMessage) -> bool {

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

    /// receive message from coordinator, use try_recv and sleep instead of block
    pub fn receive_message(&mut self, sleep_duration: Duration) -> ProtocolMessage{
        loop{
            if !self.running.load(Ordering::SeqCst) {
                let msg = message::ProtocolMessage::instantiate(message::MessageType::CoordinatorExit, 
                    0,
                    String::from(""), 
                    String::from("sid"), 
                    0);
                return msg;
            }
            let result = self.rx.try_recv();
            if result.is_ok(){
                let msg = result.unwrap();
                return msg;
            }else{
                thread::sleep(sleep_duration);
            }
        }
    }

    /// participant fail recovery
    pub fn recovery_protocol(&mut self, sleep_duration: Duration)->bool{
        
        //get the last line of log before crashing
        let mut last_log = self.log.read(&self.log_index);
        trace!("last_log {}", last_log.txid.clone());

        trace!("participant {}, query global dicision {}", self.id_str.clone(), last_log.txid.clone());
        let mut pm = last_log.clone();
        pm.mtype = MessageType::ParticipantRecover;
        //wait for coordinator to finish current transaction and send message to allow recover
        loop{        
            let result = self.rx.try_recv();
            match result {
                Ok(re) => {
                    if re.mtype != MessageType::ParticipantRecover{
                        if re.mtype == MessageType::CoordinatorExit{
                            self.unknown_ops += 1;
                        }else if re.mtype == MessageType::CoordinatorFail{
                            continue;
                        }
                        else{
                            trace!("recover message type wrong {}", self.id_str.clone());
                        }
                        return false;
                    }
                    
                    break;
                }
                Err(_) =>{
                    thread::sleep(sleep_duration);
                }
            }            
        }
        //send last record in the local log to coordinator
        //receive any missing logs from the coordinator and append to local log
        //wait for the RecoveryDone message to return to normal protocol
        let _ = self.tx.send(pm.clone());
        loop{
            let result = self.rx.try_recv();
            if result.is_ok(){        
                let msg = result.unwrap();
                if last_log.mtype == MessageType::CoordinatorCommit || last_log.mtype == MessageType::CoordinatorAbort{
                    last_log.mtype = MessageType::ParticipantRecover;
                    continue;
                }
                if msg.mtype == MessageType::CoordinatorCommit{
                    trace!("{} received result commit", self.id_str.clone());
                    
                    self.successful_ops += 1;
                    self.request_status = RequestStatus::Committed;
                }
                
                else if msg.mtype == MessageType::CoordinatorAbort{
                    trace!("{} received result abort", self.id_str.clone());
                    self.failed_ops += 1; 
                    self.request_status = RequestStatus::Aborted;                   
                }
                else if msg.mtype == MessageType::RecoveryDone{
                    //warn!("in participant, I recovered {} ", self.id_str.clone());
                    break;
                }
                //unknown exit 
                else if msg.mtype == MessageType::CoordinatorExit{
                    trace!("receive coordinator exit {}", self.id_str.to_owned());
                    self.unknown_ops += 1;
                    break;
                }
                else{
                    continue;
                }
                self.log.append(msg.mtype, msg.txid.clone(), String::from(self.id_str.clone()), msg.opid);
                self.log_index += 1;
            }else{
                thread::sleep(sleep_duration);
            }
        }
       
        return true;
                
    }

    /// coordinator fail recovery protocol
    /// When coordinator asks to do p2 vote again, check the log.
    /// If already voted, resend the vote.
    /// If not voted, do operation and sent vote.
    /// When receive global decision from the coordinator, check the log.
    /// If the decision already logged, return to Quiescent.
    /// If not logged, log the decision.
    pub fn coordinator_fail(&mut self, sleep_duration: Duration){
            
        let msg = self.receive_message(sleep_duration);
        let mut last_log = ProtocolMessage::instantiate(msg.mtype.clone(), msg.uid, String::from(""), msg.senderid.clone(), msg.opid);
        
        //warn!("in {}, coordinator fail detected {}", self.id_str, msg.txid.clone().as_str());
        if self.log_index > 0 {
            last_log = self.log.read(&self.log_index);
        }
        
        let mtype = MessageType::CoordinatorAbort;
        if msg.mtype == MessageType::CoordinatorExit{
            trace!("receive coordinator exit {}", self.id_str.to_owned());
            self.unknown_ops += 1;
            
        }
            
        if msg.mtype == MessageType::RecoveryDone{
            self.state = ParticipantState::Quiescent;
            
        }
        else if msg.mtype == MessageType::ParticipantReady{
            if last_log.mtype == MessageType::ParticipantVoteCommit || last_log.mtype == MessageType::ParticipantVoteAbort{
                let _ = self.tx.send(last_log);
                self.state = ParticipantState::AwaitingGlobalDecision;
            }else if last_log.mtype == MessageType::ParticipantReady {
                let op = self.perform_operation(msg.clone());                        
                if !op {
                    //my_msg.mtype = MessageType::ParticipantVoteAbort;
                    self.state = ParticipantState::VotedAbort;
                    self.log.append(MessageType::ParticipantVoteAbort, msg.txid.clone(), last_log.senderid.clone(), msg.opid);
                    self.log_index += 1;
                }else{
                    self.state = ParticipantState::VotedP2Commit;
                    self.log.append(MessageType::ParticipantVoteCommit, msg.txid.clone(), last_log.senderid.clone(), msg.opid); 
                    self.log_index += 1;
                }
            }
        }
        else {
            if last_log.txid == msg.txid && last_log.mtype == msg.mtype{
                self.state = ParticipantState::Quiescent;
                
            }else{
                self.log.append(msg.mtype, msg.txid, self.id_str.clone(), msg.opid);
                self.log_index += 1;
                if msg.mtype == MessageType::CoordinatorAbort{
                    self.failed_ops += 1;
                }else if msg.mtype == MessageType::CoordinatorCommit{
                    self.successful_ops += 1;
                }
                self.state = ParticipantState::Quiescent;
                
            }
        }
        //warn!("coordinator recovered in participant {}", self.id_str);
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats


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
            //let result = self.rx.recv();

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

        let sleep_duration = Duration::from_millis(2);
        let op_sleep_duration = Duration::from_millis(10);
        let mut txid= String::from("") ;
        let mut uid = 0;
        let binding = self.id_str.clone();
        let sid = binding.as_str();
        let mut opid = 0;
        let mut msg : ProtocolMessage ;
        // TODO
        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            match self.state{
                ParticipantState::Fail=>{
                    if self.recovery_protocol(sleep_duration){
                        self.state = ParticipantState::Quiescent;
                    }
                    
                }
                ParticipantState::Quiescent => {
                    msg = self.receive_message(sleep_duration);
                    if msg.mtype == MessageType::CoordinatorPropose{
                        //trace!("{} received proposal", self.id_str.clone());
                        
                        txid = msg.txid.clone();
                        uid = msg.uid;                            
                        opid = msg.opid;
                        self.state = ParticipantState::ReceivedP1;
                        self.request_status = RequestStatus::Unknown;
                        
                    }else if msg.mtype == MessageType::CoordinatorExit {
                        trace!("receive coordinator exit {}", self.id_str.to_owned());
                        break;
                    }else if msg.mtype == MessageType::CoordinatorFail{
                        self.coordinator_fail(sleep_duration);
                    }
                    
                    
                }
                ParticipantState::ReceivedP1 =>{
                    msg = ProtocolMessage::instantiate(MessageType::CoordinatorPropose, 
                                                      uid,
                                                    txid.clone(), 
                                                    String::from(sid), 
                                                    opid);
                    let op = self.perform_operation(msg.clone());
                    
                    if !op {
                        //my_msg.mtype = MessageType::ParticipantVoteAbort;
                        self.state = ParticipantState::VotedAbort;
                        self.log.append(MessageType::ParticipantVoteAbort, txid.clone(), String::from(sid), opid);
                        self.log_index += 1;
                    }else{
                        self.state = ParticipantState::VotedP1Commit;
                        self.log.append(MessageType::ParticipantReady, txid.clone(), String::from(sid), opid);
                        self.log_index += 1;
                    }                                                          
                    
                    
                }
                ParticipantState::VotedP1Commit =>{
                    let my_msg = message::ProtocolMessage::instantiate(MessageType::ParticipantVoteCommit, 
                                                                                        uid,
                                                                                        txid.clone(), 
                                                                                        String::from(sid), 
                                                                                        opid);
                    if self.send(my_msg){
                        self.state = ParticipantState::SentP1Commit;
                    }else{
                        self.state = ParticipantState::Fail;
                    }
                    thread::sleep(op_sleep_duration);
                }
                ParticipantState::SentP1Commit =>{
                    msg = self.receive_message(sleep_duration);
                    //phase 1 committed
                    if msg.mtype == MessageType::CoordinatorCommit{
                        let op = self.perform_operation(msg.clone());
                        
                        if !op {
                            self.state = ParticipantState::VotedAbort;
                            self.log.append(MessageType::ParticipantVoteAbort, txid.clone(), String::from(sid), opid);
                            self.log_index += 1;
                        }else{
                            self.state = ParticipantState::VotedP2Commit;
                            self.log.append(MessageType::ParticipantVoteCommit, txid.clone(), String::from(sid), opid); 
                            self.log_index += 1;
                        }                      
                    }
                    //phase 1 aborted
                    else if msg.mtype == MessageType::CoordinatorAbort{
                        self.failed_ops += 1;
                        let mtype = MessageType::CoordinatorAbort;
                        self.log.append(mtype, txid.clone(), String::from(sid), opid);
                        self.log_index += 1;
                        self.request_status = RequestStatus::Aborted;
                        self.state = ParticipantState::Quiescent;
                        continue;
                    }
                    //unknown exit
                    else if msg.mtype == MessageType::CoordinatorExit{
                        trace!("receive coordinator exit {}", self.id_str.to_owned());
                        self.unknown_ops += 1;
                        break;
                    }
                    else if msg.mtype == MessageType::CoordinatorFail{
                        self.coordinator_fail(sleep_duration);
                    }
                    
                    
                }
                ParticipantState::VotedP2Commit =>{
                    let my_msg = message::ProtocolMessage::instantiate(MessageType::ParticipantVoteCommit, 
                                                                                        uid,
                                                                                        txid.clone(), 
                                                                                        String::from(sid), 
                                                                                        opid);
                    if self.send(my_msg){
                        self.state = ParticipantState::AwaitingGlobalDecision;
                    }else{
                        self.state = ParticipantState::Fail;
                    }
                    thread::sleep(op_sleep_duration);
                    
                }
                ParticipantState::AwaitingGlobalDecision =>{
                    msg = self.receive_message(sleep_duration);
                    //phase 2 committed
                    if msg.mtype == MessageType::CoordinatorCommit{
                        self.successful_ops += 1;
                        self.request_status = RequestStatus::Committed;
                    }
                    //phase 2 aborted
                    else if msg.mtype == MessageType::CoordinatorAbort{
                        self.failed_ops += 1; 
                        self.request_status = RequestStatus::Aborted;                   
                    }
                    //unknown exit 
                    else if msg.mtype == MessageType::CoordinatorExit{
                        trace!("receive coordinator exit {}", self.id_str.to_owned());
                        self.unknown_ops += 1;
                        break;
                    }
                    else if msg.mtype == MessageType::CoordinatorFail{
                        self.coordinator_fail(sleep_duration);
                        continue;
                    }
                    self.log.append(msg.mtype, txid.clone(), String::from(sid), opid);
                    self.log_index += 1;
                    self.state = ParticipantState::Quiescent;
                    
                }
                ParticipantState::VotedAbort =>{
                    let my_msg = message::ProtocolMessage::instantiate(MessageType::ParticipantVoteAbort, 
                                                                                        uid,
                                                                                        txid.clone(), 
                                                                                        String::from(sid), 
                                                                                        opid);
                    if self.send(my_msg){
                        self.state = ParticipantState::AwaitingGlobalDecision;
                    }else{
                        self.state = ParticipantState::Fail;
                    }
                    thread::sleep(op_sleep_duration);
                }
            }
    

        }

        
        self.report_status();
    }
}
