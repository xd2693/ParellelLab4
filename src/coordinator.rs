//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;


use std::collections::HashMap;
use std::convert::TryInto;
//use std::str::FromStr;
use std::sync::Arc;
//use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use std::time::Instant;

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
//use coordinator::ipc_channel::ipc::channel;
//use coordinator::rand::prelude::*;

use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

//use crate::participant;

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedP1VotesCommit,
    SentP1Commit,
    ReceivedP2VotesCommit,
    SentGlobalDecision,
    CoordinatorFail
}

/// Coordinator
/// Struct maintaining state for coordinator
//#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    running: Arc<AtomicBool>,
    log: oplog::OpLog,
    vec_client: Vec<(String, std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
    vec_participant: Vec<(String, std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>, bool)>,
    vec_client_done: Vec<bool>,
    request_status : RequestStatus,
    successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,
    log_index:u32,
    vec_participant_done: Vec<bool>,
    vec_participant_fail: Vec<usize>,
    //heartbeat_tx: Sender<(RequestStatus, String, String, u32)>,
    fail_stage: u32,
}

///
/// Coordinator
/// Implementation of coordinator functionality
/// Required:
/// 1. new -- Constructor
/// 2. protocol -- Implementation of coordinator side of protocol
/// 3. report_status -- Report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- What to do when a participant joins
/// 5. client_join -- What to do when a client joins
///
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    ///
    /// <params>
    ///     log_path: directory for log files --> create a new log there.
    ///     r: atomic bool --> still running?
    ///
    pub fn new(
        log_path: String,
        r: &Arc<AtomicBool>,
        fail_stage: u32) -> Coordinator {

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(log_path),
            running: r.clone(),
            // TODO
            vec_client: Vec::new(),
            vec_participant: Vec::new(),
            vec_client_done: Vec::new(),
            request_status: RequestStatus::Committed,
            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,
            log_index: 0,
            vec_participant_done: Vec::new(),
            vec_participant_fail: Vec::new(),
            fail_stage: fail_stage,
        }
    }

    ///
    /// participant_join()
    /// Adds a new participant for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn participant_join(&mut self, name: &String, participant: (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        let (child, sender, receiver) = participant;
        self.vec_participant.push((name.to_owned(), child, sender, receiver, true));
        self.vec_participant_done.push(false);
        //if let Some(last) = self.vec_participant.last(){
        //    trace!("participant joined {}",last.0);
        //}else{
        //    trace!("empty vec");
        //}
    }

    ///
    /// client_join()
    /// Adds a new client for the coordinator to keep track of
    ///
    /// HINT: Keep track of any channels involved!
    /// HINT: You may need to change the signature of this function
    ///
    pub fn client_join(&mut self, name: &String, client: (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)) {
        assert!(self.state == CoordinatorState::Quiescent);

        // TODO
        let (child, sender, receiver) = client;
        self.vec_client.push((name.to_owned(), child, sender, receiver));
        self.vec_client_done.push(false);
        //if let Some(last) = self.vec_client.last(){
        //    trace!("client joined {}",last.0);
        //}else{
        //    trace!("empty vec");
        //}
        
    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this coordinator before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        //let successful_ops: u64 = 0;
        //let failed_ops: u64 = 0;
        //let unknown_ops: u64 = 0;
        if self.request_status == RequestStatus::Unknown{
            self.unknown_ops += 1;
        }
        println!("coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.successful_ops, self.failed_ops, self.unknown_ops);
    }

    /// send ProtocolMessage to all working participants 
    pub fn send_participants(&mut self, msg: message::ProtocolMessage) -> bool{
        for i in 0..self.vec_participant.len(){
            if !self.running.load(Ordering::SeqCst) {
                return false;
            }
            let(_, _, participant_tx, _, pState) = &self.vec_participant[i];
            if *pState{
                let _ = participant_tx.send( msg.clone());
            }
            
        }
        return true;
    }

    /// receive ProtocolMessage from all participants and return participants vote result
    /// if any participant alreay fail, return abort result to current transaction
    /// if any participant timeout, return abort result to current transaction and mark that participant as fail
    /// coordinator will not send or receive from fail participants until they recovered
    pub fn receive_participants(&mut self, timeout_duration: Duration) ->message::RequestStatus{
        let mut status = message::RequestStatus::Unknown;
        let mut vote = true;
        let mut i: usize = 0;
        let mut participant_done = 0;
        let mut start_time = Instant::now();
        
        loop{
            if !self.running.load(Ordering::SeqCst) {
                return status;
            }
            if Instant::now().duration_since(start_time) >= timeout_duration{
                break;
            }
            if participant_done == self.vec_participant.len(){
                break;
            }
            if self.vec_participant_done[i]{
                i = (i+1) % self.vec_participant.len();
                continue;
            }
            let(_, _, _, participant_rx, pState) = &self.vec_participant[i];
            if !pState{
                vote = false;
                participant_done += 1;
                i = (i+1) % self.vec_participant.len();
                continue;
            }
            let result = participant_rx.try_recv();
            match result {
                Ok(participant_pm) => {                            
                    if participant_pm.mtype == message::MessageType::ParticipantVoteAbort{
                        vote = false;
                    }
                    self.vec_participant_done[i] = true;
                    participant_done += 1;
                    start_time = Instant::now();
                }
                Err(_) => {
                    
                }
            }
            i = (i+1) % self.vec_participant.len();
        }
        //mark timeout participants and put them in a vec
        for n in 0..self.vec_participant.len(){
            if self.vec_participant_done[n]{
                self.vec_participant_done[n] = false;
            }else{
                if self.vec_participant[n].4{
                    warn!("participant {} failed", n);
                    self.vec_participant[n].4 = false;
                    self.vec_participant_fail.push(n);
                    vote = false;
                }
                
            }
        }
        if vote{
            status = message::RequestStatus::Committed;
        }else{
            status = message::RequestStatus::Aborted;
        }
        return status;
    }

    /// send recover message to failed participants in the vec
    /// if participant is ready for recover, send all coordinator global
    /// decisions that participant missed when failing
    pub fn paricipant_recover(&mut self, timeout_duration: Duration){
        let mut i = self.vec_participant_fail.len()-1;
        //warn!("i={}, vec {:?}",i, self.vec_participant_fail);
        let arc = self.log.arc();
        let map = arc.lock().unwrap();
        //let mut n_recover = self.vec_participant_fail.len();
        
        for n in 0..self.vec_participant_fail.len(){
            let p_id = self.vec_participant_fail[n];
            let(_, _, participant_tx, _, pState) = &self.vec_participant[p_id];
            let re_msg = ProtocolMessage::instantiate(MessageType::ParticipantRecover, 0, String::from("txid"), String::from("sid"), 0);
            let _ = participant_tx.send(re_msg.clone());
        }
        let mut start_time = Instant::now();
        loop {
            if Instant::now().duration_since(start_time) >= timeout_duration{
                break;
            }
            let p_id = self.vec_participant_fail[i];
            let(_, _, participant_tx, participant_rx, pState) = &self.vec_participant[p_id];

            let result = participant_rx.try_recv();
            match result {
                Ok(participant_pm) => {    
                    start_time = Instant::now();                        
                    if participant_pm.mtype == message::MessageType::ParticipantRecover{
                        trace!("recovering {}", p_id);
                        let txid = participant_pm.txid;
                        
                        let mut uid = 0;
                        for (u, pm) in map.iter(){
                            if pm.txid == txid {
                                uid = u.clone();
                                trace!("uid={}",uid);
                                break;
                            }
                        }
                        let log: HashMap<u32, message::ProtocolMessage> =
                                                    map.iter()
                                                    .filter(|e| (*e.0)>= uid)
                                                    .map(|(k,v)| (k.clone(), v.clone()))
                                                    .collect();
                        for (_, pm) in log.iter(){
                            if pm.mtype == message::MessageType::CoordinatorAbort{
                                //trace!("CoordinatorAbort {}", pm.txid.clone());
                            }
                            
                            let _ = participant_tx.send(pm.clone());
                        }
                        let msg = ProtocolMessage::instantiate(message::MessageType::RecoveryDone, 0, String::from("txid"), String::from("sid"), 0);
                        let _ = participant_tx.send(msg);
                        self.vec_participant[p_id].4 = true;
                        self.vec_participant_fail.remove(i);
                        
                    }
                        
                }
                Err(_) => {
                    trace!("participant {} recover timeout", p_id);
                }
            }
            if i == 0{
                if self.vec_participant_fail.len() != 0{
                    i = self.vec_participant_fail.len();
                }else{
                    break;
                }
                
            }

            i -= 1;
        }
        
    }

    /// coordinator recover from fail
    /// read last log from coordinator log
    /// If last log does not contain any message regarding to the current transaction, abort the transaction
    /// If last log contains a participant ready message, send request to participants again, participants do p2 vote again.
    /// If last log contains a Coordinator global decision, resend the decision.
    pub fn coordinator_recover(&mut self, txid: String, uid: u32, sid: String, opid: u32, client_index: usize){
        //warn!("in coordinator txid {}", txid.clone().as_str());
        for n in 0..self.vec_participant.len(){
            let (_, _, _,participant_rx,_) = &self.vec_participant[n];
            let _ = participant_rx.try_recv();
        }

        let msg = ProtocolMessage::instantiate(message::MessageType::CoordinatorFail, uid, txid.clone(), sid.clone(), opid);
        self.send_participants(msg.clone());
        
        //coordinator failed before doing anything, just return to Quiescent
        if uid == 0{
            let pm = message::ProtocolMessage::instantiate(MessageType::RecoveryDone, uid, txid.clone(), sid.clone(), opid);
            self.send_participants(pm.clone());
            self.state = CoordinatorState::Quiescent;
            return;
        }
        let mut last_log = message::ProtocolMessage::instantiate(MessageType::RecoveryDone, uid, String::from(""), sid.clone(), opid);
        if self.log_index > 0{
            last_log = self.log.read(&self.log_index);
        }

        if last_log.txid != txid {
            let mut pm = message::ProtocolMessage::instantiate(MessageType::CoordinatorAbort, uid, txid.clone(), sid.clone(), opid);
        
            self.log.append(MessageType::CoordinatorAbort, txid.clone(), sid.clone(), opid);
            self.log_index += 1;
            self.failed_ops += 1;
          
            self.send_participants(pm.clone());
            pm.mtype = MessageType::ClientResultAbort;
            let (_,_,client_tx,_) = &self.vec_client[client_index];
            let _ = client_tx.send(pm.clone());
            self.state = CoordinatorState::SentGlobalDecision;
        }else{
            self.send_participants(last_log.clone());
            if last_log.mtype == MessageType::CoordinatorCommit{
                last_log.mtype = MessageType::ClientResultCommit;
                let (_,_,client_tx,_) = &self.vec_client[client_index];
                
                if last_log.senderid == self.vec_client[client_index].0 {
                    let _ = client_tx.send(last_log.clone());
                    self.state = CoordinatorState::SentGlobalDecision;
                }else{
                    self.state = CoordinatorState::Quiescent;
                }
                
            }else if last_log.mtype == MessageType::CoordinatorAbort{
                last_log.mtype = MessageType::ClientResultAbort;
                let (_,_,client_tx,_) = &self.vec_client[client_index];
                
                if last_log.senderid == self.vec_client[client_index].0 {
                    let _ = client_tx.send(last_log.clone());
                    self.state = CoordinatorState::SentGlobalDecision;
                }else{
                    self.state = CoordinatorState::Quiescent;
                }
            }else if last_log.mtype == MessageType::ParticipantReady{
                self.send_participants(last_log.clone());
                self.state = CoordinatorState::SentP1Commit;
            }
        }
        
        
    }

    pub fn coordinator_exit(&mut self){
        warn!("coordinator exiting!");
        let msg = ProtocolMessage::generate(message::MessageType::CoordinatorExit, String::from("Coordinator_exit"), String::from("0"), 0);                
        self.send_participants(msg.clone());
        for i in 0..self.vec_client.len(){            
            let(_, _, client_tx, _) = &self.vec_client[i];
            
            let _= client_tx.send( msg.clone());
        }
    }

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: If the simulation ends early, don't keep handling requests!
    /// HINT: Wait for some kind of exit signal before returning from the protocol!
    ///
    pub fn protocol(&mut self) {

        // TODO
        let timeout_duration = Duration::from_millis(self.vec_participant.len()*2 as u64);
        let client_timeout = Duration::from_millis((self.vec_client*self.vec_participant+50) as u64);
        let mut client_done = 0;
        let mut txid = "";
        let mut uid = 0;
        let mut sid = "";
        let mut opid = 0;
        let mut client_index = 0;
        let mut result : Result<ProtocolMessage, TryRecvError> ;
        let mut client_pm : message::ProtocolMessage ;
        let mut fail = true;
        
        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            
            match self.state{
                CoordinatorState::CoordinatorFail =>{
                    thread::sleep(Duration::from_millis(10));
                    self.coordinator_recover(txid.to_string(), uid, sid.to_string(), opid, client_index);
                    
                }
                CoordinatorState::Quiescent =>{
                    //trace!("Quiescent");
                    if self.fail_stage == 1 && fail{
                        warn!("Coordinator fail detected in stage {:?}", &self.state);  
                        self.state = CoordinatorState::CoordinatorFail;
                        fail = false;            
                        continue;
                    }
                    if self.vec_participant_fail.len() > 0{
                        self.paricipant_recover(timeout_duration);
                    }
                    if client_done == self.vec_client.len(){
                        break;
                    }
                    if self.vec_client_done[client_index]{
                        client_done += 1;
                        continue;
                    }
                    let client_rx = &self.vec_client[client_index].3;
                    
                    result = client_rx.try_recv_timeout(client_timeout);
                    if result.is_err(){
                        self.vec_client_done[client_index] = true;
                        //trace!("client {} timeout", client_index.to_string());
                        client_index = (client_index + 1)%(self.vec_client.len());
                        
                    }else{
                        client_pm = result.unwrap();
                        txid = client_pm.txid.as_str();
                        sid = client_pm.senderid.as_str();
                        opid = client_pm.opid;
                        uid = client_pm.uid;
                        //trace!("client request received {}", &txid);
                        
                        self.request_status = RequestStatus::Unknown;
                        self.state = CoordinatorState::ReceivedRequest;
                        
                    }
                    
                    
                }
                CoordinatorState::ReceivedRequest =>{
                    //trace!("ReceivedRequest");
                    if self.fail_stage == 2 && fail{
                        warn!("Coordinator fail detected in stage {:?}", &self.state);  
                        self.state = CoordinatorState::CoordinatorFail;
                        fail = false;            
                        continue;
                    }
                    let msg = ProtocolMessage::instantiate(message::MessageType::CoordinatorPropose, uid, String::from(txid), String::from(sid), opid);                
                    let result = self.send_participants(msg.clone());
                    if !result{
                        //self.unknown_ops += 1;
                        break;
                    }
                    self.state = CoordinatorState::ProposalSent;
                    
                    
                }
                CoordinatorState::ProposalSent =>{
                    //trace!("ProposalSent");
                    if self.fail_stage == 3 && fail{
                        warn!("Coordinator fail detected in stage {:?}", &self.state);  
                        self.state = CoordinatorState::CoordinatorFail;
                        fail = false;            
                        continue;
                    }
                    let result = self.receive_participants(timeout_duration);
                    /*if result == message::RequestStatus::Unknown {
                        //self.unknown_ops += 1;
                        break;
                    }*/
                    //participant abort in phase 1
                    if result == message::RequestStatus::Aborted {                        
                        self.state = CoordinatorState::ReceivedVotesAbort;
                        self.request_status = RequestStatus::Aborted;
                        self.failed_ops += 1; 
                        continue;
                    }
                    self.state = CoordinatorState::ReceivedP1VotesCommit;
                    
                    
                }
                CoordinatorState::ReceivedP1VotesCommit =>{
                    //trace!("ReceivedP1VotesCommit");
                    if self.fail_stage == 4 && fail{
                        warn!("Coordinator fail detected in stage {:?}", &self.state);  
                        self.state = CoordinatorState::CoordinatorFail;
                        fail = false;            
                        continue;
                    }
                    let msg = ProtocolMessage::instantiate(message::MessageType::CoordinatorCommit, uid, String::from(txid), String::from(sid), opid);
                    let result = self.send_participants(msg.clone());
                    if !result{
                        //self.unknown_ops += 1;
                        break;
                    }
                    self.log.append(message::MessageType::ParticipantReady, String::from(txid), String::from(sid), opid);
                    self.log_index += 1;
                    self.state = CoordinatorState::SentP1Commit;
                    
                    
                }
                CoordinatorState::SentP1Commit =>{
                    //trace!("SentP1Commit");
                    if self.fail_stage == 5 && fail{
                        warn!("Coordinator fail detected in stage {:?}", &self.state);  
                        self.state = CoordinatorState::CoordinatorFail;
                        fail = false;            
                        continue;
                    }
                    let result = self.receive_participants(timeout_duration);
                    if result == message::RequestStatus::Unknown {
                        //self.unknown_ops += 1;
                        break;
                    }else if result == message::RequestStatus::Aborted {
                        self.state = CoordinatorState::ReceivedVotesAbort;
                        self.request_status = RequestStatus::Aborted;
                        self.failed_ops += 1; 
                        continue;
                    }
                    self.state = CoordinatorState::ReceivedP2VotesCommit;                    
                    self.successful_ops += 1;                
                    self.log.append(message::MessageType::CoordinatorCommit, String::from(txid), String::from(sid), opid);
                    self.log_index += 1;
                    self.request_status = RequestStatus::Committed;
                    
                }
                CoordinatorState::ReceivedP2VotesCommit =>{
                    //trace!("ReceivedP2VotesCommit");
                    if self.fail_stage == 6 && fail{
                        warn!("Coordinator fail detected in stage {:?}", &self.state);  
                        self.state = CoordinatorState::CoordinatorFail;
                        fail = false;            
                        continue;
                    }
                    let mut msg = ProtocolMessage::instantiate(message::MessageType::CoordinatorCommit, uid, String::from(txid), String::from(sid), opid);
                    self.send_participants(msg.clone());
                    msg.mtype = message::MessageType::ClientResultCommit;
                    let client_tx = &self.vec_client[client_index].2;
                    let _ = client_tx.send(msg);
                    //trace!("{} commited", &txid);
                    self.state = CoordinatorState::SentGlobalDecision;
                    
                }
                CoordinatorState::ReceivedVotesAbort => {
                    //trace!("ReceivedVotesAbort");
                    if self.fail_stage == 7 && fail{
                        warn!("Coordinator fail detected in stage {:?}", &self.state);  
                        self.state = CoordinatorState::CoordinatorFail;
                        fail = false;            
                        continue;
                    }
                    let mut msg = ProtocolMessage::instantiate(message::MessageType::CoordinatorAbort, uid, String::from(txid), String::from(sid), opid);                           
                    self.log.append(message::MessageType::CoordinatorAbort, String::from(txid), String::from(sid), opid);
                    self.log_index += 1;
                    self.send_participants(msg.clone());
                    msg.mtype = message::MessageType::ClientResultAbort;
                    let client_tx = &self.vec_client[client_index].2;
                    let _ = client_tx.send(msg);
                    //trace!("{} aborted", &txid);
                    self.state = CoordinatorState::SentGlobalDecision;
                    
                }
                CoordinatorState::SentGlobalDecision =>{
                    //trace!("SentGlobalDecision");
                    if self.fail_stage == 8 && fail{
                        warn!("Coordinator fail detected in stage {:?}", &self.state);  
                        self.state = CoordinatorState::CoordinatorFail;
                        fail = false;            
                        continue;
                    }
                    
                    client_index = (client_index + 1)%(self.vec_client.len());
                    
                    self.state = CoordinatorState::Quiescent;
                                        
                }
            }
            
            
        }
        self.coordinator_exit();         
        self.report_status();
    }
}
