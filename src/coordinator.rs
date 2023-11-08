//!
//! coordinator.rs
//! Implementation of 2PC coordinator
//!
extern crate log;
extern crate stderrlog;
extern crate rand;
extern crate ipc_channel;

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::Duration;
use std::time::Instant;

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::ipc_channel::ipc::channel;

use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

use crate::participant;

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
    SentGlobalDecision
}

/// Coordinator
/// Struct maintaining state for coordinator
#[derive(Debug)]
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
        r: &Arc<AtomicBool>) -> Coordinator {

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



    pub fn send_participants(&mut self, msg: message::ProtocolMessage) -> bool{
        for i in 0..self.vec_participant.len(){
            if !self.running.load(Ordering::SeqCst) {
                return false;
            }
            let(participant_id, child, participant_tx, participant_rx, pState) = &self.vec_participant[i];
            //trace!("sending proposal to {}", participant_id);
            //let msg = ProtocolMessage::generate(message::MessageType::CoordinatorPropose, String::from(txid), String::from(sid), opid);
            if *pState{
                //trace!("sent message to {}", participant_id);
                //trace!("sending message vec {:?}", self.vec_participant);
                participant_tx.send( msg.clone());
            }
            
        }
        return true;
    }

    pub fn receive_participants(&mut self, timeout_duration: Duration) ->message::RequestStatus{
        let mut status = message::RequestStatus::Unknown;
        let mut vote = true;
        let mut i: usize = 0;
        let mut participant_done = 0;
        let start_time = Instant::now();
        
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
            let(participant_id, child, participant_tx, participant_rx, pState) = &self.vec_participant[i];
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
                }
                Err(_) => {
                    //vote = false;
                    //warn!("participants {} timeout", i);
                    //self.vec_participant[i].4 = false;
                    //self.vec_participant_fail.push(i);
                    //trace!("adding vec fail {:?}, vec_par {:?}",self.vec_participant_fail, self.vec_participant);
                    //continue
                }
            }
            i = (i+1) % self.vec_participant.len();
        }
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

    pub fn paricipant_recover(&mut self, timeout_duration: Duration){
        let mut i = self.vec_participant_fail.len()-1;
        //warn!("i={}, vec {:?}",i, self.vec_participant_fail);
        let arc = self.log.arc();
        let map = arc.lock().unwrap();
        //let mut n_recover = self.vec_participant_fail.len();
        
        for n in 0..self.vec_participant_fail.len(){
            let p_id = self.vec_participant_fail[n];
            let(participant_id, child, participant_tx, participant_rx, pState) = &self.vec_participant[p_id];
            let re_msg = ProtocolMessage::instantiate(message::MessageType::ParticipantRecover, 0, String::from("txid"), String::from("sid"), 0);
            participant_tx.send(re_msg.clone());
        }
        let start_time = Instant::now();
        loop {
            if Instant::now().duration_since(start_time) >= timeout_duration{
                break;
            }
            let p_id = self.vec_participant_fail[i];
            let(participant_id, child, participant_tx, participant_rx, pState) = &self.vec_participant[p_id];

            let result = participant_rx.try_recv();
            match result {
                Ok(participant_pm) => {                            
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
                            
                            participant_tx.send(pm.clone());
                        }
                        let msg = ProtocolMessage::instantiate(message::MessageType::RecoveryDone, 0, String::from("txid"), String::from("sid"), 0);
                        participant_tx.send(msg);
                        self.vec_participant[p_id].4 = true;
                        self.vec_participant_fail.remove(i);
                        warn!("participant {} recovered", p_id);
                        
                        //info!("i={}, vec {:?}",i, self.vec_participant_fail);
                    }else{
                        //trace!("recover type wrong {}", p_id);
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
        //trace!("fail vec {:?}",self.vec_participant_fail);
    }

    pub fn coordinator_exit(&mut self){
        let msg = ProtocolMessage::generate(message::MessageType::CoordinatorExit, String::from("Coordinator_exit"), String::from("0"), 0);                
        self.send_participants(msg.clone());
        for i in 0..self.vec_client.len(){            
            let(_, _, client_tx, _) = &self.vec_client[i];
            
            client_tx.send( msg.clone());
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
        let timeout_duration = Duration::from_millis(200);
        let mut client_done = 0;
        let mut txid = "";
        let mut uid = 0;
        let mut sid = "";
        let mut opid = 0;
        let mut client_index = 0;
        let mut result : Result<ProtocolMessage, TryRecvError> ;
        let mut client_pm : message::ProtocolMessage ;
        
        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            match self.state{
                CoordinatorState::Quiescent =>{
                    //trace!("Quiescent");
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
                    
                    result = client_rx.try_recv_timeout(timeout_duration);
                    if result.is_err(){
                        self.vec_client_done[client_index] = true;
                        //trace!("client {} timeout", client_index.to_string());
                        client_index = (client_index + 1)%(self.vec_client.len());
                        continue
                    }
                    client_pm = result.unwrap();
                    txid = client_pm.txid.as_str();
                    sid = client_pm.senderid.as_str();
                    opid = client_pm.opid;
                    uid = client_pm.uid;
                    //trace!("client request received {}", &txid);
                    
                    self.request_status = RequestStatus::Unknown;
                    self.state = CoordinatorState::ReceivedRequest;
                }
                CoordinatorState::ReceivedRequest =>{
                    //trace!("ReceivedRequest");
                    let mut msg = ProtocolMessage::instantiate(message::MessageType::CoordinatorPropose, uid, String::from(txid), String::from(sid), opid);                
                    let result = self.send_participants(msg.clone());
                    if !result{
                        //self.unknown_ops += 1;
                        break;
                    }
                    self.state = CoordinatorState::ProposalSent;
                }
                CoordinatorState::ProposalSent =>{
                    //trace!("ProposalSent");
                    let result = self.receive_participants(timeout_duration);
                    if result == message::RequestStatus::Unknown {
                        //self.unknown_ops += 1;
                        break;
                    }
                    //participant abort in phase 1
                    else if result == message::RequestStatus::Aborted {                        
                        self.state = CoordinatorState::ReceivedVotesAbort;
                        self.request_status = RequestStatus::Aborted;
                        self.failed_ops += 1; 
                        continue;
                    }
                    self.state = CoordinatorState::ReceivedP1VotesCommit;
                    
                    
                }
                CoordinatorState::ReceivedP1VotesCommit =>{
                    //trace!("ReceivedP1VotesCommit");
                    let mut msg = ProtocolMessage::instantiate(message::MessageType::CoordinatorCommit, uid, String::from(txid), String::from(sid), opid);
                    let result = self.send_participants(msg.clone());
                    if !result{
                        //self.unknown_ops += 1;
                        break;
                    }
                    self.state = CoordinatorState::SentP1Commit;
                }
                CoordinatorState::SentP1Commit =>{
                    //trace!("SentP1Commit");
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
                    let mut msg = ProtocolMessage::instantiate(message::MessageType::CoordinatorCommit, uid, String::from(txid), String::from(sid), opid);
                    self.send_participants(msg.clone());
                    msg.mtype = message::MessageType::ClientResultCommit;
                    let client_tx = &self.vec_client[client_index].2;
                    client_tx.send(msg);
                    //trace!("{} commited", &txid);
                    self.state = CoordinatorState::SentGlobalDecision;
                }
                CoordinatorState::ReceivedVotesAbort => {
                    //trace!("ReceivedVotesAbort");
                    let mut msg = ProtocolMessage::instantiate(message::MessageType::CoordinatorAbort, uid, String::from(txid), String::from(sid), opid);                           
                    self.log.append(message::MessageType::CoordinatorAbort, String::from(txid), String::from(sid), opid);
                    self.log_index += 1;
                    self.send_participants(msg.clone());
                    msg.mtype = message::MessageType::ClientResultAbort;
                    let client_tx = &self.vec_client[client_index].2;
                    client_tx.send(msg);
                    //trace!("{} aborted", &txid);
                    self.state = CoordinatorState::SentGlobalDecision;
                }
                CoordinatorState::SentGlobalDecision =>{
                    //trace!("SentGlobalDecision");
                    client_index = (client_index + 1)%(self.vec_client.len());
                    
                    self.state = CoordinatorState::Quiescent;
                    
                    
                    //thread::sleep(Duration::from_millis(5));
                }
            }
            
            //receive request from client
            /*for n in 0..self.vec_client.len(){
                if self.vec_client_done[n]{
                    client_done += 1;
                    continue;
                }
                let client_rx = &self.vec_client[n].3;
                
                let result = client_rx.try_recv_timeout(timeout_duration);
                if result.is_err(){
                    self.vec_client_done[n] = true;
                    trace!("client {} timeout", n.to_string());
                    continue
                }
                
                let client_pm = result.unwrap();
                txid = client_pm.txid.as_str();
                sid = client_pm.senderid.as_str();
                opid = client_pm.opid;
                trace!("client request received {}", &txid);
                //self.request_status = RequestStatus::Unknown;   
                                                           
                //phase 1: send proprose to participants
                let mut msg = ProtocolMessage::generate(message::MessageType::CoordinatorPropose, String::from(txid), String::from(sid), opid);                
                let result = self.send_participants(msg.clone());
                if !result{
                    self.unknown_ops += 1;
                    break;
                }
                
                //phase 1: receive votes from participants
                let result = self.receive_participants(timeout_duration);
                if result == message::RequestStatus::Unknown {
                    self.unknown_ops += 1;
                    break;
                }
                //participant abort in phase 1
                else if result == message::RequestStatus::Aborted {
                    let mtype = message::MessageType::CoordinatorAbort;
                    self.failed_ops += 1;
                    msg.mtype = mtype;
                    self.log.append(mtype, String::from(txid), String::from(sid), opid);
                    self.send_participants(msg.clone());
                    msg.mtype = message::MessageType::ClientResultAbort;
                    let client_tx = &self.vec_client[n].2;
                    client_tx.send(msg).unwrap();
                    trace!("{} aborted", &txid);
                    continue;
                }
                
                //if all participants voted commit in phase 1,
                //phase 2: send Coordinator commit to participants
                trace!("all participants commit in phase 1 for {}",&txid);
                msg.mtype = message::MessageType::CoordinatorCommit;
                let result = self.send_participants(msg.clone());
                if !result{
                    self.unknown_ops += 1;
                    break;
                }

                //phase 2: receive votes from participants
                let result = self.receive_participants(timeout_duration);
                if result == message::RequestStatus::Unknown {
                    self.unknown_ops += 1;
                    break;
                }else if result == message::RequestStatus::Aborted {
                    let mtype = message::MessageType::CoordinatorAbort;
                    self.failed_ops += 1;
                    msg.mtype = mtype;
                    self.log.append(mtype, String::from(txid), String::from(sid), opid);
                    self.send_participants(msg.clone());
                    msg.mtype = message::MessageType::ClientResultAbort;
                    let client_tx = &self.vec_client[n].2;
                    client_tx.send(msg).unwrap();
                    trace!("{} aborted", &txid);
                    continue;
                }
                //all participants vote commit in phase 2, return commit to participants and client
                let mtype = message::MessageType::CoordinatorCommit;
                self.successful_ops += 1;
                msg.mtype = mtype;
                self.log.append(mtype, String::from(txid), String::from(sid), opid);
                self.send_participants(msg.clone());
                msg.mtype = message::MessageType::ClientResultCommit;
                let client_tx = &self.vec_client[n].2;
                client_tx.send(msg).unwrap();
                trace!("{} commited", &txid);
            }
            if client_done == self.vec_client.len(){
                break;
            }*/
        }
        self.coordinator_exit();         
        self.report_status();
    }
}
