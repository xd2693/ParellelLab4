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

use coordinator::ipc_channel::ipc::IpcSender as Sender;
use coordinator::ipc_channel::ipc::IpcReceiver as Receiver;
use coordinator::ipc_channel::ipc::TryRecvError;
use coordinator::ipc_channel::ipc::channel;

use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use oplog;

/// CoordinatorState
/// States for 2PC state machine
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {
    Quiescent,
    ReceivedRequest,
    ProposalSent,
    ReceivedVotesAbort,
    ReceivedVotesCommit,
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
    vec_participant: Vec<(String, std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)>,
    vec_client_done: Vec<bool>,
    request_status : RequestStatus,
    successful_ops: u64,
    failed_ops: u64,
    unknown_ops: u64,
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
        self.vec_participant.push((name.to_owned(), child, sender, receiver));
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

        println!("coordinator     :\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.successful_ops, self.failed_ops, self.unknown_ops);
    }



    pub fn send_participants(&mut self, msg: message::ProtocolMessage) -> bool{
        for i in 0..self.vec_participant.len(){
            if !self.running.load(Ordering::SeqCst) {
                return false;
            }
            let(participant_id, child, participant_tx, participant_rx) = &self.vec_participant[i];
            //trace!("sending proposal to {}", participant_id);
            //let msg = ProtocolMessage::generate(message::MessageType::CoordinatorPropose, String::from(txid), String::from(sid), opid);
            participant_tx.send( msg.clone()).unwrap();
        }
        return true;
    }

    pub fn receive_participants(&mut self, timeout_duration: Duration) ->message::RequestStatus{
        let mut status = message::RequestStatus::Unknown;
        let mut vote = true;
        for i in 0..self.vec_participant.len(){
            if !self.running.load(Ordering::SeqCst) {
                return status;
            }
            let(participant_id, child, participant_tx, participant_rx) = &self.vec_participant[i];
            let result = participant_rx.try_recv_timeout(timeout_duration);
            match result {
                Ok(participant_pm) => {                            
                    if participant_pm.mtype == message::MessageType::ParticipantVoteAbort{
                        vote = false;
                    }
                    //trace!("client request received {}", txid);    
                }
                Err(_) => {
                    vote = false;
                    trace!("participants {} timeout", i.to_string());
                    continue
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

    pub fn coordinator_exit(&mut self){
        let msg = ProtocolMessage::generate(message::MessageType::CoordinatorExit, String::from("Coordinator_exit"), String::from("0"), 0);                
        self.send_participants(msg.clone());
        for i in 0..self.vec_client.len(){            
            let(_, _, client_tx, _) = &self.vec_client[i];
            
            client_tx.send( msg.clone()).unwrap();
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
        let timeout_duration = Duration::from_millis(100);
        
        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            let mut client_done = 0;
            let mut txid = "";
            let mut sid = "";
            let mut opid = 0;
            //receive request from client
            for n in 0..self.vec_client.len(){
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
            }
        }
        self.coordinator_exit();         
        self.report_status();
    }
}
