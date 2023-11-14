//!
//! client.rs
//! Implementation of 2PC client
//!
extern crate ipc_channel;
extern crate log;
extern crate stderrlog;

use std::thread;
use std::time::Duration;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
//use std::collections::HashMap;

use client::ipc_channel::ipc::IpcReceiver as Receiver;
//use client::ipc_channel::ipc::TryRecvError;
use client::ipc_channel::ipc::IpcSender as Sender;

use message;
//use message::MessageType;
//use message::RequestStatus;
use message::ProtocolMessage;

// Client state and primitives for communicating with the coordinator
#[derive(Debug)]
pub struct Client {
    pub id_str: String,
    pub running: Arc<AtomicBool>,
    pub num_requests: u32,
    pub tx: Sender<ProtocolMessage>,
    pub rx: Receiver<ProtocolMessage>,
    pub successful_ops: u64,
    pub failed_ops: u64,
    pub unknown_ops: u64,
    pub txid : String,
    pub timeout: u64,
}

///
/// Client Implementation
/// Required:
/// 1. new -- constructor
/// 2. pub fn report_status -- Reports number of committed/aborted/unknown
/// 3. pub fn protocol(&mut self, n_requests: i32) -- Implements client side protocol
///
impl Client {

    ///
    /// new()
    ///
    /// Constructs and returns a new client, ready to run the 2PC protocol
    /// with the coordinator.
    ///
    /// HINT: You may want to pass some channels or other communication
    ///       objects that enable coordinator->client and client->coordinator
    ///       messaging to this constructor.
    /// HINT: You may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor
    ///
    pub fn new(id_str: String,
               running: Arc<AtomicBool>,
               tx: Sender<ProtocolMessage>,
               rx: Receiver<ProtocolMessage>,
               timeout: u64) -> Client {
        Client {
            id_str: id_str,
            running: running,
            num_requests: 0,
            // TODO
            tx: tx,
            rx: rx,
            successful_ops: 0,
            failed_ops: 0,
            unknown_ops: 0,
            txid: String::from(""),
            timeout: timeout,
        }
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// Wait until the running flag is set by the CTRL-C handler
    ///
    pub fn wait_for_exit_signal(&mut self, timeout_duration: Duration) {
        trace!("{}::Waiting for exit signal", self.id_str.clone());

        // TODO
        loop {
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            let re = self.rx.try_recv();
            match re {
                Ok(result) =>{
                    if result.mtype == message::MessageType::CoordinatorExit{
                        break;
                    }
                }
                Err(_) =>{
                    thread::sleep(timeout_duration);
                }
            }
            
        }

        trace!("{}::Exiting", self.id_str.clone());
    }

    ///
    /// send_next_operation(&mut self)
    /// Send the next operation to the coordinator
    ///
    pub fn send_next_operation(&mut self) {

        // Create a new request with a unique TXID.
        self.num_requests = self.num_requests + 1;
        let txid = format!("{}_op_{}", self.id_str.clone(), self.num_requests);
        self.txid = txid.clone();
        let pm = message::ProtocolMessage::generate(message::MessageType::ClientRequest,
                                                    txid.clone(),
                                                    self.id_str.clone(),
                                                    self.num_requests);
        //info!("{}::Sending operation #{}", self.id_str.clone(), self.num_requests);

        // TODO
        let _ = self.tx.send(pm);
        //trace!("{}::Sent operation #{}", self.id_str.clone(), self.num_requests);
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the
    /// last issued request. Note that we assume the coordinator does
    /// not fail in this simulation
    ///
    pub fn recv_result(&mut self, timeout_duration: Duration)  {

        //info!("{}::Receiving Coordinator Result", self.id_str.clone());

        // TODO
        loop{
            if !self.running.load(Ordering::SeqCst) {
                break;
            }
            let re = self.rx.try_recv();
            match re {
                Ok(result) =>{
                    if result.mtype == message::MessageType::ClientResultCommit{
                        if result.txid == self.txid{
                            self.successful_ops += 1;
                        }else{
                            continue;
                        }
                        
                    }
                    else if result.mtype == message::MessageType::ClientResultAbort {
                        if result.txid == self.txid{
                            self.failed_ops += 1;
                        }else{
                            continue;
                        }
                        
                    }
                    // if coodinator fail, wait for message from coordinator 
                    else if result.mtype == message::MessageType::CoordinatorFail{
                        continue;                        
                    }
                    break;
                }
                Err(_) =>{
                    thread::sleep(timeout_duration);
                }
            }
            
        }
        

    }

    ///
    /// report_status()
    /// Report the abort/commit/unknown status (aggregate) of all transaction
    /// requests made by this client before exiting.
    ///
    pub fn report_status(&mut self) {
        // TODO: Collect actual stats
        

        println!("{:16}:\tCommitted: {:6}\tAborted: {:6}\tUnknown: {:6}", self.id_str.clone(), self.successful_ops, self.failed_ops, self.unknown_ops);
    }

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    ///
    pub fn protocol(&mut self, n_requests: u32) {

        // TODO
        //info!("Sending {} requests", n_requests);
        let timeout_duration = Duration::from_millis(20);
        for _ in 0..n_requests {
            if !self.running.load(Ordering::SeqCst) {
                //warn!("ctrl c {}", self.id_str);
                break;
            }
            self.send_next_operation();
            let sleep_duration = Duration::from_millis(self.timeout/2);
            warn!("client {}", self.timeout);
            thread::sleep(sleep_duration);
            if !self.running.load(Ordering::SeqCst) {
                self.unknown_ops += 1;
                
                break;
            }
            self.recv_result(timeout_duration);
        }
        
        self.wait_for_exit_signal(timeout_duration);
        self.report_status();
    }
}
