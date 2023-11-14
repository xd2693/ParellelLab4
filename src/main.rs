#[macro_use]
extern crate log;
extern crate stderrlog;
extern crate clap;
extern crate ctrlc;
extern crate ipc_channel;
use std::env;
use std::fs;
//use std::string;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::process::{Child,Command};
use client::Client;
use ipc_channel::ipc::IpcSender as Sender;
use ipc_channel::ipc::IpcReceiver as Receiver;
use ipc_channel::ipc::IpcOneShotServer;
use ipc_channel::ipc::channel;
pub mod message;
pub mod oplog;
pub mod coordinator;
pub mod participant;
pub mod client;
pub mod checker;
pub mod tpcoptions;
use message::ProtocolMessage;
//use message::RequestStatus;
//use std::thread;
//use std::time::Duration;
//use std::time::Instant;


use crate::participant::Participant;

///
/// pub fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions) -> (std::process::Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     child_opts: CLI options for child process
///
/// 1. Set up IPC
/// 2. Spawn a child process using the child CLI options
/// 3. Do any required communication to set up the parent / child communication channels
/// 4. Return the child process handle and the communication channels for the parent
///
/// HINT: You can change the signature of the function if necessary
///
fn spawn_child_and_connect(child_opts: &mut tpcoptions::TPCOptions, pside_name_rx : IpcOneShotServer<String>) -> (Child, Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let child = Command::new(env::current_exe().unwrap())
        .args(child_opts.as_vec())
        .spawn()
        .expect("Failed to execute child process");

    let (tx, rx) = channel().unwrap();
    //To do
    // Name exchange through existing parent one shot server
    let (pside_name_rx, pside_name_tx_name) = pside_name_rx.accept().unwrap();
    
    let pside_sender_tx_name = pside_name_rx.recv().unwrap();
    
    let pside_name_tx = Sender::connect(pside_name_tx_name).unwrap();
    // Send sender_tx_name to child
    let (pside_sender_rx, pside_sender_rx_name) = IpcOneShotServer::new().unwrap();
    pside_name_tx.send(pside_sender_rx_name.clone()).unwrap();

    let pside_sender_tx = Sender::connect(pside_sender_tx_name).unwrap();
    pside_sender_tx.send(tx).unwrap();
    let (_, tx) : (_, Sender<ProtocolMessage>) = pside_sender_rx.accept().unwrap();
    (child, tx, rx)
}

///
/// pub fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>)
///
///     opts: CLI options for this process
///
/// 1. Connect to the parent via IPC
/// 2. Do any required communication to set up the parent / child communication channels
/// 3. Return the communication channels for the child
///
/// HINT: You can change the signature of the function if necessasry
///
fn connect_to_coordinator(opts: &tpcoptions::TPCOptions) -> (Sender<ProtocolMessage>, Receiver<ProtocolMessage>) {
    let (tx, rx) = channel().unwrap();

    // TODO
    // Send name sender name to parent's known name server
    let cside_name_tx = Sender::connect(opts.ipc_path.clone()).unwrap();
    let (cside_name_rx, cside_name_rx_name) = IpcOneShotServer::new().unwrap();
    cside_name_tx.send(cside_name_rx_name.clone()).unwrap();
    let (cside_sender_rx, cside_sender_rx_name) = IpcOneShotServer::new().unwrap();
    cside_name_tx.send(cside_sender_rx_name.clone()).unwrap();

    let (_, cside_sender_tx_name) = cside_name_rx.accept().unwrap();
    let cside_sender_tx = Sender::connect(cside_sender_tx_name).unwrap();
    cside_sender_tx.send(tx).unwrap();
    let(_, tx) : (_, Sender<ProtocolMessage>) = cside_sender_rx.accept().unwrap();
    (tx, rx)
}

///
/// pub fn run(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Creates a new coordinator
/// 2. Spawns and connects to new clients processes and then registers them with
///    the coordinator
/// 3. Spawns and connects to new participant processes and then registers them
///    with the coordinator
/// 4. Starts the coordinator protocol
/// 5. Wait until the children finish execution
///
fn run(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let coord_log_path = format!("{}//{}", opts.log_path, "coordinator.log");

    // TODO
    if let Ok(_) = fs::metadata(opts.log_path.clone()){
        let _ = fs::remove_dir_all(opts.log_path.clone());
    }
    let mut coordinator = coordinator::Coordinator::new(coord_log_path.clone(), &running, opts.coordinator_fail_stage);
    //info!("Created IPC {coord_log_path}");
    let mut cCount = 0;
    let mut optsClient = opts.clone();
    optsClient.mode = String::from("client");
    for _ in 0..opts.num_clients {
        optsClient.num = cCount;
        let (pside_name_rx, pside_name_rx_name) = IpcOneShotServer::new().unwrap();
        optsClient.ipc_path = pside_name_rx_name.clone();
        let client = spawn_child_and_connect(&mut optsClient, pside_name_rx);
        let client_id_str = format!("client_{}", cCount);
        coordinator.client_join(&client_id_str, client);
        cCount += 1;
    }
    info!("Created {} clients", optsClient.num_clients);

    let mut pCount = 0;
    let mut optsParticipant = opts.clone();
    optsParticipant.mode = String::from("participant");
    for _ in 0..opts.num_participants {
        optsParticipant.num = pCount;
        let (pside_name_rx, pside_name_rx_name) = IpcOneShotServer::new().unwrap();
        optsParticipant.ipc_path = pside_name_rx_name.clone();
        let participant = spawn_child_and_connect(&mut optsParticipant, pside_name_rx);
        let participant_id_str = format!("participant_{}", pCount);
        coordinator.participant_join(&participant_id_str, participant);

        pCount += 1;

    }
    
        coordinator.protocol();
}



///
/// pub fn run_client(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new client
/// 3. Starts the client protocol
///
fn run_client(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    // TODO
    let client_id_str = format!("client_{}", opts.num);
    
    let (sender, receiver) = connect_to_coordinator(opts);
    let mut client = Client::new(client_id_str, running, sender, receiver, opts.num_clients*opts.num_participants);
    client.protocol(opts.num_requests);
}

///
/// pub fn run_participant(opts: &tpcoptions:TPCOptions, running: Arc<AtomicBool>)
///     opts: An options structure containing the CLI arguments
///     running: An atomically reference counted (ARC) AtomicBool(ean) that is
///         set to be false whenever Ctrl+C is pressed
///
/// 1. Connects to the coordinator to get tx/rx
/// 2. Constructs a new participant
/// 3. Starts the participant protocol
///
fn run_participant(opts: & tpcoptions::TPCOptions, running: Arc<AtomicBool>) {
    let participant_id_str = format!("participant_{}", opts.num);
    let participant_log_path = format!("{}//{}.log", opts.log_path, participant_id_str);

    // TODO
    
    let (sender, receiver) = connect_to_coordinator(opts);
    let mut participant = Participant::new(participant_id_str, participant_log_path, running.clone(), opts.send_success_probability, opts.operation_success_probability, sender, receiver, opts.num_participants);
    participant.protocol();
}

fn main() {
    // Parse CLI arguments
    let opts = tpcoptions::TPCOptions::new();
    // Set-up logging and create OpLog path if necessary
    stderrlog::new()
            .module(module_path!())
            .quiet(false)
            .timestamp(stderrlog::Timestamp::Millisecond)
            .verbosity(opts.verbosity)
            .init()
            .unwrap();
    match fs::create_dir_all(opts.log_path.clone()) {
        Err(e) => error!("Failed to create log_path: \"{:?}\". Error \"{:?}\"", opts.log_path, e),
        _ => (),
    }

    // Set-up Ctrl-C / SIGINT handler
    let running = Arc::new(AtomicBool::new(true));
    let r = running.clone();
    let m = opts.mode.clone();
    ctrlc::set_handler(move || {
        r.store(false, Ordering::SeqCst);
        if m == "run" {
            print!("exit ctrl-c\n");
        }
    }).expect("Error setting signal handler!");

    // Execute main logic
    match opts.mode.as_ref() {
        "run" => run(&opts, running),
        "client" => run_client(&opts, running),
        "participant" => run_participant(&opts, running),
        "check" => checker::check_last_run(opts.num_clients, opts.num_requests, opts.num_participants, &opts.log_path),
        _ => panic!("Unknown mode"),
    }
}
