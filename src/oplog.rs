extern crate serde;
extern crate serde_json;
extern crate bincode;
extern crate commitlog;

use std::collections::HashMap;
use std::fs;
use std::fs::File;
//use std::intrinsics::size_of;
use std::io::BufReader;
use std::io::prelude::*;
use std::sync::Arc;
use std::sync::Mutex;

use oplog::commitlog::message::MessageSet;
//use commitlog::reader::MessageBufReader;
use oplog::commitlog::*;
use message;
use std::mem;

use crate::message::ProtocolMessage;

//#[derive(Debug)]
pub struct OpLog {
    seqno: u32,
    log_arc: Arc<Mutex<HashMap<u32, message::ProtocolMessage>>>,
    path: String,
    //lf: File,
    lf: CommitLog,
}

impl OpLog {

    /// 
    /// new(fpath: String)
    /// Creates a new Operations Log at the designated file path
    ///
    pub fn new(fpath: String) -> OpLog {
        let l = HashMap::new();
        let lck = Mutex::new(l);
        let arc = Arc::new(lck);
        let re= fs::remove_dir_all(fpath.clone());
        if re.is_err(){
            print!("delete dir err");
        }

        OpLog {
            seqno: 0,
            log_arc: arc,
            path: fpath.to_string(),
            //lf: File::create(fpath).unwrap(),
            lf: CommitLog::new(LogOptions::new(fpath)).unwrap(),
        }
    }

    ///
    /// from_file(fpath: String)
    ///
    /// Reads in and returns an existing Operations Log from the designated file
    ///
    pub fn from_file(fpath: String) -> OpLog {
        let mut seqno = 0;
        let mut l = HashMap::new();
        let scopy = fpath.clone();
        //let tlf = File::open(fpath).unwrap();
        let tlf = CommitLog::new(LogOptions::new(fpath)).unwrap();
        //let mut reader = BufReader::new(&tlf);
        
        /*let mut line = String::new();
        let mut len = reader.read_line(&mut line).unwrap();
        while len > 0 {
            let pm = message::ProtocolMessage::from_string(&line);
            if pm.uid > seqno {
                seqno = pm.uid;
            }
            l.insert(pm.uid, pm);
            line.clear();
            len = reader.read_line(&mut line).unwrap();
        }*/
        let last = tlf.last_offset().unwrap();
        
        let mut offsets =0;
        
        while offsets <= last {
            let messages = tlf.read(offsets, ReadLimit::default()).unwrap();
            for msg in messages.iter(){
                let st = String::from_utf8_lossy(msg.payload());
                let s = format!("{}",st);
                let pm = message::ProtocolMessage::from_string(&s);
                //println!("{}", serde_json::to_string(&pm).expect("read wrong"));
                if pm.uid > seqno {
                    seqno = pm.uid;
                }
                l.insert(pm.uid, pm);
                offsets += 1;
            }
        }
        
        let lck = Mutex::new(l);
        let arc = Arc::new(lck);
        OpLog {
            seqno: seqno,
            log_arc: arc,
            path: scopy,
            lf: tlf,
        }
    }

    ///
    /// append(t, tid, sender, op)
    ///
    /// Appends an entry to the Operations Log
    ///
    pub fn append(&mut self, t: message::MessageType, tid: String, sender: String, op: u32) {
        let lck = Arc::clone(&self.log_arc);
        let mut log = lck.lock().unwrap();
        self.seqno += 1;
        let id = self.seqno;
        let pm = message::ProtocolMessage::generate(t, tid, sender, op);
        //serde_json::to_writer(&mut self.lf, &pm).unwrap();
        self.lf.append_msg(serde_json::to_string(&pm).expect("")).unwrap();
        //writeln!(&mut self.lf).unwrap();
        self.lf.flush().unwrap();
        log.insert(id, pm);
    }

    ///
    /// read(offset)
    ///
    /// Returns the log entry corresponding with the given offset
    ///
    pub fn read(&mut self, offset: &u32) -> message::ProtocolMessage {
        let lck = Arc::clone(&self.log_arc);
        let log = lck.lock().unwrap();
        let pm = log[&offset].clone();
        pm
    }

    ///
    /// arc
    ///
    /// Returns an ARC of the mutex locked log
    ///
    pub fn arc(&self) -> Arc<Mutex<HashMap<u32, message::ProtocolMessage>>> {
        Arc::clone(&self.log_arc)
    }
}
