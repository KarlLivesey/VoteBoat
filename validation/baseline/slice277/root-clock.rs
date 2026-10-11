use std::{collections::BTreeSet, sync::{Arc, Barrier}, thread, time::{SystemTime, UNIX_EPOCH}};
fn main() {
 let barrier=Arc::new(Barrier::new(8));
 let workers:Vec<_>=(0..8).map(|_|{
  let barrier=barrier.clone();
  thread::spawn(move || {barrier.wait();(0..20000).map(|_|SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()).collect::<Vec<_>>()})
 }).collect();
 let values:Vec<_>=workers.into_iter().flat_map(|w|w.join().unwrap()).collect();
 let unique:BTreeSet<_>=values.iter().copied().collect();
 println!("actual_timestamp_calls={} unique={} collisions={}", values.len(), unique.len(), values.len()-unique.len());
}
