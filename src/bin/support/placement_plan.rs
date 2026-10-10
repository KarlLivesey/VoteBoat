// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
// Unless explicitly acquired and licensed from Licensor under another license,
// the contents of this file are subject to the Reciprocal Public License
// ("RPL") Version 1.5, or subsequent versions as allowed by the RPL, and You may
// not copy or use this file in either source code or executable form, except
// in compliance with the terms and conditions of the RPL.
//
// All software distributed under the RPL is provided strictly on an "AS IS"
// basis, WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESS OR IMPLIED, AND LICENSOR
// HEREBY DISCLAIMS ALL SUCH WARRANTIES, INCLUDING WITHOUT LIMITATION, ANY
// WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, QUIET
// ENJOYMENT, OR NON-INFRINGEMENT. See the RPL for specific language governing
// rights and limitations under the RPL.
//! Offline plans feed the existing authorized, recoverable execution driver.
use super::{
    administration::{node, tree},
    placement_input::Input,
    setup::{checked, Failure},
};
use std::path::Path;
use voteboat::{
    identity::*,
    membership::*,
    native::placement_planning::NativePlacementPlanner,
    placement::*,
    quorum::{Limits, Policy},
};
fn operation(s: &str) -> Result<OperationId, Failure> {
    OperationId::new(s.parse()?).ok_or_else(|| "invalid operation ID".into())
}
fn treatment(s: &str) -> Result<RemovedVoters, Failure> {
    match s {
        "retire" => Ok(RemovedVoters::Retire),
        "retain" => Ok(RemovedVoters::RetainAsLearners),
        _ => Err("expected retire or retain".into()),
    }
}
fn project(current: &Membership, record: &ConfigurationRecord) -> Result<Membership, Failure> {
    let ConfigurationChange::Learners(next) = &record.change else {
        return Err("expected planned learner".into());
    };
    let mut operations = current.operations().clone();
    operations.insert(record.operation);
    let index = operations.len() as u64;
    checked(Membership::from_checkpoint(
        next.clone(),
        None,
        index,
        operations,
        index,
    ))
}
fn records(input: &Input, args: &[String]) -> Result<Vec<ConfigurationRecord>, Failure> {
    let request = input.request(&input.current);
    match args {
        [kind,op] if kind=="learner"=>Ok(vec![checked(plan_learner(&NativePlacementPlanner,&input.authorizer,request,operation(op)?))?.record]),
        [kind,retiring,learner_op,voter_op,removed] if kind=="replace"=>{
            let replacement=checked(plan_replacement(&NativePlacementPlanner,&input.authorizer,request,node(retiring)?,operation(learner_op)?))?;
            let future=project(&input.current,&replacement.learner.record)?;
            let change=checked(plan_voter_change(&input.authorizer,input.request(&future),replacement.target_policy,treatment(removed)?,operation(voter_op)?))?;
            Ok(vec![replacement.learner.record,change.joint,change.finalize])
        },
        [kind,op,removed,policy @ ..] if kind=="voters"=>{
            let mut tokens=policy.iter().map(String::as_str);let policy=checked(Policy::new(tree(&mut tokens,0,&mut 16384)?,Limits::default()))?;
            if tokens.next().is_some(){return Err("trailing target policy fields".into())}
            let change=checked(plan_voter_change(&input.authorizer,request,policy,treatment(removed)?,operation(op)?))?;
            Ok(vec![change.joint,change.finalize])
        },
        _=>Err("expected learner OPERATION | replace RETIRING_NODE LEARNER_OPERATION VOTER_OPERATION retire|retain | voters OPERATION retire|retain POLICY".into()),
    }
}
pub fn run(path: &Path, args: &[String]) -> Result<(), Failure> {
    let input = Input::load(path)?;
    let records = records(&input, args)?;
    let output = super::placement_format::format(&input, &records)?;
    print!("{output}");
    Ok(())
}
