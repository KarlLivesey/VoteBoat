// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
//! Authenticated explicit split administration over the native guarded apps.
#[path = "support/transfer_app.rs"]
mod app;
#[path = "support/transfer_client.rs"]
mod client;
#[path = "support/command_client.rs"]
mod command_client;
#[path = "support/command_endpoints.rs"]
mod command_endpoints;
#[path = "support/transfer_connection.rs"]
mod connection;
#[path = "support/transfer_profile.rs"]
mod profile;
#[path = "support/transfer_server.rs"]
mod server;
#[path = "support/service_access.rs"]
mod service_access;
#[path = "support/service_setup.rs"]
mod setup;
#[path = "support/transfer_wire.rs"]
mod wire;
use setup::Failure;
const HELP: &str = "voteboat-transfer plan AUTHORITY SOURCE LEFT RIGHT RESPONSIBILITY SPLIT LIFECYCLE PUBLICATION\nvoteboat-transfer serve create|recover ROOT NODE BASE TLS PROFILE GROUP ACCESS tcp|quic [--deployment FILE]\nvoteboat-transfer client PROFILE ENDPOINTS TLS PRINCIPAL status|start|resume|step\nvoteboat-transfer command PROFILE ENDPOINTS TLS PRINCIPAL GROUP COMMAND...";
fn main() -> Result<(), Failure> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    match args.as_slice() {
        [v, rest @ ..] if v == "plan" => {
            print!("{}", profile::plan(rest)?);
            Ok(())
        }
        [v, rest @ ..] if v == "serve" => server::serve(rest),
        [v, rest @ ..] if v == "client" => client::operate(rest),
        [v, rest @ ..] if v == "command" => client::command(rest),
        [v] if v == "--help" => {
            println!("{HELP}");
            Ok(())
        }
        _ => Err(HELP.into()),
    }
}
