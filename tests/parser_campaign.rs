// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
#[path = "parser_campaign/generator.rs"]
mod generator;
#[path = "parser_campaign/routing.rs"]
mod routing;
#[cfg(feature = "native")]
#[path = "parser_campaign/snapshot.rs"]
mod snapshot;
#[cfg(feature = "native")]
mod support;
#[cfg(feature = "native")]
#[path = "parser_campaign/wal.rs"]
mod wal;
#[cfg(feature = "native")]
#[path = "parser_campaign/wire.rs"]
mod wire;
