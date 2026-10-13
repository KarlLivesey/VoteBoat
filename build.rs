// SPDX-License-Identifier: RPL-1.5
// Copyright (c) 2026 Karl Livesey
fn main() {
    println!("cargo:rustc-check-cfg=cfg(voteboat_checkpoint_debug_oracle)");
    println!("cargo:rerun-if-env-changed=PROFILE");
    if !matches!(std::env::var("PROFILE").as_deref(), Ok("release")) {
        println!("cargo:rustc-cfg=voteboat_checkpoint_debug_oracle");
    }
}
