bash -c uname\ -sm\;\ rustc\ +stable\ --version\;\ cargo\ +stable\ --version\;\ printf\ \"CARGO_TARGET_DIR=%s\\n\"\ \"\$CARGO_TARGET_DIR\"\;\ printf\ \"PROTOC=%s\\n\"\ \"\$PROTOC\" 
