sh -c for attempt in 1 2 3 4 5 6 7 8; do echo "attempt=$attempt"; target/debug/examples/native_benchmark-a647f3ef0d23c1ec lanes::tests offered::cluster::tests --test-threads=8 || exit; done
