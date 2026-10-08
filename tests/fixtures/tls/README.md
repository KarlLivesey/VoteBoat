# Public test-only TLS material

These generated ECDSA P-256 keys and certificates are deliberately public test
fixtures. They are not deployment credentials. Tests load them with `include_bytes!`; the local counter-service quickstart
explicitly selects this fixture directory. Production constructors require
host-supplied credentials.

The test CA and node leaves were generated with OpenSSL 3.6.5. The CA and leaves
have fixed validity from 2020-01-01 through 2050-01-01, so fixture validity does
not depend on the machine that runs a test during that interval. Leaves have
critical CA:false and digitalSignature constraints, serverAuth/clientAuth EKUs,
and DNS SANs node1.voteboat.test through node3.voteboat.test. The unrelated CA
is used only for a rejection test. Keys are unencrypted PKCS#8 DER; certificates
are X.509 DER. Node 3 is an otherwise trusted alternative certificate pin.

To replace the fixtures, generate an EC P-256 CA with keyCertSign/cRLSign and
sign the named leaf CSRs using `openssl ca` with explicit start/end dates,
then export certificates with `openssl x509 -outform DER` and private keys with
`openssl pkcs8 -topk8 -nocrypt -outform DER`. Replace the complete CA/leaf/key set
together and run the secure-session tests. These generated project fixtures
are distributed under the repository's RPL 1.5 license.
