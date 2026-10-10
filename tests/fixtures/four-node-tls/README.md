# Public four-node TLS test material

Separate ECDSA P-256 test CA and leaves for authenticated new-voter histories.
All private keys here are deliberately public test fixtures, never deployment
credentials. The CA private key is not retained. RPL-1.5 applies.

Generated with OpenSSL 3.6.5, SHA-256 signatures, validity fixed from 2020-01-01
through 2050-01-01. Leaves have critical CA:false/digitalSignature constraints,
serverAuth/clientAuth EKUs and their exact node1–4.voteboat.test DNS SAN. Keys
are unencrypted PKCS#8 DER; certificates are X.509 DER. Existing three-node
fixtures remain unchanged.

Generation uses `openssl ecparam -name prime256v1 -genkey`, `openssl req`,
`openssl ca` with explicit start/end dates and extensions, then `openssl x509`
and `openssl pkcs8 -topk8 -nocrypt` DER export. The actual executable tests
validate the TLS sessions and pins; four-node peer and client names agree.
