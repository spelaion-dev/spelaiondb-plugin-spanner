#!/usr/bin/env bash
# Generates the test keys (they are not real secrets; they are in .gitignore).
#   keys/ca.crt      test certificate authority
#   keys/server.pem  the TLS proxy's certificate and key, valid for "localhost" and
#                    "spanner.spelaion.test" but not for 127.0.0.1: the tests connect to 127.0.0.1
#                    and check the certificate against a name, as through a tunnel
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p keys
# Git Bash on Windows rewrites arguments that start with "/" (like -subj "/CN=…").
export MSYS_NO_PATHCONV=1

if [ ! -f keys/ca.crt ]; then
  openssl req -x509 -new -nodes -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout keys/ca.key -out keys/ca.crt -days 3650 -subj "/CN=SpelaionDB test CA" \
    -addext "basicConstraints=critical,CA:TRUE" -addext "keyUsage=critical,keyCertSign,cRLSign" 2>/dev/null
fi

if [ ! -f keys/server.pem ]; then
  openssl req -new -nodes -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 \
    -keyout keys/server.key -out keys/server.csr -subj "/CN=spanner.spelaion.test" 2>/dev/null
  printf 'subjectAltName=DNS:localhost,DNS:spanner.spelaion.test\nbasicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\nextendedKeyUsage=serverAuth\n' > keys/server.ext
  openssl x509 -req -in keys/server.csr -CA keys/ca.crt -CAkey keys/ca.key -CAcreateserial \
    -out keys/server.crt -days 3650 -extfile keys/server.ext 2>/dev/null
  cat keys/server.crt keys/server.key > keys/server.pem
  rm -f keys/server.csr keys/server.ext keys/ca.srl
fi

echo "Keys in $(pwd)/keys"
