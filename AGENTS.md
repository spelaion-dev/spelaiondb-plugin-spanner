# SpelaionDB's Spanner plugin — rules for agents

Google Cloud Spanner for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb): the engine's
driver, and a small program that serves it to SpelaionDB over its plugin protocol. Apache-2.0.

**The rules of SpelaionDB's repository apply here** (its `AGENTS.md`: all the code is written
by an agent, the compiler, the tests and CI are the only reviewer; everything in English;
sign-off; one task, one short branch; commit subjects that read well in a changelog). This file
says only what is different in this repository.

## What this is
- `src/`: the driver. It implements `Driver` and `Connection` of `spelaion-driver-api`
  (SpelaionDB's ADR-005) over Spanner's REST API, GoogleSQL dialect.
  - `http.rs` opens its own connections with `hyper`: to `connect_addr` when it is given (the
    local end of SpelaionDB's tunnel or proxy), never through a system proxy, with TLS verified
    against `host` (`tls.rs`, libpq's modes, as in the other plugins). The password is an OAuth
    access token, sent as `Authorization: Bearer`, never in an address, and taken out of every
    error. Without one (the emulator) no header is sent.
  - `lib.rs`: the database's name is checked (`database_path`) and every name the server gives
    (a session, an operation) is checked to stay under it (`checked_name`) before it reaches a
    path. A connection is one Spanner session; its requests go one at a time through an actor.
    A statement goes where its first word says (`kind`):
    - a query is a single-use read;
    - a data change outside a transaction runs in a read-write transaction begun first and committed after, or rolled back when it fails (one left open would hold every schema change back);
    - `BEGIN`, `COMMIT` and `ROLLBACK` drive a read-write transaction, its statements numbered (`seqno`);
    - DDL goes through the admin API and its operation is followed until it is done.

    A session the server no longer has loses the connection (ADR-028). No cancel: the REST API cannot stop a statement.
  - `types.rs`: Spanner's typed JSON into Arrow (`INT64` sent as text, `BYTES` in base64,
    `TIMESTAMP` to the microsecond); arrays and structs are JSON cells, with an `INT64` past
    2^53 kept as a string. `Rows` joins the streaming API's cut values (`chunkedValue`) as
    Spanner's API describes, before rows are read. The emulator's gateway wraps each streamed
    message in `{"result": …}`, Google sends a JSON array: both are read.
  - `introspect.rs`: `INFORMATION_SCHEMA` with every name bound as a parameter. The default
    schema (`''`) is shown as `default`; definitions come from the database's DDL.
- `src/bin/spelaiondb-plugin-spanner.rs`: the program. It hands the driver to
  `spelaion_plugin_sdk::run` and nothing else.
- `tests/spanner.rs`: the type zoo, schema and data changes, transactions, a failed change
  leaving nothing open, batches, long values, errors, the explorer, the database name's checks,
  and TLS checked against the host name with the token never shown, against the emulator.
  Spanner runs one schema change at a time: the tests that change the schema take turns.
  `tests/conformance.rs`: SpelaionDB's conformance set against this program, and `plugin.toml`
  checked against what the program says of itself.
- `testing/` and `ci/services.sh`: Google's Spanner emulator 1.5.28 on 19020 (no login, no shell
  in its image: `ci/services.sh` waits for it and creates instance `test` and database `demo`
  in project `spelaion`), and a TLS proxy on 19443 with a certificate for
  `spanner.spelaion.test` (`testing/gen-keys.sh`). Memory is bounded (1 GB and 256 MB).
- `plugin.toml`: the protocol version, the engines and the lowest SpelaionDB of this plugin.

## A plugin is the wire, not the rules
SpelaionDB's core splits, classifies and guards statements, keeps secrets, opens tunnels and
writes the audit (ADR-041 there). This plugin connects, runs what it is sent and introspects.
So, and these are not negotiable (`docs/plugin-protocol.md` in SpelaionDB's repository, "What a
plugin must never do"):

- **Standard output is the protocol and nothing else.** Never `println!`.
- **The token (the password) arrives in `connect` and is written nowhere**: not to a log, an error, a
  file, the environment or a request's address.
- **It runs what it is sent.** It never decides whether a statement is a read, and never
  changes one.
- **Nothing is run again** after a session is lost.
- **It ends when its standard input closes** (the SDK does this).
- It reads no configuration file, keychain or store of SpelaionDB's, opens no tunnel, and
  reads no proxy from the environment.

## The contract comes from SpelaionDB, by tag
`spelaion-driver-api`, `spelaion-plugin-sdk` and, for the tests, `spelaion-plugin-host` are git
dependencies on one tag of SpelaionDB's repository (`plugin-sdk-vX.Y.Z`).

## Releases
Pushing a tag `vX.Y.Z` (the version in `Cargo.toml`) runs the release every plugin shares
(the template's repository, called by commit). **Nothing is signed and no secret lives here**:
a release reaches users only when SpelaionDB's repository pins its hashes in a pull request of
its own (ADR-042 there).

## Branches
`main` is protected: it changes only through pull requests whose CI is green, and cannot be
force-pushed or deleted. A pull request that touches a file in `.github/CODEOWNERS` waits for
an owner. The work is tracked in SpelaionDB's tracker (Plane, project `SDB`): write the item's
identifier in the pull request's description, never in a commit subject.

## Before calling something done
- `cargo fmt --all`, `cargo clippy --all-targets -- -D warnings`.
- The tests against real servers (without the variables they are skipped, and that does
  **not** count as tested): `ci/services.sh` and
  `SPELAION_TEST_SPANNER=127.0.0.1:19020 SPELAION_TEST_SPANNER_TLS=127.0.0.1:19443 cargo test`.
- `cargo deny check` if dependencies change.
- If something could not be tested, say so.
