# spelaiondb-plugin-spanner

Google Cloud Spanner for [SpelaionDB](https://github.com/spelaion-dev/spelaiondb), the desktop
database client. The engine's driver, served to SpelaionDB as a plugin.

SpelaionDB's engines are programs it starts and talks to over their standard input and output
(its ADR-041). This is the one for Spanner. It is built here, in its own repository, and
SpelaionDB pins by hash the exact release it was tested with (its ADR-042, ADR-055).

- **Protocol:** [SpelaionDB's plugin protocol](https://github.com/spelaion-dev/spelaiondb/blob/dev/docs/plugin-protocol.md),
  version 1.1. Experimental until SpelaionDB 1.0.
- **Engine:** `spanner`, over Spanner's REST API (GoogleSQL dialect), with `hyper` and rustls.
  The connection's database is `projects/<project>/instances/<instance>/databases/<database>`
  (or `<project>/<instance>/<database>`). On Google the password is an OAuth access token
  (for example a `cmd:` secret running `gcloud auth print-access-token`); the emulator takes
  none.
  - Statements:
    - a query is a single-use read;
    - a data change outside a transaction runs in a read-write transaction of its own;
    - `BEGIN`, `COMMIT` and `ROLLBACK` drive a read-write transaction across statements;
    - DDL goes through the admin API, followed until it is done.
  - Results are read from the streaming API, with long values joined.
  - Values are typed by their column: arrays and structs are JSON.
  - The explorer shows schemas (the default one as `default`), tables, views, columns and primary keys.
- **What it does not do yet:** read-only connections and agents (Spanner enforces read-only transactions, but making them SpelaionDB's barrier is a decision of its own), cancelling a statement (the REST API cannot), editing rows, bulk imports, bound values, plans, foreign keys, and the PostgreSQL dialect.
  Google itself is not tested here: the tests run against Google's own emulator.
- **License:** Apache-2.0.

## Working on it

```bash
ci/services.sh
```

starts the test servers (Google's Spanner emulator 1.5.28 in Docker, and the same behind a TLS
proxy, with their keys) and creates the test instance and database. Then

```bash
SPELAION_TEST_SPANNER=127.0.0.1:19020 SPELAION_TEST_SPANNER_TLS=127.0.0.1:19443 cargo test
```

runs the type zoo, schema and data changes, transactions, batches, long values, errors, the
explorer, the database name's checks, TLS and the token, and the protocol's conformance set
against this plugin's own program. Without the variables the tests that need a server are
skipped, which does not count as tested.

To try a build with SpelaionDB, point a development build of it at the program
(`SPELAION_DEV_PLUGIN_SPANNER=/path/to/target/debug/spelaiondb-plugin-spanner`, in SpelaionDB's
repository; a release build ignores that variable).

`AGENTS.md` has the rules. Reports and questions go to
[SpelaionDB's issues](https://github.com/spelaion-dev/spelaiondb/issues).
