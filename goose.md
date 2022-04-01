# Slow RPC-API debugging report

A user reported running a load test and getting and capping out at ~25 requests/s. Which is pretty abysmal. We haven't actually done any load testing at all, so maybe this shouldn't be unexpected. The user used [locust](https://locust.io/) which is a very easy to use python framework for load testing.

The user ran a combination of 5 different API calls, all equally split. He since deleted that message so I don't know exactly which ones, and I don't know what parameters he used (valid or invalid hashes?).

User was also running a fully syncd node on mainnet, so db write access "should" be quite limited.

## Methodology

I've found a rust `locust` clone built in rust, [goose](https://docs.rs/goose/latest/goose/). I'll be using this to replicate the users report and investigate causes (if any).

I'll be adding a commit for each test run which will include source code changes and results.

Running node with:
```
RUST_LOG=pathfinder=info cargo run --release --bin pathfinder -- -c goerli.toml
```
and `goose`:
```
cargo run --release --bin goose -- -H http://127.0.0.1:9545 --report-file report.html -u 30 -t 50 --no-reset-metrics
```

`pathfinder` is still actively syncing from goerli network; it is very near to genesis (`< 300`).

## Pre-testing thoughts

Possible culprits:
- python :D `locust` might be slow -- which it probably is, but not 25rps slow..
- sqlite WAL -- we haven't enabled this.. known multi-access performance boost.
- lack of indexing in some tables.. although could it really be this bad?

## Test 1

This tests a single endpoint: `starknet_syncing`. This was chosen as it doesn't access the database, so it should be a baseline of sorts.

Throughput: 83 811 rps.