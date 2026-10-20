# W-C41: bounded snapshot reads

Writer pages consume eight rows per query and receiver pages consume four, with one lookahead row. Every page starts a fresh bounded read; unconsumed rows are discarded at the boundary. The incremental size calculation and exact final full measurement remain in place.

The receiver uses the native near-cap peak above its empty baseline as the memory proxy. Four is the largest measured candidate below the 64 MiB ceiling, leaving 8.75 MiB of margin. Durable Object total memory is unmeasured: the existing Worker harness does not expose this read path, and no endpoint was added. The three-copy arithmetic is not the acceptance criterion.

The native writer uses eight: sixteen saved another 1.73 seconds (15.8%) while doubling the consumed-row prefetch. Writer admission is separate from paging and scans full rows in batches of 256. Its roughly 132–134 MiB near-cap process peak is a follow-up, not a measurement of the pager buffer.

## Native measurements

One dev-profile run per candidate and fixture; sequential normal builds in target/debug, locked dependencies, four build jobs. Preparation ran in separate processes. Timing sums only public page calls; serialization and row-digest validation happen outside that timer. RSS is per-child os.wait4 rusage on macOS, calibrated with a 100 MiB allocation (reported increment 100.000 MiB). Process RSS includes engine/cache/code/allocator state and is not Durable Object isolate memory.

The small fixture has 20,000 rows, each with a 512-byte body. It always produced 79 pages. The near-cap fixture has 17 rows with 1,568,768-byte ASCII bodies; only its first page is measured (one row, 1,569,300 encoded writer bytes / 1,569,298 receiver bytes). Empty baselines use the same API and schema.

| Pager | Batch | 20k paging seconds | Near-cap RSS MiB | Empty RSS MiB | Increment MiB |
|---|---:|---:|---:|---:|---:|
| writer | 1 | 76.544 | 134.422 | 28.297 | 106.125 |
| writer | 8 | 10.964 | 132.078 | 28.406 | 103.672 |
| writer | 16 | 9.232 | 133.578 | 28.375 | 105.203 |
| copy | 1 | 76.626 | 63.328 | 26.203 | 37.125 |
| copy | 2 | 40.244 | 70.859 | 26.203 | 44.656 |
| copy | 4 | 20.816 | 81.500 | 26.250 | 55.250 |
| copy | 8 | 11.158 | 95.031 | 26.219 | 68.812 |
| copy | 16 | 6.330 | 114.375 | 26.219 | 88.156 |

All candidates preserved row counts, page counts, total encoded bytes and row digests within each pager/fixture. The writer and receiver have different position/schema metadata, so their encoded totals are compared within their own path. The permanent example is crates/fleet-sync-writer/examples/snapshot_paging_measurement.

## Proof gates

The existing independent one-row/full-measure oracles compare exact serialized page bytes, page digests and refusal encoded_size values, including row caps, table transitions, composite/blob values, escaping, invalid cursors, exact target/hard limits and over-limit rows/schema. Added per-page row-query assertions use literal adopted bounds independent of production constants: floor(rows/8)+1 writer, floor(rows/4)+1 receiver. Across pages these sum to at most floor(total rows/B)+pages.

Sensitivity: changing either pager back to one consumed row per query independently fails its generated real-path test at 256 row queries for a 256-row page (0 passed / 1 failed each, exit 101). Both final source files were restored byte-for-byte after the controls. The restored differential tests passed in the full workspace run: 763 parent tests passed, 0 failed, 2 existing ignores. The consumer suite passed 96 tests, 0 failed, 0 ignored. All 13 final batch gates exited 0. Exact commands are in the W-C43 proof document, Final validation section. No hosted-CI or Durable Object heap observation is inferred.

## Limits and follow-up

- No Durable Object total peak was measured. Native proxy headroom applies to the measured near-cap fixture; arbitrary oversized stored values are not a hard allocation bound.
- The independent writer admission scan read batches of 256 full rows. In the near-cap fixture its native process RSS was 132.078 MiB at the chosen batch, versus 28.406 MiB empty (103.672 MiB increment). Investigating that scan is outside this batch.
- One dev run is a local comparison, not a production throughput or tail-latency claim.
- Initial time-l statistics were unavailable in the sandbox; that attempt was excluded and the measurement was repeated with calibrated per-child rusage.

Measurement fixture cleanup removed 319.789 MiB of logical data after lsof found no users; df available space increased 319.766 MiB during that cleanup. Reports, logs and receipts remain. Every measurement binary was built/run in the worktree target/debug/examples; none was copied.
