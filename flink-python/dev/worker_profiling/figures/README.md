<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# FLIP figure provenance

The architecture is an illustration; the other four figures show actual POC results.
The UI and captures originate from the 2026-11-07 single-TaskManager experiment. Report screenshots
were rendered on 2026-11-09. No cluster was restarted or new profiling session executed for these images.

| Figure | Source and interpretation |
| --- | --- |
| `../architecture.png` | Updated architecture, exported with draw.io 30.2.6 from `../architecture.drawio`. The PNG also embeds the editable diagram. |
| `flink-web-ui.png` | Unmodified copy of `single-tm-workers/worker-selection.png`; one TM, one slot, two unfused operators, Python managed memory disabled. Actual browser UI, not a mockup. |
| `memray-result.png` | Memray 1.20.0 offline flamegraph from the completed MEMORY capture for PID 1757. Native stacks disabled. The hovered allocation is `allocate_feature_buffer`; the default report shows high-water-mark allocations. |
| `pyspy-flamegraph.png` | Browser rendering of the original py-spy 0.4.3 SVG for PID 1757, 5 seconds at 50 Hz, native and idle options disabled. The synthetic inference hotspot is visible. |

The source experiment is `output/playwright/pyflink-20261107/single-tm-workers/` at the repository root.
Its `unmanaged/result.json` records worker handles, JobID, PIDs, capture options, successful completion,
download comparison and independent UDF identities. Its `run.md` documents the workload and validation.
The image evidence does not certify general PyTorch/YOLO performance or overhead.

The original SVG is rendered unchanged as an HTML image for a static screenshot; its long command
heading can extend beyond the SVG viewport. Profiling data and frame labels were not rewritten.
The Memray report was generated with:

```sh
python -m memray flamegraph --no-web --confidential-files all \
  -o memray-infer.html python-memory-095664f1-0659-47d0-bf12-b3fe4f9a8424.bin
```

This uses the captured Python stacks and does not require resolving native libraries from the Linux
worker. The report is separate from Flink's UI. Local rendering files are retained under
`output/playwright/pyflink-flip-figures-20261109/`; the PNGs here are portable document attachments.

Original artifact SHA-256 checksums:

- `worker-selection.png`: `71d62ad8fe4c01b8121dacded2883c0f4bbe6ce11c5f2b6842a682c1f8755714`
- `python-4075ddc1-4a1b-43ea-9a36-b102f8b10f62.svg`: `b29fe868ea355fe394fbfa53224eb58dbecb6b7eeb96a7f820cf092a3478a221`
- `python-memory-095664f1-0659-47d0-bf12-b3fe4f9a8424.bin`: `44e3568c1c2998dd7cbcfab3bc4152a2fb145be7bec891e534feb2cb2c58c0d3`

## Additional Memray reports

`memray-stats.txt` and `memray-tree.png` were generated offline on 2026-11-09 from the same
PID 1757 recording as `memray-result.png`, using Memray 1.20.0. No new capture was run.

- [Stats](memray-stats.txt) preserves the complete output of `python -m memray stats -n 5 <recording.bin>`.
  Total allocation volume is cumulative; it is not peak or current memory usage.
- [Tree](memray-tree.png) shows Memray's native interactive terminal reporter, using its default
  high-watermark snapshot with up to 200 allocation locations. It selects `allocate_feature_buffer`.
  The view was rendered through Memray's `TreeReporter` / Textual application in headless mode at
  220 columns by 34 rows, exported with Textual's SVG screenshot API and rasterized with `rsvg-convert`.
  This is the same reporter used by `python -m memray tree <recording.bin>`. Allocation data and
  labels were not rewritten. The source-code pane is empty because the original Linux source path
  is unavailable on the analysis host; frame metadata remains visible.

- `memray-stats.txt`: `f4f430f2e589567723573a35f167e0c16a4d2b927e8b72f3c217fa11063eae6c`
- `memray-tree.png`: `36d051faeacd63c079b4e936dac39ad0286af7dccd8c2c790ede37e211500d7b`
