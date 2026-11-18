# Load test summary: match / unmatch (1.0.9 - 1.0.13)

Source: `<version>/result_all.txt`.

- Memory = peak memory (MB, 1 MB = 1,048,576 bytes), Time = elapsed time (ms). Raw values are in the result files.
- `1.0.12` is the result of tag `1.0.12-beta1`; `1.0.13` is the result of tag `1.0.13-beta1`.
- noHA only: 1k/10k/100k/1m events.
- Change = 1.0.13 vs 1.0.9.
- Each value is a single run. Time varies by roughly +-15-25% between runs of the same code for small sizes, so small differences are not significant.

## Memory (MB)

| Scenario | Events | 1.0.9 | 1.0.10 | 1.0.11 | 1.0.12 | 1.0.13 | Change |
|---|---|---:|---:|---:|---:|---:|---:|
| match | 1k | 5.12 | 5.15 | 5.12 | 5.16 | 5.23 | +2.1% |
| match | 10k | 5.16 | 5.17 | 5.19 | 5.17 | 5.21 | +1.1% |
| match | 100k | 5.15 | 5.16 | 5.16 | 5.18 | 5.18 | +0.7% |
| match | 1m | 5.18 | 5.21 | 5.20 | 5.22 | 5.19 | +0.2% |
| unmatch | 1k | 5.12 | 5.14 | 5.08 | 5.15 | 5.18 | +1.1% |
| unmatch | 10k | 5.11 | 5.10 | 5.11 | 5.17 | 5.16 | +1.0% |
| unmatch | 100k | 5.11 | 5.11 | 5.11 | 5.12 | 5.15 | +0.8% |
| unmatch | 1m | 5.12 | 5.12 | 5.14 | 5.13 | 5.15 | +0.7% |

## Time (ms)

| Scenario | Events | 1.0.9 | 1.0.10 | 1.0.11 | 1.0.12 | 1.0.13 | Change |
|---|---|---:|---:|---:|---:|---:|---:|
| match | 1k | 198 | 192 | 198 | 188 | 186 | -6.1% |
| match | 10k | 661 | 691 | 672 | 699 | 665 | +0.6% |
| match | 100k | 3,820 | 3,905 | 3,660 | 3,835 | 3,944 | +3.2% |
| match | 1m | 35,355 | 35,344 | 35,754 | 35,319 | 35,214 | -0.4% |
| unmatch | 1k | 113 | 120 | 127 | 117 | 136 | +20.4% |
| unmatch | 10k | 395 | 405 | 412 | 432 | 396 | +0.3% |
| unmatch | 100k | 1,752 | 1,665 | 1,660 | 1,770 | 1,712 | -2.3% |
| unmatch | 1m | 14,705 | 14,846 | 14,304 | 14,804 | 14,719 | +0.1% |
