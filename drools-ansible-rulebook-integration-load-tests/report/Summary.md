# Load test summary: match / unmatch (2.0.1 - 2.0.4)

Source: `<version>/result_match_unmatch_noHA.txt` and `<version>/result_match_unmatch_HA-PG.txt`.

- Memory = peak memory (MB, 1 MB = 1,048,576 bytes), Time = elapsed time (ms). Raw values are in the result files.
- `2.0.3` is the result of tag `2.0.3-beta1`; `2.0.4` is the result of tag `2.0.4-beta1`.
- noHA: 1k/10k/100k/1m events. HA-PG: 1k/5k/10k events (capped because HA-PG is slow).
- Change = 2.0.4 vs 2.0.1.
- Each value is a single run. Time varies by roughly +-15-25% between runs of the same code for small sizes (e.g. 2.0.4 was run twice), so small differences are not significant.

## noHA

### noHA - Memory (MB)

| Scenario | Events | 2.0.1 | 2.0.2 | 2.0.3 | 2.0.4 | Change |
|---|---|---:|---:|---:|---:|---:|
| match | 1k | 6.34 | 6.34 | 6.33 | 6.33 | -0.1% |
| match | 10k | 6.37 | 6.37 | 6.36 | 6.36 | -0.1% |
| match | 100k | 6.37 | 6.37 | 6.36 | 6.36 | -0.1% |
| match | 1m | 6.43 | 6.41 | 6.38 | 6.39 | -0.6% |
| unmatch | 1k | 6.30 | 6.30 | 6.29 | 6.30 | -0.1% |
| unmatch | 10k | 6.33 | 6.32 | 6.32 | 6.32 | -0.1% |
| unmatch | 100k | 6.33 | 6.33 | 6.32 | 6.32 | -0.1% |
| unmatch | 1m | 6.38 | 6.37 | 6.34 | 6.34 | -0.7% |

### noHA - Time (ms)

| Scenario | Events | 2.0.1 | 2.0.2 | 2.0.3 | 2.0.4 | Change |
|---|---|---:|---:|---:|---:|---:|
| match | 1k | 213 | 191 | 196 | 242 | +13.6% |
| match | 10k | 901 | 849 | 844 | 1,162 | +29.0% |
| match | 100k | 5,471 | 5,296 | 5,287 | 5,600 | +2.4% |
| match | 1m | 54,177 | 51,386 | 49,579 | 51,584 | -4.8% |
| unmatch | 1k | 133 | 120 | 135 | 125 | -6.0% |
| unmatch | 10k | 479 | 492 | 467 | 453 | -5.4% |
| unmatch | 100k | 2,132 | 2,127 | 2,144 | 2,031 | -4.7% |
| unmatch | 1m | 17,582 | 17,547 | 17,915 | 17,440 | -0.8% |

## HA-PG

### HA-PG - Memory (MB)

| Scenario | Events | 2.0.1 | 2.0.2 | 2.0.3 | 2.0.4 | Change |
|---|---|---:|---:|---:|---:|---:|
| match | 1k | 7.78 | 7.75 | 7.74 | 7.78 | -0.1% |
| match | 5k | 7.79 | 7.78 | 7.85 | 7.77 | -0.3% |
| match | 10k | 7.84 | 7.82 | 7.86 | 7.83 | -0.1% |
| unmatch | 1k | 7.76 | 7.79 | 7.72 | 7.74 | -0.3% |
| unmatch | 5k | 7.80 | 7.75 | 7.80 | 7.77 | -0.4% |
| unmatch | 10k | 7.81 | 7.76 | 7.77 | 7.77 | -0.5% |

### HA-PG - Time (ms)

| Scenario | Events | 2.0.1 | 2.0.2 | 2.0.3 | 2.0.4 | Change |
|---|---|---:|---:|---:|---:|---:|
| match | 1k | 12,161 | 8,022 | 12,429 | 12,010 | -1.2% |
| match | 5k | 54,957 | 49,382 | 55,153 | 54,590 | -0.7% |
| match | 10k | 107,749 | 108,749 | 107,478 | 108,056 | +0.3% |
| unmatch | 1k | 11,366 | 11,183 | 10,515 | 8,524 | -25.0% |
| unmatch | 5k | 51,154 | 50,271 | 50,786 | 40,895 | -20.1% |
| unmatch | 10k | 101,732 | 88,373 | 102,010 | 102,865 | +1.1% |
