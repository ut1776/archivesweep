# Bounded-window results

`max_workers=4`, window = `2 * max_workers` = 8

| files | strategy | peak futures in flight | peak traced MB | seconds | groups | issues |
|---:|---|---:|---:|---:|---:|---:|
| 2000 | upfront | 2000 | 4.4 | 0.27 | 1 | 0 |
| 2000 | bounded | 8 | 1.6 | 0.28 | 1 | 0 |
| 10000 | upfront | 10000 | 21.1 | 1.42 | 1 | 0 |
| 10000 | bounded | 8 | 3.8 | 1.39 | 1 | 0 |
| 30000 | upfront | 30000 | 65.9 | 4.40 | 1 | 0 |
| 30000 | bounded | 8 | 12.0 | 3.98 | 1 | 0 |

- barrier of 1 blocked sample hooks releases: **True**
- barrier of 2 blocked sample hooks releases: **True**
- barrier of 4 blocked sample hooks releases: **True**
- barrier of 8 blocked sample hooks releases: **True**
