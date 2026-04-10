# gemstools

`gemstools` is an R package for reading MachineLab GEMS output files and turning them into analysis-ready tables for RGA, ADV, and status data.

## How The Library Works

The main processing path is:

1. `gems_read_file()` reads each raw text file into a table of line number, record type, and payload.
2. `gems_parse_file()` splits the file into separate streams by record type:
   - `R` records -> `gems_rga_data()`
   - `D` records -> `gems_adv_data()`
   - `S` records -> `gems_status_data()`
3. If `clean = TRUE`, the package applies `gems_clean_rga()`, `gems_clean_adv_data()`, and `gems_clean_status()` to remove values outside expected ranges.
4. ADV timestamps are then reconstructed from status records with `make_gems_ts()`, using the corrected ADV clock from `correct_status_timestamp_adv()`.
5. `gems_parse_file()` returns a list with final `rga`, `status`, and `adv_data` tables. `gems_parse_files()` applies that workflow across many files and row-binds the results.
6. `gems_process_data()` is the top-level batch wrapper. It finds files, parses them in parallel, optionally deduplicates or resamples ADV data, and writes CSV or Parquet outputs.

## Output Tables

`gems_parse_file()` returns three main data frames:

- `rga`: `timestamp`, `mass`, `current`, `pressure`
- `status`: `timestamp`, `adv_timestamp`, `bat`, `soundspeed`, `heading`, `pitch`, `roll`, `temp`
- `adv_data`: `timestamp`, `pressure`, `u`, `v`, `w`, `amp1`, `amp2`, `amp3`, `corr1`, `corr2`, `corr3`

These are the columns selected for downstream use after parsing and timestamp alignment.

## Engineering Unit Conversions

The package converts selected integer fields to engineering units during parsing.

### ADV

`gems_adv_data()` parses raw ADV `D` records and applies these conversions:

- `u`, `v`, and `w` velocity counts are multiplied by `0.0001`, so output velocity is in `m/s`.
- `pressure` is parsed as numeric and divided by `1000`. The resulting values are treated as engineering-unit pressure and used as decibar-scale pressure in the rest of the package.
- `amp1`, `amp2`, `amp3`, `corr1`, `corr2`, and `corr3` are not rescaled and remain in raw instrument units.

### Status

`gems_status_data()` converts status integers as follows:

- `bat`, `soundspeed`, `heading`, `pitch`, and `roll` are multiplied by `0.1`
- `temp` is multiplied by `0.01`
- `adv_timestamp` is reconstructed from the ADV date and time fields

### RGA

`gems_rga_data()` converts RGA values as follows:

- `current` is scaled by `1e-16`
- `pressure` is derived from `current / 0.0801`

## Example

```r
files <- list.files("path/to/gems/files", full.names = TRUE)

out <- gems_parse_files(files, clean = TRUE)

adv <- out$adv_data
status <- out$status
rga <- out$rga
```

For a single-file workflow:

```r
out <- gems_parse_file("path/to/gems_2025....txt", clean = TRUE)
```
