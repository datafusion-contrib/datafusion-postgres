# ParameterStatus Compatibility Notes

Analysis of pgwire 0.38 startup behavior vs our SET handler, cross-referenced with PostgreSQL wire protocol.

## Wire Order (Confirmed Correct)

```
Client: SET datestyle = 'ISO, MDY'
Server: ParameterStatus("DateStyle", "ISO, MDY")  ← our feed()
Server: CommandComplete("SET")                      ← framework send()
Server: ReadyForQuery(Idle)                         ← framework auto
```

`feed()` buffers; framework's `send(CommandComplete)` flushes both. Matches vanilla PG.

## Name Casing (Fixed)

pgwire startup sends `DateStyle`, `IntervalStyle`, `TimeZone` (mixed case). Our SET handler now matches:

| Metadata key | ParameterStatus name | PG canonical |
|---|---|---|
| `datestyle` | `DateStyle` | `DateStyle` |
| `intervalstyle` | `IntervalStyle` | `IntervalStyle` |
| `bytea_output` | `bytea_output` | `bytea_output` |
| `application_name` | `application_name` | `application_name` |
| `extra_float_digits` | `extra_float_digits` | `extra_float_digits` |
| `search_path` | `search_path` | `search_path` |
| (timezone) | `TimeZone` | `TimeZone` |

## Known Gaps (Future Work)

### `RESET <var>` not handled
PG: `RESET datestyle` restores default and sends `ParameterStatus("DateStyle", "ISO, MDY")`.
Us: sqlparser parses standalone `RESET` differently from `SET`. Falls through to datafusion, no ParameterStatus sent. Clients cache stale value.

### `SET <var> TO DEFAULT` not handled
PG: equivalent to `RESET`. Parsed as `SingleAssignment` with value `DEFAULT`.
Us: handler only matches `Expr::Value(...)` — bare `DEFAULT` keyword skips the metadata update.

### `SET LOCAL` silently ignored
PG: `SET LOCAL datestyle = 'ISO'` is transaction-scoped, still sends ParameterStatus.
Us: handler matches only `scope: None`, so `SET LOCAL` falls through without ParameterStatus.

### `SET timezone = 'UTC'` via SingleAssignment
PG: `SET timezone = 'UTC'` and `SET TIME ZONE 'UTC'` are equivalent.
Us: `SET TIME ZONE` hits the `SetTimeZone` arm and calls `client::set_timezone()`. But `SET timezone = 'UTC'` hits `SingleAssignment`, stores in metadata without calling `client::set_timezone()`. The ParameterStatus helper then reads `client::get_timezone()` which returns the old value.

### `standard_conforming_strings` missing
pgwire sends this during startup. If a client does `SET standard_conforming_strings = 'on'`, we store it in metadata but don't send ParameterStatus (not in our match list).

## Reference

- pgwire startup ParameterStatus: `src/api/auth/mod.rs` lines 308-316
- pgwire `DefaultServerParameterProvider`: `src/api/auth/mod.rs` lines 64-185
- pgwire SimpleQueryHandler flow: `src/api/query.rs` lines 57-136
- pgwire ExtendedQueryHandler flow: `src/api/query.rs` lines 240-319
