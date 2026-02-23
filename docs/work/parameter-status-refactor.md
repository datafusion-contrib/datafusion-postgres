# ParameterStatus Refactor: Moving Logic Into QueryHook

**Context:** PR #293 review feedback from @sunng87 — move ParameterStatus sending
from `handlers.rs` into `set_show.rs`'s `try_respond_set_statements`.

**Problem:** Currently `handlers.rs:150-162` and `handlers.rs:254-266` contain
ParameterStatus logic that conceptually belongs in the SET/SHOW hook.

---

## Approach A: Make QueryHook Generic (Full Sink Access)

Change `QueryHook` trait methods from `dyn ClientInfo` to generic `C: ClientInfo + Sink<PgWireBackendMessage>`.

**Changes:**
- `QueryHook` trait: generic `C` param on each method
- `try_respond_set_statements`: add `Sink` bound, call `client.feed(ParameterStatus)` directly
- Remove `parameter_status_key_for_set` entirely

**Pros:**
- Hook has full control — sends ParameterStatus itself
- Clean separation: handlers don't know about SET semantics at all

**Cons:**
- **Breaks object safety** — `Vec<Arc<dyn QueryHook>>` no longer compiles
- Requires refactoring all hook storage to enum dispatch or generics
- Touches `DfSessionService`, `DfSessionServiceInner`, `QueryParser` impl, `lib.rs`
- All 3 hook impls (SetShow, Transactions, Permissions) must change signature
- Large blast radius for what the reviewer asked

**Verdict:** Too invasive. Save for a future refactor if more hooks need Sink access.

---

## Approach B: Return ParameterStatus Data From Hook (Chosen)

Have `try_respond_set_statements` compute and return the ParameterStatus key/value
alongside the Response. The handler sends it.

**Changes:**
1. `try_respond_set_statements` → returns `Option<PgWireResult<(Response, Option<(String, String)>)>>`
2. Inline `parameter_status_key_for_set` logic into `try_respond_set_statements`
3. `handle_simple_query` / `handle_extended_query` in `SetShowHook` propagate the tuple
4. `QueryHook` trait: change return type to include optional ParameterStatus data
5. `handlers.rs`: read returned tuple, send ParameterStatus if present
6. Remove standalone `parameter_status_key_for_set` function

**Pros:**
- All SET→ParameterStatus logic lives in `set_show.rs`
- Object safety preserved — `Vec<Arc<dyn QueryHook>>` still works
- Handler just forwards what the hook returns
- Minimal blast radius

**Cons:**
- Handler still does the actual `client.feed()` call (unavoidable without Sink on trait)
- Return type is slightly more complex

**Verdict:** Best balance of reviewer's intent vs. minimal churn.

---

## Test Impact

Existing tests:
- `handlers.rs` integration tests: `test_set_sends_parameter_status`, `test_set_statement_timeout_no_parameter_status` — verify ParameterStatus via MockClient Sink. **Still valid after refactor.**
- `set_show.rs` unit tests: `test_parameter_status_key_for_all_set_vars`, `test_no_parameter_status_for_statement_timeout` — test `parameter_status_key_for_set`. **Must update** since that function is removed; logic moves into `try_respond_set_statements` return value.

New/updated tests needed:
- Update `set_show.rs` unit tests to check the `Option<(String, String)>` in the return tuple from `try_respond_set_statements`
- Integration tests in `handlers.rs` remain unchanged (they test end-to-end behavior)
