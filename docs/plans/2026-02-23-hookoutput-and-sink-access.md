# HookOutput Design & Sink Access for QueryHook

**Date:** 2026-02-23
**Status:** Plan (not yet implemented)
**Context:** PR #293 review — @sunng87 suggested moving ParameterStatus sending into
`set_show.rs`'s `try_respond_set_statements`, and noted "We can modify the definition
of client in QueryHook if needed."

---

## Current State (Approach B — implemented)

Hook returns `(Response, Option<(String, String)>)`. Handler calls `client.feed(ParameterStatus)`.

- All SET→ParameterStatus mapping logic lives in `set_show.rs`
- `handlers.rs` does the mechanical `client.feed()` (5 lines, in both `do_query` impls)
- Object safety preserved — `Vec<Arc<dyn QueryHook>>` works
- Follows Tower/Axum return-value pattern

---

## Problem: Public API Shape

`HookOutput` is a public type alias:

```rust
pub type ParameterStatusChange = Option<(String, String)>;
pub type HookOutput = (Response, ParameterStatusChange);
```

Exported via `pub mod hooks` in `lib.rs`. Anyone implementing `QueryHook` must
return this type. Current issues:

1. **Bare tuple** — positional fields (`.0`, `.1`) are unclear
2. **Adding fields later breaks the API** — e.g., adding notices requires changing the tuple
3. **Single ParameterStatus only** — `Option` limits to one; real PG can send multiple
4. **No path to notices/warnings** — `log::warn!()` calls in hooks should be client-visible

---

## Approaches Considered

### 1. Full Generics on QueryHook (Doc Approach A)

Make methods generic: `C: ClientInfo + Sink<PgWireBackendMessage>`.

- Breaks object safety — `dyn QueryHook` no longer compiles
- Requires enum dispatch or threading generics through DfSessionService, Parser, lib.rs
- Verdict: **Too invasive.** Save for major version bump if ever needed.

### 2. Supertrait `dyn HookClient` (Wrapper)

New trait `HookClient: ClientInfo` with `send_parameter_status()`, `send_notice()`.
Wrapper in handlers.rs delegates all ClientInfo methods + adds Sink-backed writes.

- Object-safe
- Requires delegating 12+ ClientInfo methods (including feature-gated `sni_server_name`,
  `client_certificates`) — fragile, breaks when pgwire adds methods
- Verdict: **Too much boilerplate for marginal gain.**

### 3. Extra Writer Parameter

Add `writer: &mut dyn HookWriter` to QueryHook methods alongside existing `dyn ClientInfo`.

- Object-safe, no delegation
- Clean separation: ClientInfo for reads, HookWriter for writes
- Most hooks ignore `writer` (only SetShowHook uses it)
- Verdict: **Viable but changes every hook method signature for one hook's benefit.**

### 4. `#[non_exhaustive]` Struct HookOutput (Recommended — Phase 1)

Replace tuple with a struct. Keep return-value pattern. Evolve later if needed.

- Zero architectural change — same data flow
- Named fields, self-documenting
- `#[non_exhaustive]` prevents struct-literal construction → adding fields is non-breaking
- Builder pattern for ergonomic construction
- Verdict: **Smallest change, best future-proofing.**

### 5. `#[non_exhaustive]` Struct + Writer Parameter (Phase 2, if needed)

Combine approach 3 + 4. Add writer param only when a second hook actually needs write
access.

---

## Recommended Plan

### Phase 1: Struct HookOutput (do now)

Replace the tuple type alias with a proper struct:

```rust
#[non_exhaustive]
pub struct HookOutput {
    pub response: Response,
    pub parameter_status: Vec<(String, String)>,
}

impl HookOutput {
    pub fn new(response: Response) -> Self {
        HookOutput {
            response,
            parameter_status: Vec::new(),
        }
    }

    pub fn with_parameter_status(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.parameter_status.push((name.into(), value.into()));
        self
    }
}
```

**Changes required:**

| File | Change | Effort |
|---|---|---|
| `hooks/mod.rs` | Replace `ParameterStatusChange` + `HookOutput` type aliases with struct + impl | Small |
| `hooks/set_show.rs` | `(Response::Execution(tag), Some((k, v)))` → `HookOutput::new(resp).with_parameter_status(k, v)` | Small |
| `hooks/set_show.rs` | `(resp, None)` → `HookOutput::new(resp)` | Trivial |
| `hooks/transactions.rs` | `(r, None)` → `HookOutput::new(r)` | Trivial |
| `hooks/permissions.rs` | No change (returns `None` or `Err`, never constructs HookOutput) | None |
| `handlers.rs` | `let (response, ps_change) = result?` → `let output = result?; let response = output.response;` + iterate `output.parameter_status` | Small |
| Tests in `set_show.rs` | Destructure struct instead of tuple | Small |
| Tests in `handlers.rs` | No change (test end-to-end via MockClient Sink) | None |

**What this enables without future API breaks:**

```rust
// Phase 1 (now): ParameterStatus
HookOutput::new(response).with_parameter_status("TimeZone", "UTC")

// Future: add notices (non-breaking — new field + builder method)
HookOutput::new(response)
    .with_parameter_status("TimeZone", "UTC")
    .with_notice(Severity::Warning, "deprecated parameter name")

// Future: multiple ParameterStatus (already supported — Vec)
HookOutput::new(response)
    .with_parameter_status("DateStyle", "ISO, MDY")
    .with_parameter_status("datestyle", "ISO, MDY")
```

### Phase 2: Notices (future, when needed)

Add `notices` field to the struct:

```rust
#[non_exhaustive]
pub struct HookOutput {
    pub response: Response,
    pub parameter_status: Vec<(String, String)>,
    pub notices: Vec<Notice>,              // new — non-breaking addition
}
```

Handler interprets: sends `NoticeResponse` for each entry, then `ParameterStatus`, then `Response`.

Replaces current `log::warn!()` calls in:
- `set_show.rs:222` — unsupported SET fallback
- `transactions.rs:107` — nested BEGIN warning

### Phase 3: Writer Parameter (future, if multiple hooks need Sink)

Only if a second hook (beyond SetShowHook) genuinely needs direct write access.
Add `writer: &mut dyn HookWriter` to trait methods. Not needed now — YAGNI.

---

## Packet Ordering

PostgreSQL sends packets in this order for `SET timezone = 'UTC'`:

```
← ParameterStatus (name=TimeZone, value=UTC)
← CommandComplete (SET)
← ReadyForQuery
```

Current implementation sends ParameterStatus before adding Response to results,
which matches this order. The struct approach preserves this — handler sends
all `output.parameter_status` entries first, then processes `output.response`.

**TODO:** Verify with a packet capture against real PostgreSQL for edge cases
(multiple ParameterStatus, SET with notices).

---

## Responding to Reviewer

Suggested reply to @sunng87:

> All SET→ParameterStatus logic now lives in `set_show.rs`. The handler forwards
> the returned data via `client.feed()`.
>
> Making hooks write directly would require either breaking object safety
> (`Vec<Arc<dyn QueryHook>>`) or a 12-method ClientInfo delegation wrapper.
> The return-value pattern matches how Tower/Axum handle middleware I/O.
>
> I've replaced the bare tuple with a `#[non_exhaustive]` struct so future
> additions (notices, multiple ParameterStatus) are non-breaking.

---

## References

- `docs/work/parameter-status-refactor.md` — original Approach A vs B analysis
- Tower `Service` trait — return-value pattern precedent
- Zed gpui `Context::emit` — effect-queue pattern precedent
- pgwire `ClientInfo` — 12+ methods (see `pgwire/src/api/mod.rs:44-74`)
- pgwire `PgWireBackendMessage` — ParameterStatus, NoticeResponse, NotificationResponse
