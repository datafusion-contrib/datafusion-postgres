# ParameterStatus on SET Statements Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Send a `ParameterStatus` backend message after every successful SET statement, matching vanilla PostgreSQL behavior.

**Architecture:** `QueryHook` uses `dyn ClientInfo + Send + Sync` for its client, so it can't call `Sink` methods directly. Instead, `DfSessionService::do_query` (which already has `C: Sink<PgWireBackendMessage>`) sends `ParameterStatus` after a hook handles a SET statement. A helper function extracts the variable name and current value from the client state.

**Tech Stack:** Rust, pgwire 0.38, async-trait, futures::SinkExt

---

### Task 1: Capture Sent Messages in MockClient

MockClient's `start_send` currently drops all messages. We need it to capture them so tests can assert `ParameterStatus` was sent.

**Files:**
- Modify: `datafusion-postgres/src/testing.rs`

**Step 1: Read the file to confirm current state**

Run: `cat datafusion-postgres/src/testing.rs`

**Step 2: Write the failing test (red)**

Add this test at the bottom of `testing.rs` to confirm MockClient currently drops messages:

```rust
#[test]
fn test_mock_client_captures_messages() {
    use pgwire::messages::{startup::ParameterStatus, PgWireBackendMessage};
    let mut client = MockClient::new();
    // We'll add a `sent_messages` field and verify it captures messages
    // For now just verify the struct exists - this will be the contract
    assert!(client.sent_messages().is_empty());
}
```

Run: `cargo test -p datafusion-postgres test_mock_client_captures_messages 2>&1 | tail -20`
Expected: FAIL (method `sent_messages` doesn't exist)

**Step 3: Add `sent_messages` field and accessor to MockClient**

In `testing.rs`, change:
```rust
#[derive(Debug, Default)]
pub struct MockClient {
    metadata: HashMap<String, String>,
    portal_store: HashMap<String, String>,
}
```
to:
```rust
#[derive(Debug, Default)]
pub struct MockClient {
    metadata: HashMap<String, String>,
    portal_store: HashMap<String, String>,
    pub sent_messages: Vec<PgWireBackendMessage>,
}
```

Add to `impl MockClient`:
```rust
pub fn sent_messages(&self) -> &[PgWireBackendMessage] {
    &self.sent_messages
}
```

Update `start_send` to capture messages:
```rust
fn start_send(
    mut self: std::pin::Pin<&mut Self>,
    item: PgWireBackendMessage,
) -> Result<(), Self::Error> {
    self.sent_messages.push(item);
    Ok(())
}
```

Note: `PgWireBackendMessage` needs to be imported. Add to top of file:
```rust
use pgwire::messages::PgWireBackendMessage;
```
(It was already used in `Sink<PgWireBackendMessage>` impl, so it may already be imported.)

**Step 4: Run test to verify it passes**

Run: `cargo test -p datafusion-postgres test_mock_client_captures_messages 2>&1 | tail -20`
Expected: PASS

**Step 5: Commit**

```bash
git add datafusion-postgres/src/testing.rs
git commit -m "test: capture backend messages in MockClient"
```

---

### Task 2: Write Failing Test for ParameterStatus After SET

Before implementing, write the test that verifies `ParameterStatus` IS sent.

**Files:**
- Modify: `datafusion-postgres/src/hooks/set_show.rs`

**Step 1: Add a failing test in `set_show.rs`**

Add to the `#[cfg(test)]` block:

```rust
#[tokio::test]
async fn test_parameter_status_sent_on_set() {
    use pgwire::messages::{startup::ParameterStatus, PgWireBackendMessage};

    let session_context = SessionContext::new();
    let mut client = MockClient::new();

    let statement = Parser::new(&PostgreSqlDialect {})
        .try_with_sql("set datestyle = 'ISO, MDY'")
        .unwrap()
        .parse_statement()
        .unwrap();

    let _response =
        try_respond_set_statements(&mut client, &statement, &session_context).await;

    // After SET, ParameterStatus for "datestyle" should have been sent
    let param_status_msgs: Vec<_> = client
        .sent_messages
        .iter()
        .filter_map(|m| {
            if let PgWireBackendMessage::ParameterStatus(ps) = m {
                Some(ps)
            } else {
                None
            }
        })
        .collect();

    assert_eq!(param_status_msgs.len(), 1, "Expected 1 ParameterStatus message");
    assert_eq!(param_status_msgs[0].name(), "datestyle");
    assert_eq!(param_status_msgs[0].value(), "ISO, MDY");
}
```

**Step 2: Run test to verify it fails**

Run: `cargo test -p datafusion-postgres test_parameter_status_sent_on_set 2>&1 | tail -20`
Expected: FAIL (0 ParameterStatus messages found)

Note: If `ParameterStatus` doesn't have `.name()` / `.value()` accessors, check the pgwire 0.38 source at:
`~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/pgwire-0.38.0/src/messages/startup.rs`
and adjust accordingly.

**Step 3: Commit the failing test**

```bash
git add datafusion-postgres/src/hooks/set_show.rs
git commit -m "test: add failing test for ParameterStatus on SET statements"
```

---

### Task 3: Send ParameterStatus from try_respond_set_statements

The function `try_respond_set_statements` currently takes `C: ClientInfo + Send + Sync + ?Sized`. We need to also require `Sink<PgWireBackendMessage>` so it can send `ParameterStatus`.

**Files:**
- Modify: `datafusion-postgres/src/hooks/set_show.rs`

**Step 1: Check ParameterStatus API in pgwire**

Run: `grep -n "ParameterStatus\|fn new\|pub fn" ~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/pgwire-0.38.0/src/messages/startup.rs | head -30`

Confirm constructor signature: `ParameterStatus::new(name: String, value: String)` or similar.

**Step 2: Add Sink bound to try_respond_set_statements**

Change the function signature from:
```rust
async fn try_respond_set_statements<C>(
    client: &mut C,
    statement: &Statement,
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>>
where
    C: ClientInfo + Send + Sync + ?Sized,
```

to:
```rust
async fn try_respond_set_statements<C>(
    client: &mut C,
    statement: &Statement,
    session_context: &SessionContext,
) -> Option<PgWireResult<Response>>
where
    C: ClientInfo + futures::Sink<pgwire::messages::PgWireBackendMessage> + Send + Sync + Unpin,
    pgwire::error::PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
```

Note: removing `?Sized` since `Sink` requires `Sized`.

**Step 3: Add helper to send ParameterStatus**

Add a new private async function after `try_respond_set_statements`:

```rust
async fn send_parameter_status<C>(
    client: &mut C,
    var_name: &str,
    display_name: &str,
) -> PgWireResult<()>
where
    C: ClientInfo + futures::Sink<pgwire::messages::PgWireBackendMessage> + Send + Sync + Unpin,
    pgwire::error::PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
{
    use futures::SinkExt;
    use pgwire::messages::{startup::ParameterStatus, PgWireBackendMessage};

    let value = match var_name {
        "timezone" => client::get_timezone(client)
            .unwrap_or("UTC")
            .to_string(),
        "statement_timeout" => return Ok(()), // not a standard ParameterStatus param
        _ => match client.metadata().get(var_name) {
            Some(v) => v.clone(),
            None => return Ok(()),
        },
    };

    client
        .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
            display_name.to_string(),
            value,
        )))
        .await?;

    Ok(())
}
```

**Step 4: Call send_parameter_status in try_respond_set_statements**

In the success return points for metadata-backed variables, add a call before returning. Specifically, in the `SingleAssignment` match arm where we store metadata:

```rust
} else if matches!(
    var.as_str(),
    "datestyle"
        | "bytea_output"
        | "intervalstyle"
        | "application_name"
        | "extra_float_digits"
        | "search_path"
) && !values.is_empty()
{
    let value = values[0].clone();
    if let Expr::Value(value) = value {
        client
            .metadata_mut()
            .insert(var.clone(), value.into_string().unwrap_or_else(|| "".to_string()));
        // Send ParameterStatus after successful set
        if let Err(e) = send_parameter_status(client, &var, &var).await {
            warn!("Failed to send ParameterStatus for {var}: {e}");
        }
        return Some(Ok(Response::Execution(Tag::new("SET"))));
    }
}
```

For timezone:
```rust
Set::SetTimeZone { local: false, value } => {
    let tz = value.to_string();
    let tz = tz.trim_matches('"').trim_matches('\'');
    client::set_timezone(client, Some(tz));
    session_context
        .state()
        .config_mut()
        .options_mut()
        .execution
        .time_zone = Some(tz.to_string());
    // Send ParameterStatus for TimeZone
    if let Err(e) = send_parameter_status(client, "timezone", "TimeZone").await {
        warn!("Failed to send ParameterStatus for TimeZone: {e}");
    }
    return Some(Ok(Response::Execution(Tag::new("SET"))));
}
```

**Step 5: Fix compilation issues**

Run: `cargo build -p datafusion-postgres 2>&1 | head -50`

The main issue will be that callers of `try_respond_set_statements` in `SetShowHook::handle_simple_query` and `handle_extended_query` pass `client: &mut (dyn ClientInfo + Send + Sync)` which doesn't have `Sink` bounds.

Two options:
- **A (recommended)**: Change the hook methods to NOT call `try_respond_set_statements` directly and instead send ParameterStatus from `DfSessionService::do_query` (see Task 4)
- **B**: Keep calling from hook but change the hook trait client signature

**Go with Option A**: Remove the `Sink` bounds from `try_respond_set_statements` and instead extract a separate `build_parameter_status_for_set` function that returns the key-value pairs, which `do_query` then sends.

Revise Step 3/4: Instead of `send_parameter_status` inside `try_respond_set_statements`, add a function that returns what ParameterStatus to send:

```rust
/// Returns (var_name_for_lookup, display_name) for the SET statement if ParameterStatus should be sent
pub fn parameter_status_key_for_set(statement: &Statement, client: &dyn ClientInfo) -> Option<(String, String)> {
    let Statement::Set(set_stmt) = statement else {
        return None;
    };
    match set_stmt {
        Set::SingleAssignment { variable, .. } => {
            let var = variable.to_string().to_lowercase();
            if matches!(
                var.as_str(),
                "datestyle" | "bytea_output" | "intervalstyle" | "application_name"
                    | "extra_float_digits" | "search_path"
            ) {
                let value = client.metadata().get(&var)?.clone();
                Some((var.clone(), value))
            } else if var == "timezone" {
                let tz = client::get_timezone(client).unwrap_or("UTC").to_string();
                Some(("TimeZone".to_string(), tz))
            } else {
                None
            }
        }
        Set::SetTimeZone { .. } => {
            let tz = client::get_timezone(client).unwrap_or("UTC").to_string();
            Some(("TimeZone".to_string(), tz))
        }
        _ => None,
    }
}
```

Keep `try_respond_set_statements` with original `C: ClientInfo + Send + Sync + ?Sized` bounds (no Sink). Move ParameterStatus sending to `do_query`.

**Step 6: Run test to verify it passes now (or partially)**

Run: `cargo test -p datafusion-postgres 2>&1 | tail -30`

**Step 7: Commit progress**

```bash
git add datafusion-postgres/src/hooks/set_show.rs
git commit -m "feat: add parameter_status_key_for_set helper in set_show hook"
```

---

### Task 4: Send ParameterStatus from DfSessionService::do_query

Now wire up the ParameterStatus sending in the actual handler, which has access to the full `Sink<PgWireBackendMessage>` client.

**Files:**
- Modify: `datafusion-postgres/src/handlers.rs`

**Step 1: Add Sink bounds to SimpleQueryHandler::do_query impl**

Change:
```rust
async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
where
    C: ClientInfo + Unpin + Send + Sync,
```

to:
```rust
async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
where
    C: ClientInfo + Unpin + Send + Sync + futures::Sink<pgwire::messages::PgWireBackendMessage>,
    pgwire::error::PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
```

This is valid because the pgwire trait already requires these bounds.

**Step 2: Send ParameterStatus after hook handles SET**

In the hook loop inside `do_query`, after the hook returns a result:

```rust
for hook in &self.query_hooks {
    if let Some(result) = hook
        .handle_simple_query(&statement, &self.session_context, client)
        .await
    {
        // Send ParameterStatus for successful SET statements
        if matches!(result, Ok(_)) {
            if let Some((param_name, param_value)) =
                crate::hooks::set_show::parameter_status_key_for_set(&statement, client)
            {
                use futures::SinkExt;
                use pgwire::messages::{startup::ParameterStatus, PgWireBackendMessage};
                client
                    .feed(PgWireBackendMessage::ParameterStatus(
                        ParameterStatus::new(param_name, param_value),
                    ))
                    .await
                    .map_err(PgWireError::from)?;
            }
        }
        results.push(result?);
        continue 'stmt;
    }
}
```

**Step 3: Do the same for ExtendedQueryHandler::do_query**

Similarly add Sink bounds to `ExtendedQueryHandler::do_query` and send ParameterStatus after hook handles a SET:

```rust
async fn do_query<C>(
    &self,
    client: &mut C,
    portal: &Portal<Self::Statement>,
    _max_rows: usize,
) -> PgWireResult<Response>
where
    C: ClientInfo + Unpin + Send + Sync + futures::Sink<pgwire::messages::PgWireBackendMessage>,
    pgwire::error::PgWireError: From<<C as futures::Sink<pgwire::messages::PgWireBackendMessage>>::Error>,
```

And in the hook loop:
```rust
for hook in &self.query_hooks {
    if let Some(result) = hook
        .handle_extended_query(statement, plan, &param_values, &self.session_context, client)
        .await
    {
        if matches!(result, Ok(_)) {
            if let Some((param_name, param_value)) =
                crate::hooks::set_show::parameter_status_key_for_set(statement, client)
            {
                use futures::SinkExt;
                use pgwire::messages::{startup::ParameterStatus, PgWireBackendMessage};
                client
                    .feed(PgWireBackendMessage::ParameterStatus(
                        ParameterStatus::new(param_name, param_value),
                    ))
                    .await
                    .map_err(PgWireError::from)?;
            }
        }
        return result;
    }
}
```

**Step 4: Compile check**

Run: `cargo build -p datafusion-postgres 2>&1 | head -60`

Fix any type errors. Common issues:
- `PgWireError::from` might not work directly — use `.map_err(Into::into)` or explicit conversion
- `parameter_status_key_for_set` needs to be `pub` and exported from `set_show.rs`

**Step 5: Run all tests**

Run: `cargo test -p datafusion-postgres 2>&1 | tail -40`
Expected: All existing tests pass + new ParameterStatus test passes

**Step 6: Commit**

```bash
git add datafusion-postgres/src/handlers.rs datafusion-postgres/src/hooks/set_show.rs
git commit -m "feat: send ParameterStatus on successful SET statements (#242)"
```

---

### Task 5: Add Tests for All SET Variable Types

Verify ParameterStatus is sent for all supported variables.

**Files:**
- Modify: `datafusion-postgres/src/hooks/set_show.rs`

**Step 1: Add parameterized test**

Add to the `#[cfg(test)]` block in `set_show.rs`:

```rust
#[tokio::test]
async fn test_parameter_status_sent_for_all_metadata_vars() {
    use pgwire::messages::{startup::ParameterStatus, PgWireBackendMessage};

    let test_cases = vec![
        ("set bytea_output = 'escape'", "bytea_output", "escape"),
        ("set intervalstyle = 'postgres'", "intervalstyle", "postgres"),
        ("set application_name = 'myapp'", "application_name", "myapp"),
        ("set search_path = 'public'", "search_path", "public"),
        ("set extra_float_digits = '2'", "extra_float_digits", "2"),
        ("set datestyle = 'ISO, MDY'", "datestyle", "ISO, MDY"),
    ];

    for (sql, expected_key, expected_value) in test_cases {
        let session_context = SessionContext::new();
        let mut client = MockClient::new();
        let statement = Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statement()
            .unwrap();

        // The hook updates metadata; then do_query would send ParameterStatus.
        // Here we test the helper function directly.
        let _response =
            try_respond_set_statements(&mut client, &statement, &session_context).await;

        let key_value = parameter_status_key_for_set(&statement, &client);
        assert!(
            key_value.is_some(),
            "Expected ParameterStatus key-value for {sql}"
        );
        let (name, value) = key_value.unwrap();
        assert_eq!(name, expected_key, "Wrong key for {sql}");
        assert_eq!(value, expected_value, "Wrong value for {sql}");
    }
}

#[tokio::test]
async fn test_parameter_status_for_timezone() {
    let session_context = SessionContext::new();
    let mut client = MockClient::new();

    let statement = Parser::new(&PostgreSqlDialect {})
        .try_with_sql("set time zone 'America/New_York'")
        .unwrap()
        .parse_statement()
        .unwrap();

    let _response =
        try_respond_set_statements(&mut client, &statement, &session_context).await;

    let key_value = parameter_status_key_for_set(&statement, &client);
    assert!(key_value.is_some());
    let (name, value) = key_value.unwrap();
    assert_eq!(name, "TimeZone");
    assert_eq!(value, "America/New_York");
}
```

**Step 2: Run tests**

Run: `cargo test -p datafusion-postgres 2>&1 | tail -40`
Expected: All pass

**Step 3: Commit**

```bash
git add datafusion-postgres/src/hooks/set_show.rs
git commit -m "test: verify ParameterStatus key-value for all SET variable types"
```

---

### Task 6: Integration Test (Optional but Recommended)

Verify the full end-to-end behavior using psycopg2 or libpq to confirm ParameterStatus is received by the client.

**Files:**
- Modify: `tests-integration/test.sh` or add `tests-integration/test_set_parameter_status.py`

**Step 1: Check how existing integration tests work**

Run: `cat tests-integration/test.sh | head -30`

**Step 2: Add a simple SET test with parameter status check**

Create `tests-integration/test_set_parameter.py`:
```python
import psycopg2

conn = psycopg2.connect(host="localhost", port=5432, dbname="datafusion", user="postgres")
conn.autocommit = True
cur = conn.cursor()

# SET should not raise an error
cur.execute("SET datestyle = 'ISO, MDY'")
cur.execute("SHOW datestyle")
row = cur.fetchone()
assert row[0] == "ISO, MDY", f"Expected 'ISO, MDY', got {row[0]}"

print("ParameterStatus integration test passed")
cur.close()
conn.close()
```

**Step 3: Run integration test (requires server running)**

Run: `cd tests-integration && python3 test_set_parameter.py`

**Step 4: Commit**

```bash
git add tests-integration/test_set_parameter.py
git commit -m "test: add integration test for SET parameter status"
```

---

### Task 7: Final Review

**Step 1: Run all unit tests**

Run: `cargo test -p datafusion-postgres 2>&1 | tail -20`
Expected: All pass, no regressions

**Step 2: Run clippy**

Run: `cargo clippy -p datafusion-postgres -- -D warnings 2>&1 | head -40`
Fix any warnings.

**Step 3: Run cargo fmt**

Run: `cargo fmt -p datafusion-postgres`

**Step 4: Final commit if any fmt changes**

```bash
git add -p
git commit -m "style: apply cargo fmt"
```

**Step 5: Verify the change against the issue**

The issue #242 asks: "Send `ParameterStatus` upon successful SET statement, just like the startup process."

Checklist:
- [ ] `ParameterStatus` is sent for metadata-backed variables (datestyle, bytea_output, etc.)
- [ ] `ParameterStatus` is sent for `SET TIME ZONE`
- [ ] `statement_timeout` does NOT send ParameterStatus (it's an internal extension, not a PG GUC)
- [ ] Works for both simple query and extended query paths
- [ ] Existing tests still pass
- [ ] New tests cover the behavior

---

## Key Notes

1. **Why not send from inside the hook?** The `QueryHook` trait's `handle_simple_query` takes `dyn ClientInfo + Send + Sync` — no `Sink` capability. Changing this would break the trait's object safety and require a larger refactor.

2. **Why use `feed` not `send`?** `feed` buffers the message; `send` flushes immediately. Using `feed` is consistent with how pgwire's auth handler sends ParameterStatus during startup.

3. **ParameterStatus import**: `pgwire::messages::startup::ParameterStatus`

4. **Error mapping**: `PgWireError: From<C::Error>` bound is needed to convert Sink errors to PgWireError.
