
● Now I have the full picture — pgwire's message types, all three hook implementations, and the public API surface. Here's what's realistic:

  What pgwire Can Send (Side-Channel Messages)

  From PgWireBackendMessage, these are "out-of-band" messages that aren't part of the query response:

  ┌──────────────────────┬──────────────────────────────────┬──────────────────────────────────────────────────────────────┐
  │       Message        │           What it does           │                 PostgreSQL sends it when...                  │
  ├──────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────────────────────┤
  │ ParameterStatus      │ Notifies client of config change │ SET timezone = ..., SET datestyle = ...                      │
  ├──────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────────────────────┤
  │ NoticeResponse       │ Non-fatal warning/info           │ Deprecated feature, implicit casts, BEGIN inside transaction │
  ├──────────────────────┼──────────────────────────────────┼──────────────────────────────────────────────────────────────┤
  │ NotificationResponse │ Async notification               │ NOTIFY channel, 'payload' (LISTEN/NOTIFY)                    │
  └──────────────────────┴──────────────────────────────────┴──────────────────────────────────────────────────────────────┘

  Mapping to Your Hooks

  SetShowHook — already needs ParameterStatus, will likely need NoticeResponse

  Looking at your code, set_show.rs:221-228:
  // fallback to datafusion and ignore all errors
  if let Err(e) = execute_set_statement(session_context, statement.clone()).await {
      warn!("SET statement {statement} is not supported by datafusion, error {e}, statement ignored");
  }
  // Always return SET success

  That warn! goes to the server log, but real PostgreSQL would send a NOTICE to the client. Clients like psql display these. Today you silently swallow it — a notices field
  would let you surface this properly.

  Also: SET search_path to a nonexistent schema, SET to a value outside valid range, deprecated parameter names — all NOTICE scenarios.

  TransactionStatementHook — will likely need NoticeResponse

  transactions.rs:107:
  log::warn!("BEGIN command ignored: already in transaction block");

  PostgreSQL sends this as a WARNING notice to the client, not just a server-side log. Same for COMMIT outside a transaction block, ROLLBACK with no active transaction, etc.

  PermissionsHook — could use NoticeResponse

  Advisory-mode permission checks ("you don't have permission for X, proceeding anyway"), or deprecation warnings on specific tables/views.

  Future: LISTEN/NOTIFY hook

  If someone implements LISTEN/NOTIFY as a hook, they'd need NotificationResponse. This is more speculative but pgwire explicitly supports it.

  Realistic HookOutput Struct

  Based on what's actually needed (not speculative):

  #[non_exhaustive]
  pub struct HookOutput {
      pub response: Response,
      pub parameter_status: Vec<(String, String)>,   // 0 or more ParameterStatus
      pub notices: Vec<NoticeFields>,                  // 0 or more NOTICE/WARNING
  }

  Where:
  - parameter_status becomes a Vec — a single SET could trigger multiple (e.g., SET datestyle in PostgreSQL sends both DateStyle and date_style as a compatibility thing).
  Also cleaner than Option<(String, String)>.
  - notices covers all the log::warn!() calls that should actually go to the client.
  - #[non_exhaustive] means adding fields later (e.g., notifications for LISTEN/NOTIFY) is non-breaking — downstream can't construct with struct literals, must use a
  constructor.

  Then you provide:

  impl HookOutput {
      pub fn new(response: Response) -> Self { ... }           // common case — just a response
      pub fn with_parameter_status(mut self, k, v) -> Self     // builder for SET
      pub fn with_notice(mut self, notice) -> Self             // builder for warnings
  }

  What this buys you

  ┌───────────────────────────────────────┬────────────────────────────────┬──────────────────────────────────────────────────────────────┐
  │               Scenario                │         Today (tuple)          │                         With struct                          │
  ├───────────────────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────┤
  │ Hook returns just a Response          │ (response, None)               │ HookOutput::new(response)                                    │
  ├───────────────────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────┤
  │ Hook returns ParameterStatus          │ (response, Some((k, v)))       │ HookOutput::new(response).with_parameter_status(k, v)        │
  ├───────────────────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────┤
  │ Hook sends a WARNING notice           │ Not possible                   │ HookOutput::new(response).with_notice(...)                   │
  ├───────────────────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────┤
  │ Add a new field (e.g., notifications) │ Breaking change                │ Non-breaking (add field + builder method)                    │
  ├───────────────────────────────────────┼────────────────────────────────┼──────────────────────────────────────────────────────────────┤
  │ Multiple ParameterStatus from one SET │ Not possible (Option, not Vec) │ .with_parameter_status(k1, v1).with_parameter_status(k2, v2) │
  └───────────────────────────────────────┴────────────────────────────────┴──────────────────────────────────────────────────────────────┘

  What it costs

  - Slightly more verbose construction (HookOutput::new(resp) vs (resp, None))
  - One more type for downstream to learn
  - #[non_exhaustive] forces builder pattern — no struct literal construction

  Bottom line

  The two concrete things you'd add soonest are:
  1. notices: Vec<...> — replacing the log::warn!() calls that should be client-visible
  2. parameter_status as Vec — supporting multiple status messages per hook response

  Everything else (notifications, audit) is speculative. But #[non_exhaustive] + builder means you never break the API again regardless.

