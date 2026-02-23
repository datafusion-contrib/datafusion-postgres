 pgwire Changes (new branch in ../pgwire)

  Goal: Enable SimpleQueryHandler::do_query to access PortalStore with known statement types.

  Approach: Add type Statement associated type to SimpleQueryHandler with a sensible default, and update the do_query bound.

  // In pgwire/src/api/query.rs
  pub trait SimpleQueryHandler: Send + Sync {
      // NEW: Associated type for prepared statements (optional, with default)
      type Statement: Clone + Send + Sync + 'static = String;

      async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
      where
          C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
          C::PortalStore: PortalStore<Statement = Self::Statement>, // NEW bound
          C::Error: Debug,
          PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;
  }

  Impact:
  - Backward compatible with default type
  - Allows implementors (like DfSessionService) to specify their own type Statement
  - Gives SimpleQueryHandler::do_query the type info needed to use portal store

  datafusion-postgres Changes

  1. Update SimpleQueryHandler impl:
  - Add type Statement = (String, Option<(sqlparser::ast::Statement, LogicalPlan)>) to DfSessionService
  - Update do_query bounds to include C::PortalStore: PortalStore<Statement = Self::Statement>
  - In do_query: detect Statement::Prepare, Statement::Execute, Statement::Deallocate and handle them before hook chain

  2. Handler Logic (DfSessionService::do_query):
  For each SQL statement:
    IF Statement::Prepare { name, data_types, statement }:
      Parse inner statement → LogicalPlan (via state.statement_to_plan)
      Create StoredStatement
      portal_store.put_statement(name, StoredStatement)
      Return Response::Execution("PREPARE")

    ELSE IF Statement::Execute { name, parameters, using }:
      Get StoredStatement from portal_store(name)
      Evaluate parameters as Expr to literal values
      Substitute into LogicalPlan
      Execute plan
      Return Response

    ELSE IF Statement::Deallocate { name, prepare }:
      portal_store.rm_statement(name)
      Return Response::Execution("DEALLOCATE")

    ELSE:
      Continue to hook chain (existing behavior)

  3. New PrepareExecuteHook (optional):
  - Hook can intercept PREPARE/EXECUTE to customize behavior (e.g., permission checks, logging)
  - Returns None for default handling via portal store
  - Returns Some(Response) to override

  4. Cargo.toml change (temporary):
  [patch.crates-io]
  pgwire = { path = "../pgwire" }
     Or directly:
  pgwire = { path = "../pgwire", version = "0.38" }

  Architecture Diagram

  SimpleQueryHandler::do_query(client, sql)
    ↓
  Parse SQL → Vec<Statement>
    ↓
  For each Statement:
    ├─ Prepare { name, data_types, inner_stmt }
    │  ├─ Parse inner_stmt → LogicalPlan
    │  ├─ portal_store.put_statement(name, LogicalPlan)
    │  └─ Return PREPARE response
    │
    ├─ Execute { name, parameters, ... }
    │  ├─ Get StoredStatement from portal_store(name)
    │  ├─ Evaluate param expressions
    │  ├─ Execute plan
    │  └─ Return result
    │
    ├─ Deallocate { name, prepare }
    │  ├─ portal_store.rm_statement(name)
    │  └─ Return DEALLOCATE response
    │
    └─ Other
       ├─ Hook chain (existing)
       └─ DataFusion execution (existing)

  Testing Strategy

  1. Unit tests in datafusion-postgres:
    - PREPARE stores in portal store
    - EXECUTE retrieves and executes
    - DEALLOCATE removes
    - Parameter substitution works correctly
  2. Integration tests:
    - Simple query protocol: PREPARE, EXECUTE, DEALLOCATE
    - Cross-protocol: extended query can access SQL-prepared statements? (or keep separate)
  3. pgwire tests:
    - Verify default type Statement = String works for existing implementations

  ---
  Next steps:
  1. Create new branch in ../pgwire
  2. Implement pgwire changes
  3. Update datafusion-postgres with handler logic + hook
  4. Test together via path dependency
  5. Create two sister PRs

  Does this design align with your vision?

