mod common;

use common::*;
use pgwire::api::query::SimpleQueryHandler;

const DATAGRIP_QUERIES: &[&str] = &[
    "SET extra_float_digits = 3",
    "SET application_name = ''",
    "select version()",
    "SET application_name = 'DataGrip 2025.2.3'",
    "select current_database() as a, current_schemas(false) as b",
    "SHOW TRANSACTION ISOLATION LEVEL",
    "select round(extract(epoch from pg_postmaster_start_time() at time zone 'UTC')) as startup_time",
    "select L.transactionid::varchar::bigint as transaction_id
        from pg_catalog.pg_locks L
        where L.transactionid is not null
        order by pg_catalog.age(L.transactionid) desc
        limit 1",
    "select case
          when pg_catalog.pg_is_in_recovery()
            then null
          else
            (pg_catalog.txid_current() % 4294967296)::varchar::bigint
          end as current_txid",
    r#"select N.oid::bigint as id,
               datname as name,
               D.description,
               datistemplate as is_template,
               datallowconn as allow_connections,
               pg_catalog.pg_get_userbyid(N.datdba) as "owner"
        from pg_catalog.pg_database N
          left join pg_catalog.pg_shdescription D on N.oid = D.objoid
        order by case when datname = pg_catalog.current_database() then -1::bigint else N.oid::bigint end"#,
    r#"select N.oid::bigint as id,
               N.xmin as state_number,
               nspname as name,
               D.description,
               pg_catalog.pg_get_userbyid(N.nspowner) as "owner"
        from pg_catalog.pg_namespace N
          left join pg_catalog.pg_description D on N.oid = D.objoid
        order by case when nspname = pg_catalog.current_schema() then -1::bigint else N.oid::bigint end"#,
    r#"SELECT typinput='pg_catalog.array_in'::regproc as is_array, typtype, typname, pg_type.oid   FROM pg_catalog.pg_type   LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  WHERE pg_type.oid = '28'  ORDER BY sp.r, pg_type.oid DESC"#,
    r#"show DateStyle"#,
    r#"select name, is_dst from pg_catalog.pg_timezone_names
        union distinct
        select abbrev as name, is_dst from pg_catalog.pg_timezone_abbrevs"#,
    r#"select R.oid::bigint as role_id, rolname as role_name,
          rolsuper is_super, rolinherit is_inherit,
          rolcreaterole can_createrole, rolcreatedb can_createdb,
          rolcanlogin can_login, rolreplication /* false */ is_replication,
          rolconnlimit conn_limit, rolvaliduntil valid_until,
          rolbypassrls /* false */ bypass_rls, rolconfig config,
          D.description
        from pg_catalog.pg_roles R
          left join pg_catalog.pg_shdescription D on D.objoid = R.oid"#,
    r#"select member id, roleid role_id, admin_option
                  from pg_catalog.pg_auth_members order by id, roleid::text"#,
    r#"select T.oid::bigint as id, T.spcname as name,
               T.xmin as state_number, pg_catalog.pg_get_userbyid(T.spcowner) as owner,
               pg_catalog.pg_tablespace_location(T.oid) /* null */ as location,
               T.spcoptions /* null */ as options,
               D.description as comment
        from pg_catalog.pg_tablespace T
          left join pg_catalog.pg_shdescription D on D.objoid = T.oid
        --  where pg_catalog.age(T.xmin) <= #TXAGE"#,
    r#"select T.oid as object_id,
                         T.spcacl as acl
                  from pg_catalog.pg_tablespace T
                  union all
                  select T.oid as object_id,
                         T.datacl as acl
                  from pg_catalog.pg_database T"#,
    r#" SELECT typinput='pg_catalog.array_in'::regproc as is_array, typtype, typname, pg_type.oid   FROM pg_catalog.pg_type   LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  WHERE pg_type.oid = '1034'  ORDER BY sp.r, pg_type.oid DESC"#,
    r#"SELECT e.typdelim FROM pg_catalog.pg_type t, pg_catalog.pg_type e WHERE t.oid = '1034' and t.typelem = e.oid"#,
    r#" SELECT e.oid, n.nspname = ANY(current_schemas(true)), n.nspname, e.typname FROM pg_catalog.pg_type t JOIN pg_catalog.pg_type e ON t.typelem = e.oid JOIN pg_catalog.pg_namespace n ON t.typnamespace = n.oid WHERE t.oid = '1034'"#,
    r#"SELECT typinput='pg_catalog.array_in'::regproc as is_array, typtype, typname, pg_type.oid   FROM pg_catalog.pg_type   LEFT JOIN (select ns.oid as nspoid, ns.nspname, r.r           from pg_namespace as ns           join ( select s.r, (current_schemas(false))[s.r] as nspname                    from generate_series(1, array_upper(current_schemas(false), 1)) as s(r) ) as r          using ( nspname )        ) as sp     ON sp.nspoid = typnamespace  WHERE pg_type.oid = '1033'  ORDER BY sp.r, pg_type.oid DESC"#,
];

#[tokio::test]
pub async fn test_datagrip_startup_sql() {
    env_logger::init();
    let service = setup_handlers();
    let mut client = MockClient::new();

    for query in DATAGRIP_QUERIES {
        SimpleQueryHandler::do_query(&service, &mut client, query)
            .await
            .unwrap_or_else(|e| {
                panic!("failed to run sql:\n-----------------\n {query}\n-----------------\n {e}")
            });
    }
}
