# PostgreSQL Built-in Functions — DataFusion Coverage Catalog

This document catalogs the **built-in functions** shipped with PostgreSQL and
tracks how they map onto Apache DataFusion. It is the working checklist used by
the `datafusion-pg-functions` crate: every entry marked 🚧 here is a candidate
for implementation as a DataFusion `ScalarUDF` in this crate.

Functions are grouped by the same categories PostgreSQL uses in its
[Functions and Operators](https://www.postgresql.org/docs/current/functions.html)
reference. The "Module" column points at the submodule under
`datafusion-pg-functions/src/` where the function belongs once implemented.

> **Hand-maintained.** This file was bootstrapped from the PostgreSQL
> source (`func.sgml` and `pg_proc.dat` at `REL_18_STABLE`) and is now edited
> by hand. Each row carries a status marker (✅/🚧/🔧/❌) and an implementation
> priority (P1–P4); see the legends below.


_PostgreSQL source ref: `REL_18_STABLE` · [`func.sgml`](https://github.com/postgres/postgres/blob/REL_18_STABLE/doc/src/sgml/func.sgml) · [`pg_proc.dat`](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/catalog/pg_proc.dat)_

## Status legend

| Marker | Meaning |
|---|---|
| ✅ | **Native** — DataFusion already provides a function of this name. Semantic deviations from Postgres are flagged in the *Notes* column. |
| 🚧 | **Planned** — not yet available; this crate is the home for a future implementation. |
| 🔧 | **Implemented** — registered by `datafusion-pg-functions` or `datafusion-pg-catalog`. |
| ❌ | **Out of scope** — depends on backend machinery DataFusion lacks (PL/pgSQL, WAL, sequence state) or on a type DataFusion does not have (JSON/JSONB, `inet`/`cidr`, `tsvector`, ranges, enums, XML, geometric types). |

## Priority legend

The **Pri** column sorts *planned* work by where implementation effort is
best spent. Native and already-implemented rows show `—`.

| Pri | Meaning |
|---|---|
| **P1** | **Trivial** — thin alias or wrapper around an existing DataFusion function. Quick wins. |
| **P2** | **Moderate** — real implementation, no new types or backend machinery required. High client-tool value (psql, dbeaver, grafana). |
| **P3** | **Heavier** — multi-overload, numeric-internal, ordered-set aggregates, or otherwise non-trivial. |
| **P4** | **Blocked** — needs a DataFusion type or backend feature that does not yet exist. Effectively ❌ until that lands. |

## Kind legend

The **Kind** column records how each entry is invoked in SQL. This affects the
implementation strategy in DataFusion.

| Kind | Meaning | Implementation strategy |
|---|---|---|
| `fn` | SQL function: `name(args, ...)`. | Implement as a [`ScalarUDF`](datafusion::logical_expr::ScalarUDF). |
| `op` | Operator / special-syntax only — no `name(args)` form (e.g. `a LIKE b`, `a SIMILAR TO b`). | Requires SQL parser / operator support in DataFusion; a UDF alone is not enough. |
| `both` | Has both a function form and an operator / SQL-clause form (e.g. `substring(s FROM 1 FOR 3)` and `substr(s, 1, 3)`). | UDF for the function form; SQL parser work for the clause form. |

## Summary by category

| Category | Documented | ✅ Native | 🔧 Impl | 🚧 fn P1 | 🚧 fn P2 | 🚧 fn P3 | 🚧 op/both | ❌/🚧 P4 |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| [Comparison](#functions-comparison) | 2 | 0 | 0 | 0 | 2 | 0 | 0+0 | 0 |
| [Logical Operators](#functions-logical) | 0 | 0 | 0 | 0 | 0 | 0 | 0+0 | 0 |
| [Conditional Expressions](#functions-conditional) | 7 | 6 | 0 | 0 | 0 | 1 | 0+0 | 0 |
| [Subquery Expressions](#functions-subquery) | 0 | 0 | 0 | 0 | 0 | 0 | 0+0 | 0 |
| [Row and Array Comparisons](#functions-comparisons) | 0 | 0 | 0 | 0 | 0 | 0 | 0+0 | 0 |
| [Mathematical](#functions-math) | 55 | 33 | 18 | 0 | 0 | 4 | 0+0 | 0 |
| [String](#functions-string) | 61 | 42 | 16 | 0 | 3 | 0 | 0+4 | 0 |
| [Binary String](#functions-binarystring) | 30 | 20 | 0 | 0 | 10 | 0 | 0+4 | 0 |
| [Bit String](#functions-bitstring) | 9 | 6 | 0 | 0 | 0 | 0 | 0+3 | 3 |
| [Pattern Matching](#functions-matching) | 15 | 7 | 0 | 0 | 4 | 0 | 3+2 | 0 |
| [Data Type Formatting](#functions-formatting) | 10 | 3 | 0 | 0 | 7 | 0 | 0+0 | 0 |
| [Date/Time](#functions-datetime) | 33 | 11 | 0 | 0 | 21 | 0 | 0+1 | 0 |
| [Enum Support](#functions-enum) | 3 | 0 | 0 | 0 | 0 | 0 | 0+0 | 3 |
| [Geometric](#functions-geometry) | 23 | 1 | 0 | 0 | 0 | 0 | 0+0 | 22 |
| [Network Address](#functions-net) | 14 | 1 | 0 | 0 | 0 | 0 | 0+0 | 13 |
| [Text Search](#functions-textsearch) | 28 | 1 | 0 | 0 | 0 | 0 | 0+0 | 27 |
| [UUID](#functions-uuid) | 5 | 0 | 0 | 1 | 4 | 0 | 0+0 | 0 |
| [XML](#functions-xml) | 33 | 0 | 0 | 0 | 0 | 0 | 0+0 | 33 |
| [JSON](#functions-json) | 67 | 1 | 0 | 0 | 0 | 0 | 0+4 | 66 |
| [Sequence Manipulation](#functions-sequence) | 4 | 0 | 0 | 0 | 0 | 0 | 0+0 | 4 |
| [Array](#functions-array) | 23 | 16 | 2 | 0 | 5 | 0 | 0+0 | 0 |
| [Range/Multirange](#functions-range) | 10 | 2 | 0 | 0 | 0 | 0 | 0+0 | 8 |
| [Set Returning](#functions-srf) | 3 | 1 | 0 | 0 | 2 | 0 | 0+0 | 0 |
| [Aggregate](#functions-aggregate) | 64 | 35 | 0 | 2 | 0 | 6 | 0+0 | 21 |
| [Window](#functions-window) | 11 | 11 | 0 | 0 | 0 | 0 | 0+0 | 0 |
| [Merge Support](#functions-merge-support) | 1 | 0 | 0 | 0 | 0 | 0 | 0+0 | 1 |
| [Statistics Information](#functions-statistics) | 1 | 0 | 0 | 0 | 0 | 0 | 0+0 | 1 |
| [System Information](#functions-info) | 143 | 1 | 16 | 1 | 8 | 54 | 0+0 | 63 |
| [System Administration](#functions-admin) | 105 | 0 | 2 | 0 | 5 | 0 | 0+0 | 98 |
| [Trigger](#functions-trigger) | 3 | 0 | 0 | 0 | 0 | 0 | 0+0 | 3 |
| [Event Trigger](#functions-event-triggers) | 5 | 0 | 0 | 0 | 0 | 0 | 0+0 | 5 |
| **TOTAL** | **768** | **198** | **54** | **4** | **71** | **65** | **21** | **371** |

---

## Comparison Functions and Operators

*Section `functions-comparison` · Module: [`conditional`](src/conditional.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `num_nonnulls` | fn | 🚧 | P2 | 1 | count the number of non-NULL arguments |  |
| `num_nulls` | fn | 🚧 | P2 | 1 | count the number of NULL arguments |  |

## Logical Operators

*Section `functions-logical` · Module: [`conditional`](src/conditional.rs)

| Operator / Construct | Status | Notes |
|---|:---:|---|
| `a AND b` | fn | 🚧 | Logical conjunction, with NULL as third truth value. |
| `a OR b` | fn | 🚧 | Logical disjunction. |
| `NOT a` | fn | 🚧 | Logical negation. |

## Conditional Expressions

*Section `functions-conditional` · Module: [`conditional`](src/conditional.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `coalesce` | fn | ✅ | — | 0 | First non-NULL argument | Native DataFusion (SQL special form). |
| `greatest` | fn | ✅ | — | 0 | Largest of arguments | Native DataFusion (SQL special form). |
| `ifnull` | fn | ✅ | — | 0 | — | Native DataFusion (SQL special form). |
| `least` | fn | ✅ | — | 0 | Smallest of arguments | Native DataFusion (SQL special form). |
| `nullif` | fn | ✅ | — | 0 | NULL if a = b, else a | Native DataFusion (SQL special form). |
| `nvl` | fn | ✅ | — | 0 | — | Native DataFusion (SQL special form). |
| `switch` | fn | 🚧 | P3 | 0 | — | PG 18+. |

## Subquery Expressions

*Section `functions-subquery` · Module: [`conditional`](src/conditional.rs)

| Operator / Construct | Status | Notes |
|---|:---:|---|
| `EXISTS (subquery)` | fn | 🚧 | True if subquery returns at least one row. |
| `NOT EXISTS (subquery)` | fn | 🚧 | True if subquery returns no rows. |
| `UNIQUE (subquery)` | fn | 🚧 | True if subquery has no duplicate rows. |
| `a IN (subquery)` | fn | 🚧 | True if a equals any row of subquery. |
| `a NOT IN (subquery)` | fn | 🚧 | True if a differs from every row of subquery. |

## Row and Array Comparisons

*Section `functions-comparisons` · Module: [`conditional`](src/conditional.rs)

| Operator / Construct | Status | Notes |
|---|:---:|---|
| `a IN (x, ...)` | fn | 🚧 | True if a equals any value. |
| `a NOT IN (x, ...)` | fn | 🚧 | True if a differs from every value. |
| `a ANY (array/subquery)` | fn | 🚧 | Combined with =, <>, <, >, <=, >=. |
| `a ALL (array/subquery)` | fn | 🚧 | Combined with =, <>, <, >, <=, >=. |
| `row_constructor op row_constructor` | fn | 🚧 | Per-column comparison. |

## Mathematical Functions and Operators

*Section `functions-math` · Module: [`numeric`](src/numeric.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `abs` | fn | ✅ | — | 6 | absolute value | Native DataFusion |
| `acos` | fn | ✅ | — | 1 | arccosine | Native DataFusion |
| `acosd` | fn | 🔧 | — | 1 | arccosine, degrees |  |
| `acosh` | fn | ✅ | — | 1 | inverse hyperbolic cosine | Native DataFusion |
| `asin` | fn | ✅ | — | 1 | arcsine | Native DataFusion |
| `asind` | fn | 🔧 | — | 1 | arcsine, degrees |  |
| `asinh` | fn | ✅ | — | 1 | inverse hyperbolic sine | Native DataFusion |
| `atan` | fn | ✅ | — | 1 | arctangent | Native DataFusion |
| `atan2` | fn | ✅ | — | 1 | arctangent, two arguments | Native DataFusion |
| `atan2d` | fn | 🔧 | — | 1 | arctangent, two arguments, degrees |  |
| `atand` | fn | 🔧 | — | 1 | arctangent, degrees |  |
| `atanh` | fn | ✅ | — | 1 | inverse hyperbolic tangent | Native DataFusion |
| `cbrt` | fn | ✅ | — | 1 | cube root | Native DataFusion |
| `ceil` | fn | ✅ | — | 2 | nearest integer >= value | Native DataFusion |
| `ceiling` | fn | 🔧 | — | 2 | nearest integer >= value | Implemented in this crate (`numeric::aliases`). |
| `cos` | fn | ✅ | — | 1 | cosine | Native DataFusion |
| `cosd` | fn | 🔧 | — | 1 | cosine, degrees |  |
| `cosh` | fn | ✅ | — | 1 | hyperbolic cosine | Native DataFusion |
| `cot` | fn | ✅ | — | 1 | cotangent | Native DataFusion |
| `cotd` | fn | 🔧 | — | 1 | cotangent, degrees |  |
| `degrees` | fn | ✅ | — | 1 | radians to degrees | Native DataFusion |
| `div` | fn | 🔧 | — | 1 | trunc(x/y) | Integer quotient. |
| `erf` | fn | 🔧 | — | 1 | error function | Special function. |
| `erfc` | fn | 🔧 | — | 1 | complementary error function |  |
| `exp` | fn | ✅ | — | 2 | natural exponential (e^x) | Native DataFusion |
| `factorial` | fn | ✅ | — | 1 | factorial | Native DataFusion |
| `floor` | fn | ✅ | — | 2 | nearest integer <= value | Native DataFusion |
| `gamma` | fn | 🔧 | — | 1 | gamma function |  |
| `gcd` | fn | ✅ | — | 3 | greatest common divisor | Native DataFusion |
| `lcm` | fn | ✅ | — | 3 | least common multiple | Native DataFusion |
| `lgamma` | fn | 🔧 | — | 1 | natural logarithm of absolute value of gamma function |  |
| `ln` | fn | ✅ | — | 2 | natural logarithm | Native DataFusion |
| `log` | fn | ✅ | — | 3 | logarithm base m of n | PG default base is 10; DataFusion default base is e. Native DataFusion |
| `log10` | fn | ✅ | — | 2 | base 10 logarithm | Native DataFusion (base-10 log). |
| `min_scale` | fn | 🚧 | P3 | 1 | minimum scale needed to represent the value | Numeric-internal. |
| `mod` | fn | 🔧 | — | 4 | modulus | Implemented in this crate (`numeric::mod_op`). |
| `pi` | fn | ✅ | — | 1 | PI | Native DataFusion |
| `power` | fn | ✅ | — | 2 | exponentiation | Native DataFusion |
| `radians` | fn | ✅ | — | 1 | degrees to radians | Native DataFusion |
| `random` | fn | ✅ | — | 4 | random integer in range | Native DataFusion |
| `random_normal` | fn | 🔧 | — | 1 | random value from normal distribution | Box-Muller on `random()`. |
| `round` | fn | ✅ | — | 3 | value rounded to 'scale' of zero | PG 2-arg form: `round(x, n)`. Native DataFusion. DataFusion's round lacks the 2-argument form in some versions. |
| `scale` | fn | 🚧 | P3 | 1 | number of decimal digits in the fractional part | Numeric-internal. |
| `setseed` | fn | 🚧 | P3 | 1 | set random seed | Mutates global RNG; thread-safety concern. |
| `sign` | fn | 🔧 | — | 2 | sign of value | Implemented in this crate (`numeric::aliases`). DataFusion names this `signum`. |
| `sin` | fn | ✅ | — | 1 | sine | Native DataFusion |
| `sind` | fn | 🔧 | — | 1 | sine, degrees |  |
| `sinh` | fn | ✅ | — | 1 | hyperbolic sine | Native DataFusion |
| `sqrt` | fn | ✅ | — | 2 | square root | Native DataFusion |
| `tan` | fn | ✅ | — | 1 | tangent | Native DataFusion |
| `tand` | fn | 🔧 | — | 1 | tangent, degrees |  |
| `tanh` | fn | ✅ | — | 1 | hyperbolic tangent | Native DataFusion |
| `trim_scale` | fn | 🚧 | P3 | 1 | numeric with minimum scale needed to represent the value | Numeric-internal. |
| `trunc` | fn | ✅ | — | 5 | value truncated to 'scale' of zero | PG 2-arg form: `trunc(x, n)`. Native DataFusion. Same 2-arg concern as round. |
| `width_bucket` | fn | 🔧 | — | 3 | bucket number of operand given a sorted array of bucket lower bounds | Numeric histogram bucketing. |

## String Functions and Operators

*Section `functions-string` · Module: [`string`](src/string/)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `abs` | fn | ✅ | — | 6 | absolute value | Native DataFusion |
| `array_to_string` | fn | ✅ | — | 2 | concatenate array elements, using delimiter and null string, into text | Native DataFusion |
| `ascii` | fn | ✅ | — | 1 | convert first char to int4 | Native DataFusion |
| `bit_length` | fn | ✅ | — | 3 | length in bits | Native DataFusion |
| `btrim` | fn | ✅ | — | 3 | trim selected characters from both ends of string | Native DataFusion |
| `casefold` | fn | 🚧 | P2 | 1 | fold case |  |
| `char_length` | fn | ✅ | — | 2 | character length | Native DataFusion |
| `character_length` | fn | ✅ | — | 2 | character length | Native DataFusion |
| `chr` | fn | ✅ | — | 1 | convert int4 to char | Native DataFusion |
| `concat` | fn | ✅ | — | 1 | concatenate values | Native DataFusion |
| `concat_ws` | fn | ✅ | — | 1 | concatenate values with separators | Native DataFusion |
| `format` | fn | 🚧 | P2 | 2 | format text message | PG `%s`/`%I`/`%L` format strings; high-value for psql/dbeaver. |
| `initcap` | fn | ✅ | — | 1 | capitalize each word | Native DataFusion |
| `left` | fn | ✅ | — | 1 | extract the first n characters | Native DataFusion |
| `length` | fn | ✅ | — | 8 | length of string in specified encoding | Overloaded in PG (text/bytea/bit); DF covers text only. Native DataFusion |
| `lower` | fn | ✅ | — | 3 | lower bound of multirange | Native DataFusion |
| `lpad` | fn | ✅ | — | 2 | left-pad string to length | Native DataFusion |
| `ltrim` | fn | ✅ | — | 3 | trim selected characters from left end of string | Native DataFusion |
| `md5` | fn | ✅ | — | 2 | MD5 hash | Native DataFusion |
| `normalize` | fn | 🚧 | P2 | 1 | Unicode normalization | Unicode normalization. |
| `octet_length` | fn | ✅ | — | 4 | octet length | Native DataFusion |
| `overlay` | both | ✅ | — | 6 | substitute portion of bitstring | Native DataFusion |
| `parse_ident` | fn | 🔧 | — | 1 | parse qualified identifier to array of identifiers | In datafusion-pg-catalog |
| `pg_client_encoding` | fn | 🚧 | P2 | 1 | encoding name of current database |  |
| `position` | both | ✅ | — | 3 | position of sub-bitstring | Native DataFusion |
| `quote_ident` | fn | 🔧 | — | 1 | quote an identifier for usage in a querystring | In datafusion-pg-catalog |
| `quote_literal` | fn | 🚧 | P2 | 2 | quote a data value for usage in a querystring |  |
| `quote_nullable` | fn | 🚧 | P2 | 2 | quote a possibly-null data value for usage in a querystring |  |
| `regexp_count` | fn | ✅ | — | 3 | count regexp matches | Native DataFusion |
| `regexp_instr` | fn | ✅ | — | 6 | position of regexp match | Native DataFusion |
| `regexp_like` | fn | ✅ | — | 2 | test for regexp match | Native DataFusion |
| `regexp_match` | fn | ✅ | — | 2 | find first match for regexp | Native DataFusion |
| `regexp_matches` | fn | 🚧 | P2 | 2 | find match(es) for regexp | Postgres is set-returning; DataFusion returns an array. |
| `regexp_replace` | fn | ✅ | — | 5 | replace text using regexp | Native DataFusion |
| `regexp_split_to_array` | fn | 🔧 | — | 2 | split string by pattern |  |
| `regexp_split_to_table` | fn | 🚧 | P2 | 2 | split string by pattern | Set-returning. |
| `regexp_substr` | fn | 🔧 | — | 5 | extract substring that matches regexp |  |
| `repeat` | fn | ✅ | — | 1 | replicate string n times | Native DataFusion |
| `replace` | fn | ✅ | — | 1 | replace all occurrences in string of old_substr with new_substr | Native DataFusion |
| `reverse` | fn | ✅ | — | 2 | reverse bytea | Native DataFusion |
| `right` | fn | ✅ | — | 1 | extract the last n characters | Native DataFusion |
| `rpad` | fn | ✅ | — | 2 | right-pad string to length | Native DataFusion |
| `rtrim` | fn | ✅ | — | 3 | trim selected characters from right end of string | Native DataFusion |
| `split_part` | fn | ✅ | — | 1 | split string by field_sep and return field_num | Native DataFusion |
| `sprintf` | fn | 🚧 | P2 | 0 | — | PG 18+. |
| `starts_with` | fn | ✅ | — | 1 | — | Native DataFusion |
| `string_agg` | fn | ✅ | — | 2 | concatenate aggregate input into a string | Native DataFusion |
| `string_to_array` | fn | ✅ | — | 2 | split delimited text, with null string | Native DataFusion |
| `string_to_table` | fn | 🚧 | P2 | 2 | split delimited text, with null string |  |
| `strpos` | fn | ✅ | — | 1 | position of substring | DataFusion is 0-based; Postgres is 1-based. Native DataFusion. Same naming and offset semantics as substr. |
| `substr` | fn | ✅ | — | 4 | extract portion of string | DataFusion is 0-based; Postgres is 1-based. Native DataFusion. DataFusion's substr is 0-based; Postgres is 1-based. Translate. |
| `substring` | both | ✅ | — | 8 | extract text matching SQL regular expression | Native DataFusion. Postgres 1-based offsets. |
| `to_ascii` | fn | 🚧 | P2 | 3 | encode text from DB encoding to ASCII text |  |
| `to_bin` | fn | 🚧 | P2 | 2 | convert int4 number to binary | Int→binary text. |
| `to_hex` | fn | ✅ | — | 2 | convert int4 number to hex | Native DataFusion |
| `to_oct` | fn | 🚧 | P2 | 2 | convert int4 number to oct | Int→octal text. |
| `translate` | fn | ✅ | — | 1 | map a set of characters appearing in string | Native DataFusion |
| `trim` | both | ✅ | — | 0 | — | Native DataFusion (SQL special form). |
| `unicode_assigned` | fn | 🚧 | P2 | 1 | check valid Unicode |  |
| `unistr` | fn | 🚧 | P2 | 1 | unescape Unicode characters | Unicode escape decoder. |
| `upper` | fn | ✅ | — | 3 | upper bound of multirange | Native DataFusion |

## Binary String Functions and Operators

*Section `functions-binarystring` · Module: [`binary`](src/binary.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `bit_count` | fn | 🚧 | P2 | 2 | number of set bits |  |
| `bit_length` | fn | ✅ | — | 3 | length in bits | Native DataFusion |
| `btrim` | fn | ✅ | — | 3 | trim selected characters from both ends of string | Native DataFusion |
| `convert` | fn | 🚧 | P2 | 1 | convert string with specified encoding names |  |
| `convert_from` | fn | 🚧 | P2 | 1 | convert string with specified source encoding name |  |
| `convert_to` | fn | 🚧 | P2 | 1 | convert string with specified destination encoding name |  |
| `crc32` | fn | 🚧 | P2 | 1 | CRC-32 value |  |
| `crc32c` | fn | 🚧 | P2 | 1 | CRC-32C value |  |
| `decode` | fn | ✅ | — | 1 | convert ascii-encoded text string into bytea value | Native DataFusion |
| `encode` | fn | ✅ | — | 1 | convert bytea value into some ascii-only text string | Native DataFusion |
| `get_bit` | fn | 🚧 | P2 | 2 | get bit |  |
| `get_byte` | fn | 🚧 | P2 | 1 | get byte |  |
| `length` | fn | ✅ | — | 8 | length of string in specified encoding | Overloaded in PG (text/bytea/bit); DF covers text only. Native DataFusion |
| `ltrim` | fn | ✅ | — | 3 | trim selected characters from left end of string | Native DataFusion |
| `md5` | fn | ✅ | — | 2 | MD5 hash | Native DataFusion |
| `octet_length` | fn | ✅ | — | 4 | octet length | Native DataFusion |
| `overlay` | both | ✅ | — | 6 | substitute portion of bitstring | Native DataFusion |
| `position` | both | ✅ | — | 3 | position of sub-bitstring | Native DataFusion |
| `reverse` | fn | ✅ | — | 2 | reverse bytea | Native DataFusion |
| `rtrim` | fn | ✅ | — | 3 | trim selected characters from right end of string | Native DataFusion |
| `set_bit` | fn | 🚧 | P2 | 2 | set bit |  |
| `set_byte` | fn | 🚧 | P2 | 1 | set byte |  |
| `sha224` | fn | ✅ | — | 1 | SHA-224 hash | Native DataFusion |
| `sha256` | fn | ✅ | — | 1 | SHA-256 hash | Native DataFusion |
| `sha384` | fn | ✅ | — | 1 | SHA-384 hash | Native DataFusion |
| `sha512` | fn | ✅ | — | 1 | SHA-512 hash | Native DataFusion |
| `string_agg` | fn | ✅ | — | 2 | concatenate aggregate input into a string | Native DataFusion |
| `substr` | fn | ✅ | — | 4 | extract portion of string | DataFusion is 0-based; Postgres is 1-based. Native DataFusion. DataFusion's substr is 0-based; Postgres is 1-based. Translate. |
| `substring` | both | ✅ | — | 8 | extract text matching SQL regular expression | Native DataFusion. Postgres 1-based offsets. |
| `trim` | both | ✅ | — | 0 | — | Native DataFusion (SQL special form). |

## Bit String Functions and Operators

*Section `functions-bitstring` · Module: [`bitstring`](src/bitstring.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `bit_count` | fn | ❌ | P4 | 2 | number of set bits | Blocked: no bit(n) type (use int ops instead). |
| `bit_length` | fn | ✅ | — | 3 | length in bits | Native DataFusion |
| `get_bit` | fn | ❌ | P4 | 2 | get bit | Blocked: no bit(n) type (use int ops instead). |
| `length` | fn | ✅ | — | 8 | length of string in specified encoding | Overloaded in PG (text/bytea/bit); DF covers text only. Native DataFusion |
| `octet_length` | fn | ✅ | — | 4 | octet length | Native DataFusion |
| `overlay` | both | ✅ | — | 6 | substitute portion of bitstring | Native DataFusion |
| `position` | both | ✅ | — | 3 | position of sub-bitstring | Native DataFusion |
| `set_bit` | fn | ❌ | P4 | 2 | set bit | Blocked: no bit(n) type (use int ops instead). |
| `substring` | both | ✅ | — | 8 | extract text matching SQL regular expression | Native DataFusion. Postgres 1-based offsets. |

## Pattern Matching

*Section `functions-matching` · Module: [`string`](src/string.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `ilike` | op | 🚧 | P3 | 0 | — | PG operator; needs a UDF shim to be callable as a function. |
| `like` | both | 🚧 | P2 | 3 | matches LIKE expression |  |
| `not like` | op | 🚧 | P2 | 0 | — |  |
| `regexp_count` | fn | ✅ | — | 3 | count regexp matches | Native DataFusion |
| `regexp_instr` | fn | ✅ | — | 6 | position of regexp match | Native DataFusion |
| `regexp_like` | fn | ✅ | — | 2 | test for regexp match | Native DataFusion |
| `regexp_match` | fn | ✅ | — | 2 | find first match for regexp | Native DataFusion |
| `regexp_matches` | fn | 🚧 | P2 | 2 | find match(es) for regexp | Postgres is set-returning; DataFusion returns an array. |
| `regexp_replace` | fn | ✅ | — | 5 | replace text using regexp | Native DataFusion |
| `regexp_split_to_array` | fn | 🔧 | — | 2 | split string by pattern |  |
| `regexp_split_to_table` | fn | 🚧 | P2 | 2 | split string by pattern | Set-returning. |
| `regexp_substr` | fn | 🔧 | — | 5 | extract substring that matches regexp |  |
| `similar to` | op | 🚧 | P2 | 0 | — |  |
| `starts_with` | fn | ✅ | — | 1 | — | Native DataFusion |
| `substring` | both | ✅ | — | 8 | extract text matching SQL regular expression | Native DataFusion. Postgres 1-based offsets. |

## Data Type Formatting Functions

*Section `functions-formatting` · Module: [`format`](src/format.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `extract(dow from ...)` | fn | 🚧 | P2 | 0 | — |  |
| `extract(isodow from ...)` | fn | 🚧 | P2 | 0 | — |  |
| `to_char` | fn | ✅ | — | 8 | format timestamp with time zone to text | PG numeric templates differ from DataFusion's. Native DataFusion. DataFusion supports timestamp formats; PG numeric templates differ. |
| `to_char(..., 'd')` | fn | 🚧 | P2 | 0 | — |  |
| `to_char(..., 'id')` | fn | 🚧 | P2 | 0 | — |  |
| `to_char(interval)` | fn | 🚧 | P2 | 0 | — |  |
| `to_date` | fn | ✅ | — | 1 | convert text to date | Native DataFusion |
| `to_number` | fn | 🚧 | P2 | 1 | convert text to numeric | PG numeric-template parser. |
| `to_timestamp` | fn | ✅ | — | 2 | convert text to timestamp with time zone | Native DataFusion |
| `to_timestamp(double precision)` | fn | 🚧 | P2 | 0 | — |  |

## Date/Time Functions and Operators

*Section `functions-datetime` · Module: [`datetime`](src/datetime.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `age` | fn | 🚧 | P2 | 5 | age of a transaction ID, in transactions before current transaction |  |
| `clock_timestamp` | fn | 🚧 | P2 | 1 | current clock time |  |
| `current_date` | fn | ✅ | — | 0 | Current date | Native DataFusion (SQL special form). |
| `current_time` | fn | ✅ | — | 0 | Current time of day | Native DataFusion (SQL special form). |
| `current_timestamp` | fn | ✅ | — | 0 | Current date and time | Native DataFusion (SQL special form). |
| `date_add` | fn | 🚧 | P2 | 2 | add interval to timestamp with time zone in specified time zone |  |
| `date_bin` | fn | ✅ | — | 2 | bin timestamp with time zone into specified interval | Native DataFusion |
| `date_part` | fn | ✅ | — | 6 | extract field from timestamp with time zone | Native DataFusion |
| `date_subtract` | fn | 🚧 | P2 | 2 | subtract interval from timestamp with time zone in specified time zone |  |
| `date_trunc` | fn | ✅ | — | 4 | truncate timestamp with time zone to specified units in specified time zone | Native DataFusion |
| `extract` | both | 🚧 | P2 | 6 | extract field from timestamp with time zone |  |
| `isfinite` | fn | 🚧 | P2 | 4 | finite timestamp? |  |
| `justify_days` | fn | 🚧 | P2 | 1 | promote groups of 30 days to numbers of months |  |
| `justify_hours` | fn | 🚧 | P2 | 1 | promote groups of 24 hours to numbers of days |  |
| `justify_interval` | fn | 🚧 | P2 | 1 | promote groups of 24 hours to numbers of days and promote groups of 30 days to numbers of months |  |
| `localtime` | fn | 🚧 | P2 | 0 | Current date and time, no time zone |  |
| `localtimestamp` | fn | 🚧 | P2 | 0 | Current timestamp, no time zone |  |
| `make_date` | fn | ✅ | — | 1 | construct date | Native DataFusion |
| `make_interval` | fn | 🚧 | P2 | 1 | construct interval |  |
| `make_time` | fn | ✅ | — | 1 | construct time | PG returns `time`; DF returns `Time64`/`Time32`. Native DataFusion |
| `make_timestamp` | fn | 🚧 | P2 | 1 | construct timestamp |  |
| `make_timestamptz` | fn | 🚧 | P2 | 2 | construct timestamp with time zone |  |
| `now` | fn | ✅ | — | 1 | current transaction time | PG returns `timestamptz`; DF returns `Timestamp(Nanos, None)`. Native DataFusion |
| `pg_sleep` | fn | 🚧 | P2 | 1 | sleep for the specified time in seconds |  |
| `pg_sleep_for` | fn | 🚧 | P2 | 1 | sleep for the specified interval |  |
| `pg_sleep_until` | fn | 🚧 | P2 | 1 | sleep until the specified time |  |
| `statement_timestamp` | fn | 🚧 | P2 | 1 | current statement time |  |
| `timeofday` | fn | 🚧 | P2 | 1 | current date and time - increments during transactions |  |
| `timezone` | fn | 🚧 | P2 | 9 | adjust time with time zone to new zone |  |
| `to_char(...,
        'd')` | fn | 🚧 | P2 | 0 | — |  |
| `to_timestamp` | fn | ✅ | — | 2 | convert text to timestamp with time zone | Native DataFusion |
| `transaction_timestamp` | fn | 🚧 | P2 | 1 | current transaction time |  |
| `trunc` | fn | ✅ | — | 5 | value truncated to 'scale' of zero | PG 2-arg form: `trunc(x, n)`. Native DataFusion. Same 2-arg concern as round. |

## Enum Support Functions

*Section `functions-enum` · Module: [`enum_type`](src/enum-type.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `enum_first` | fn | ❌ | P4 | 1 | first value of the input enum type | Blocked: no enum type. |
| `enum_last` | fn | ❌ | P4 | 1 | last value of the input enum type | Blocked: no enum type. |
| `enum_range` | fn | ❌ | P4 | 2 | range between the two given enum values, as an ordered array | Blocked: no enum type. |

## Geometric Functions and Operators

*Section `functions-geometry` · Module: [`geometric`](src/geometric.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `area` | fn | ❌ | P4 | 3 | area of a closed path | Blocked: no geometric types. |
| `bound_box` | fn | ❌ | P4 | 1 | bounding box of two boxes | Blocked: no geometric types. |
| `box` | fn | ❌ | P4 | 4 | convert polygon to bounding box | Blocked: no geometric types. |
| `center` | fn | ❌ | P4 | 2 | center of | Blocked: no geometric types. |
| `circle` | fn | ❌ | P4 | 3 | convert point and radius to circle | Blocked: no geometric types. |
| `diagonal` | fn | ❌ | P4 | 1 | box diagonal | Blocked: no geometric types. |
| `diameter` | fn | ❌ | P4 | 1 | diameter of circle | Blocked: no geometric types. |
| `height` | fn | ❌ | P4 | 1 | box height | Blocked: no geometric types. |
| `isclosed` | fn | ❌ | P4 | 1 | path closed? | Blocked: no geometric types. |
| `isopen` | fn | ❌ | P4 | 1 | path open? | Blocked: no geometric types. |
| `length` | fn | ✅ | — | 8 | length of string in specified encoding | Overloaded in PG (text/bytea/bit); DF covers text only. Native DataFusion |
| `line` | fn | ❌ | P4 | 1 | construct line from points | Blocked: no geometric types. |
| `lseg` | fn | ❌ | P4 | 2 | convert points to line segment | Blocked: no geometric types. |
| `lseg(box)` | fn | ❌ | P4 | 0 | — | Blocked: no geometric types. |
| `npoints` | fn | ❌ | P4 | 2 | number of points | Blocked: no geometric types. |
| `path` | fn | ❌ | P4 | 1 | convert polygon to path | Blocked: no geometric types. |
| `pclose` | fn | ❌ | P4 | 1 | close path | Blocked: no geometric types. |
| `point` | fn | ❌ | P4 | 5 | convert x, y to point | Blocked: no geometric types. |
| `polygon` | fn | ❌ | P4 | 4 | convert vertex count and circle to polygon | Blocked: no geometric types. |
| `popen` | fn | ❌ | P4 | 1 | open path | Blocked: no geometric types. |
| `radius` | fn | ❌ | P4 | 1 | radius of circle | Blocked: no geometric types. |
| `slope` | fn | ❌ | P4 | 1 | slope between points | Blocked: no geometric types. |
| `width` | fn | ❌ | P4 | 1 | box width | Blocked: no geometric types. |

## Network Address Functions and Operators

*Section `functions-net` · Module: [`network`](src/network.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `abbrev` | fn | ❌ | P4 | 2 | abbreviated display of cidr value | Blocked: no inet/cidr/macaddr type. |
| `broadcast` | fn | ❌ | P4 | 1 | broadcast address of network | Blocked: no inet/cidr/macaddr type. |
| `family` | fn | ❌ | P4 | 1 | address family (4 for IPv4, 6 for IPv6) | Blocked: no inet/cidr/macaddr type. |
| `host` | fn | ❌ | P4 | 1 | show address octets only | Blocked: no inet/cidr/macaddr type. |
| `hostmask` | fn | ❌ | P4 | 1 | hostmask of address | Blocked: no inet/cidr/macaddr type. |
| `inet_merge` | fn | ❌ | P4 | 1 | the smallest network which includes both of the given networks | Blocked: no inet/cidr/macaddr type. |
| `inet_same_family` | fn | ❌ | P4 | 1 | are the addresses from the same family? | Blocked: no inet/cidr/macaddr type. |
| `macaddr8_set7bit` | fn | ❌ | P4 | 1 | set 7th bit in macaddr8 | Blocked: no inet/cidr/macaddr type. |
| `masklen` | fn | ❌ | P4 | 1 | netmask length | Blocked: no inet/cidr/macaddr type. |
| `netmask` | fn | ❌ | P4 | 1 | netmask of address | Blocked: no inet/cidr/macaddr type. |
| `network` | fn | ❌ | P4 | 1 | network part of address | Blocked: no inet/cidr/macaddr type. |
| `set_masklen` | fn | ❌ | P4 | 2 | change netmask of cidr | Blocked: no inet/cidr/macaddr type. |
| `text` | fn | ❌ | P4 | 6 | serialize an XML value to a character string | Blocked: no inet/cidr/macaddr type. |
| `trunc` | fn | ✅ | — | 5 | value truncated to 'scale' of zero | PG 2-arg form: `trunc(x, n)`. Native DataFusion. Same 2-arg concern as round. |

## Text Search Functions and Operators

*Section `functions-textsearch` · Module: [`text_search`](src/text-search.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `array_to_tsvector` | fn | ❌ | P4 | 1 | build tsvector from array of lexemes | Blocked: no tsvector/tsquery type. |
| `get_current_ts_config` | fn | ❌ | P4 | 1 | get current tsearch configuration | Blocked: no tsvector/tsquery type. |
| `json_to_tsvector` | fn | ❌ | P4 | 2 | transform specified values from json to tsvector | Blocked: no tsvector/tsquery type. |
| `jsonb_to_tsvector` | fn | ❌ | P4 | 2 | transform specified values from jsonb to tsvector | Blocked: no tsvector/tsquery type. |
| `length` | fn | ✅ | — | 8 | length of string in specified encoding | Overloaded in PG (text/bytea/bit); DF covers text only. Native DataFusion |
| `numnode` | fn | ❌ | P4 | 1 | number of nodes | Blocked: no tsvector/tsquery type. |
| `phraseto_tsquery` | fn | ❌ | P4 | 2 | transform to tsquery | Blocked: no tsvector/tsquery type. |
| `plainto_tsquery` | fn | ❌ | P4 | 2 | transform to tsquery | Blocked: no tsvector/tsquery type. |
| `querytree` | fn | ❌ | P4 | 1 | show real useful query for GiST index | Blocked: no tsvector/tsquery type. |
| `setweight` | fn | ❌ | P4 | 2 | set given weight for whole tsvector | Blocked: no tsvector/tsquery type. |
| `strip` | fn | ❌ | P4 | 1 | strip position information | Blocked: no tsvector/tsquery type. |
| `to_tsquery` | fn | ❌ | P4 | 2 | make tsquery | Blocked: no tsvector/tsquery type. |
| `to_tsvector` | fn | ❌ | P4 | 6 | transform string values from jsonb to tsvector | Blocked: no tsvector/tsquery type. |
| `ts_debug` | fn | ❌ | P4 | 2 | debug function for current text search configuration | Blocked: no tsvector/tsquery type. |
| `ts_delete` | fn | ❌ | P4 | 2 | delete given lexemes | Blocked: no tsvector/tsquery type. |
| `ts_filter` | fn | ❌ | P4 | 1 | delete lexemes that do not have one of the given weights | Blocked: no tsvector/tsquery type. |
| `ts_headline` | fn | ❌ | P4 | 12 | generate headline from jsonb | Blocked: no tsvector/tsquery type. |
| `ts_lexize` | fn | ❌ | P4 | 1 | normalize one word by dictionary | Blocked: no tsvector/tsquery type. |
| `ts_parse` | fn | ❌ | P4 | 2 | parse text to tokens | Blocked: no tsvector/tsquery type. |
| `ts_rank` | fn | ❌ | P4 | 4 | relevance | Blocked: no tsvector/tsquery type. |
| `ts_rank_cd` | fn | ❌ | P4 | 4 | relevance | Blocked: no tsvector/tsquery type. |
| `ts_rewrite` | fn | ❌ | P4 | 2 | rewrite tsquery | Blocked: no tsvector/tsquery type. |
| `ts_stat` | fn | ❌ | P4 | 2 | statistics of tsvector column | Blocked: no tsvector/tsquery type. |
| `ts_token_type` | fn | ❌ | P4 | 2 | get parser's token types | Blocked: no tsvector/tsquery type. |
| `tsquery_phrase` | fn | ❌ | P4 | 2 | phrase-concatenate with distance | Blocked: no tsvector/tsquery type. |
| `tsvector_to_array` | fn | ❌ | P4 | 1 | convert tsvector to array of lexemes | Blocked: no tsvector/tsquery type. |
| `unnest` | fn | ❌ | P4 | 3 | expand multirange to set of ranges | Blocked: no tsvector/tsquery type. |
| `websearch_to_tsquery` | fn | ❌ | P4 | 2 | transform to tsquery | Blocked: no tsvector/tsquery type. |

## UUID Functions

*Section `functions-uuid` · Module: [`uuid`](src/uuid.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `gen_random_uuid` | fn | 🚧 | P1 | 1 | generate random UUID | Alias of DF `uuid()`. |
| `uuid_extract_timestamp` | fn | 🚧 | P2 | 1 | extract timestamp from UUID |  |
| `uuid_extract_version` | fn | 🚧 | P2 | 1 | extract version from RFC 9562 UUID |  |
| `uuidv4` | fn | 🚧 | P2 | 1 | generate UUID version 4 |  |
| `uuidv7` | fn | 🚧 | P2 | 2 | generate UUID version 7 with a timestamp shifted by specified interval |  |

## XML Functions

*Section `functions-xml` · Module: [`xml`](src/xml.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `cursor_to_xml` | fn | ❌ | P4 | 1 | map rows from cursor to XML | Blocked: no XML type. |
| `cursor_to_xmlschema` | fn | ❌ | P4 | 1 | map cursor structure to XML Schema | Blocked: no XML type. |
| `database_to_xml` | fn | ❌ | P4 | 1 | map database contents to XML | Blocked: no XML type. |
| `database_to_xml_and_xmlschema` | fn | ❌ | P4 | 1 | map database contents and structure to XML and XML Schema | Blocked: no XML type. |
| `database_to_xmlschema` | fn | ❌ | P4 | 1 | map database structure to XML Schema | Blocked: no XML type. |
| `nextval` | fn | ❌ | P4 | 1 | sequence next value | Blocked: no XML type. |
| `query_to_xml` | fn | ❌ | P4 | 1 | map query result to XML | Blocked: no XML type. |
| `query_to_xml_and_xmlschema` | fn | ❌ | P4 | 1 | map query result and structure to XML and XML Schema | Blocked: no XML type. |
| `query_to_xmlschema` | fn | ❌ | P4 | 1 | map query result structure to XML Schema | Blocked: no XML type. |
| `schema_to_xml` | fn | ❌ | P4 | 1 | map schema contents to XML | Blocked: no XML type. |
| `schema_to_xml_and_xmlschema` | fn | ❌ | P4 | 1 | map schema contents and structure to XML and XML Schema | Blocked: no XML type. |
| `schema_to_xmlschema` | fn | ❌ | P4 | 1 | map schema structure to XML Schema | Blocked: no XML type. |
| `string` | fn | ❌ | P4 | 0 | — | Blocked: no XML type. |
| `table_to_xml` | fn | ❌ | P4 | 1 | map table contents to XML | Blocked: no XML type. |
| `table_to_xml_and_xmlschema` | fn | ❌ | P4 | 1 | map table contents and structure to XML and XML Schema | Blocked: no XML type. |
| `table_to_xmlschema` | fn | ❌ | P4 | 1 | map table structure to XML Schema | Blocked: no XML type. |
| `xml_is_well_formed` | fn | ❌ | P4 | 1 | determine if a string is well formed XML | Blocked: no XML type. |
| `xml_is_well_formed_content` | fn | ❌ | P4 | 1 | determine if a string is well formed XML content | Blocked: no XML type. |
| `xml_is_well_formed_document` | fn | ❌ | P4 | 1 | determine if a string is well formed XML document | Blocked: no XML type. |
| `xmlagg` | fn | ❌ | P4 | 1 | concatenate XML values | Blocked: no XML type. |
| `xmlcomment` | fn | ❌ | P4 | 1 | generate XML comment | Blocked: no XML type. |
| `xmlconcat` | fn | ❌ | P4 | 0 | — | Blocked: no XML type. |
| `xmlelement` | fn | ❌ | P4 | 0 | — | Blocked: no XML type. |
| `xmlexists` | fn | ❌ | P4 | 1 | test XML value against XPath expression | Blocked: no XML type. |
| `xmlforest` | fn | ❌ | P4 | 0 | — | Blocked: no XML type. |
| `xmlparse` | fn | ❌ | P4 | 0 | — | Blocked: no XML type. |
| `xmlpi` | fn | ❌ | P4 | 0 | — | Blocked: no XML type. |
| `xmlroot` | fn | ❌ | P4 | 0 | — | Blocked: no XML type. |
| `xmlserialize` | fn | ❌ | P4 | 0 | — | Blocked: no XML type. |
| `xmltable` | fn | ❌ | P4 | 0 | — | Blocked: no XML type. |
| `xmltext` | fn | ❌ | P4 | 1 | generate XML text node | Blocked: no XML type. |
| `xpath` | fn | ❌ | P4 | 2 | evaluate XPath expression, with namespaces support | Blocked: no XML type. |
| `xpath_exists` | fn | ❌ | P4 | 2 | test XML value against XPath expression, with namespace support | Blocked: no XML type. |

## JSON Functions and Operators

*Section `functions-json` · Module: [`json`](src/json.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `array_to_json` | fn | ❌ | P4 | 2 | map array to json with optional pretty printing | Blocked: no JSON/JSONB type in DataFusion. |
| `json` | fn | ❌ | P4 | 0 | — | Blocked: no JSON/JSONB type in DataFusion. |
| `json[b]_populate_record` | fn | ❌ | P4 | 0 | — | Blocked: no JSON/JSONB type in DataFusion. |
| `json_agg` | fn | ❌ | P4 | 1 | aggregate input into json | Blocked: no JSON type. |
| `json_array` | fn | ❌ | P4 | 0 | — | Blocked: no JSON type. |
| `json_array_elements` | fn | ❌ | P4 | 1 | key value pairs of a json object | Blocked: no JSON/JSONB type in DataFusion. |
| `json_array_elements_text` | fn | ❌ | P4 | 1 | elements of json array | Blocked: no JSON/JSONB type in DataFusion. |
| `json_array_length` | fn | ❌ | P4 | 1 | length of json array | Blocked: no JSON/JSONB type in DataFusion. |
| `json_build_array` | fn | ❌ | P4 | 2 | build a json array from any inputs | Blocked: no JSON/JSONB type in DataFusion. |
| `json_build_object` | fn | ❌ | P4 | 2 | build a json object from pairwise key/value inputs | Blocked: no JSON/JSONB type in DataFusion. |
| `json_each` | fn | ❌ | P4 | 1 | key value pairs of a json object | Blocked: no JSON/JSONB type in DataFusion. |
| `json_each_text` | fn | ❌ | P4 | 1 | key value pairs of a json object | Blocked: no JSON/JSONB type in DataFusion. |
| `json_exists` | fn | ❌ | P4 | 0 | — | Blocked: no JSON/JSONB type in DataFusion. |
| `json_extract_path` | both | ❌ | P4 | 1 | get value from json with path elements | Blocked: no JSON/JSONB type in DataFusion. |
| `json_extract_path_text` | both | ❌ | P4 | 1 | get value from json as text with path elements | Blocked: no JSON/JSONB type in DataFusion. |
| `json_object` | fn | ❌ | P4 | 2 | map text arrays of keys and values to json object |  |
| `json_object_agg` | fn | ❌ | P4 | 1 | aggregate input into a json object | Blocked: no JSON type. |
| `json_object_keys` | fn | ❌ | P4 | 1 | get json object keys | Blocked: no JSON/JSONB type in DataFusion. |
| `json_populate_record` | fn | ❌ | P4 | 1 | get record fields from a json object | Blocked: no JSON/JSONB type in DataFusion. |
| `json_populate_recordset` | fn | ❌ | P4 | 1 | get set of records with fields from a json array of objects | Blocked: no JSON/JSONB type in DataFusion. |
| `json_query` | fn | ❌ | P4 | 0 | — | Blocked: no JSON/JSONB type in DataFusion. |
| `json_scalar` | fn | ❌ | P4 | 0 | — | Blocked: no JSON/JSONB type in DataFusion. |
| `json_serialize` | fn | ❌ | P4 | 0 | — | Blocked: no JSON/JSONB type in DataFusion. |
| `json_strip_nulls` | fn | ❌ | P4 | 1 | remove object fields with null values from json | Blocked: no JSON/JSONB type in DataFusion. |
| `json_table` | fn | ❌ | P4 | 0 | — | Blocked: no JSON/JSONB type in DataFusion. |
| `json_to_record` | fn | ❌ | P4 | 1 | get record fields from a json object | Blocked: no JSON/JSONB type in DataFusion. |
| `json_to_recordset` | fn | ❌ | P4 | 1 | get set of records with fields from a json array of objects | Blocked: no JSON/JSONB type in DataFusion. |
| `json_typeof` | fn | ❌ | P4 | 1 | get the type of a json value | Blocked: no JSON/JSONB type in DataFusion. |
| `json_value` | fn | ❌ | P4 | 0 | — | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_agg` | fn | ❌ | P4 | 1 | aggregate input into jsonb | Blocked: no JSON type. |
| `jsonb_array_elements` | fn | ❌ | P4 | 1 | elements of a jsonb array | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_array_elements_text` | fn | ❌ | P4 | 1 | elements of jsonb array | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_array_length` | fn | ❌ | P4 | 1 | length of jsonb array | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_build_array` | fn | ❌ | P4 | 2 | build a jsonb array from any inputs | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_build_object` | fn | ❌ | P4 | 2 | build a jsonb object from pairwise key/value inputs | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_each` | fn | ❌ | P4 | 1 | key value pairs of a jsonb object | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_each_text` | fn | ❌ | P4 | 1 | key value pairs of a jsonb object | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_extract_path` | both | ❌ | P4 | 1 | get value from jsonb with path elements | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_extract_path_text` | both | ❌ | P4 | 1 | get value from jsonb as text with path elements | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_insert` | fn | ❌ | P4 | 1 | Insert value into a jsonb | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_object` | fn | ❌ | P4 | 2 | map text array of key value pairs to jsonb object | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_object_agg` | fn | ❌ | P4 | 1 | aggregate inputs into jsonb object |  |
| `jsonb_object_keys` | fn | ❌ | P4 | 1 | get jsonb object keys | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_path_exists` | fn | ❌ | P4 | 1 | jsonpath exists test | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_path_exists_tz` | fn | ❌ | P4 | 1 | jsonpath exists test with timezone | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_path_match` | fn | ❌ | P4 | 1 | jsonpath match | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_path_match_tz` | fn | ❌ | P4 | 1 | jsonpath match with timezone | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_path_query` | fn | ❌ | P4 | 1 | jsonpath query | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_path_query_array` | fn | ❌ | P4 | 1 | jsonpath query wrapped into array | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_path_query_array_tz` | fn | ❌ | P4 | 1 | jsonpath query wrapped into array with timezone | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_path_query_first` | fn | ❌ | P4 | 1 | jsonpath query first item | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_path_query_first_tz` | fn | ❌ | P4 | 1 | jsonpath query first item with timezone | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_path_query_tz` | fn | ❌ | P4 | 1 | jsonpath query with timezone | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_populate_record` | fn | ❌ | P4 | 1 | get record fields from a jsonb object | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_populate_record_valid` | fn | ❌ | P4 | 1 | test get record fields from a jsonb object | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_populate_recordset` | fn | ❌ | P4 | 1 | get set of records with fields from a jsonb array of objects | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_pretty` | fn | ❌ | P4 | 1 | Indented text from jsonb | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_set` | fn | ❌ | P4 | 1 | Set part of a jsonb | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_set_lax` | fn | ❌ | P4 | 1 | Set part of a jsonb, handle NULL value | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_strip_nulls` | fn | ❌ | P4 | 1 | remove object fields with null values from jsonb | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_to_record` | fn | ❌ | P4 | 1 | get record fields from a jsonb object | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_to_recordset` | fn | ❌ | P4 | 1 | get set of records with fields from a jsonb array of objects | Blocked: no JSON/JSONB type in DataFusion. |
| `jsonb_typeof` | fn | ❌ | P4 | 1 | get the type of a jsonb value | Blocked: no JSON/JSONB type in DataFusion. |
| `row_to_json` | fn | ❌ | P4 | 2 | map row to json with optional pretty printing | Blocked: no JSON/JSONB type in DataFusion. |
| `to_json` | fn | ❌ | P4 | 1 | map input to json | Blocked: no JSON type. |
| `to_jsonb` | fn | ❌ | P4 | 1 | map input to jsonb | Blocked: no JSON type. |
| `to_timestamp` | fn | ✅ | — | 2 | convert text to timestamp with time zone | Native DataFusion |

## Sequence Manipulation Functions

*Section `functions-sequence` · Module: [`sequence`](src/sequence.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `currval` | fn | ❌ | P4 | 1 | sequence current value | Out of scope: server-side mutable state. |
| `lastval` | fn | ❌ | P4 | 1 | current value from last used sequence | Out of scope: server-side mutable state. |
| `nextval` | fn | ❌ | P4 | 1 | sequence next value | Out of scope: server-side mutable state. |
| `setval` | fn | ❌ | P4 | 2 | set sequence value and is_called status | Out of scope: server-side mutable state. |

## Array Functions and Operators

*Section `functions-array` · Module: [`array`](src/array.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `array_agg` | fn | ✅ | — | 2 | concatenate aggregate input into an array | Native DataFusion |
| `array_append` | fn | ✅ | — | 1 | append element onto end of array | Native DataFusion |
| `array_cat` | fn | ✅ | — | 1 | — | Native DataFusion |
| `array_dims` | fn | ✅ | — | 1 | array dimensions | Native DataFusion |
| `array_fill` | fn | 🚧 | P2 | 2 | array constructor with value |  |
| `array_length` | fn | ✅ | — | 1 | array length | Native DataFusion |
| `array_lower` | fn | 🔧 | — | 1 | array lower dimension | In datafusion-pg-catalog |
| `array_ndims` | fn | ✅ | — | 1 | number of array dimensions | Native DataFusion |
| `array_position` | fn | ✅ | — | 2 | returns an offset of value in array with start index | Native DataFusion |
| `array_positions` | fn | ✅ | — | 1 | returns an array of offsets of some value in array | Native DataFusion |
| `array_prepend` | fn | ✅ | — | 1 | prepend element onto front of array | Native DataFusion |
| `array_remove` | fn | ✅ | — | 1 | remove any occurrences of an element from an array | Native DataFusion |
| `array_replace` | fn | ✅ | — | 1 | replace any occurrences of an element in an array | Native DataFusion |
| `array_reverse` | fn | ✅ | — | 1 | reverse array | Native DataFusion |
| `array_sample` | fn | 🚧 | P2 | 1 | take samples from array |  |
| `array_shuffle` | fn | 🚧 | P2 | 1 | shuffle array |  |
| `array_sort` | fn | ✅ | — | 3 | sort array | Native DataFusion |
| `array_to_string` | fn | ✅ | — | 2 | concatenate array elements, using delimiter and null string, into text | Native DataFusion |
| `array_upper` | fn | 🔧 | — | 1 | array upper dimension | In datafusion-pg-catalog |
| `cardinality` | fn | ✅ | — | 1 | array cardinality | Native DataFusion |
| `string_to_array` | fn | ✅ | — | 2 | split delimited text, with null string | Native DataFusion |
| `trim_array` | fn | 🚧 | P2 | 1 | remove last N elements of array | PG 14+. |
| `unnest` | fn | 🚧 | P2 | 3 | expand multirange to set of ranges |  |

## Range/Multirange Functions and Operators

*Section `functions-range` · Module: [`range`](src/range.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `isempty` | fn | ❌ | P4 | 2 | is the multirange empty? | Blocked: no range/multirange type. |
| `lower` | fn | ✅ | — | 3 | lower bound of multirange | Native DataFusion |
| `lower_inc` | fn | ❌ | P4 | 2 | is the multirange's lower bound inclusive? | Blocked: no range/multirange type. |
| `lower_inf` | fn | ❌ | P4 | 2 | is the multirange's lower bound infinite? | Blocked: no range/multirange type. |
| `multirange` | fn | ❌ | P4 | 1 | anymultirange cast | Blocked: no range/multirange type. |
| `range_merge` | fn | ❌ | P4 | 2 | the smallest range which includes both of the given ranges | Blocked: no range/multirange type. |
| `unnest` | fn | ❌ | P4 | 3 | expand multirange to set of ranges | Blocked: no range/multirange type. |
| `upper` | fn | ✅ | — | 3 | upper bound of multirange | Native DataFusion |
| `upper_inc` | fn | ❌ | P4 | 2 | is the multirange's upper bound inclusive? | Blocked: no range/multirange type. |
| `upper_inf` | fn | ❌ | P4 | 2 | is the multirange's upper bound infinite? | Blocked: no range/multirange type. |

## Set Returning Functions

*Section `functions-srf` · (set-returning; distributed)*

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `generate_series` | fn | ✅ | — | 9 | non-persistent series generator | Native DataFusion |
| `generate_subscripts` | fn | 🚧 | P2 | 2 | array subscripts generator | Array subscript generator. |
| `unnest` | fn | 🚧 | P2 | 3 | expand multirange to set of ranges |  |

## Aggregate Functions

*Section `functions-aggregate` · (aggregates; mostly DataFusion core)*

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `any` | fn | 🚧 | P3 | 0 | — | PG keyword form of `bool_or`. |
| `any_value` | fn | 🚧 | P3 | 1 | arbitrary value from among input values |  |
| `array_agg` | fn | ✅ | — | 2 | concatenate aggregate input into an array | Native DataFusion |
| `avg` | fn | ✅ | — | 7 | the average (arithmetic mean) as interval of all interval values | Native DataFusion |
| `bit_and` | fn | ✅ | — | 4 | bitwise-and smallint aggregate | Native DataFusion |
| `bit_or` | fn | ✅ | — | 4 | bitwise-or smallint aggregate | Native DataFusion |
| `bit_xor` | fn | ✅ | — | 4 | bitwise-xor smallint aggregate | Native DataFusion |
| `bool_and` | fn | ✅ | — | 1 | boolean-and aggregate | Native DataFusion |
| `bool_or` | fn | ✅ | — | 1 | boolean-or aggregate | Native DataFusion |
| `coalesce` | fn | ✅ | — | 0 | First non-NULL argument | Native DataFusion (SQL special form). |
| `corr` | fn | ✅ | — | 1 | correlation coefficient | Native DataFusion |
| `count` | fn | ✅ | — | 2 | number of input rows for which the input expression is not null | Native DataFusion |
| `covar_pop` | fn | ✅ | — | 1 | population covariance | Native DataFusion |
| `covar_samp` | fn | ✅ | — | 1 | sample covariance | Native DataFusion |
| `cume_dist` | fn | ✅ | — | 2 | cumulative distribution of hypothetical row | Native DataFusion |
| `dense_rank` | fn | ✅ | — | 2 | rank of hypothetical row without gaps | Native DataFusion |
| `every` | fn | 🚧 | P1 | 1 | boolean-and aggregate | Alias of `bool_and`. |
| `grouping` | fn | ✅ | — | 0 | — | Native DataFusion (SQL special form). |
| `json_agg` | fn | ❌ | P4 | 1 | aggregate input into json | Blocked: no JSON type. |
| `json_agg_strict` | fn | ❌ | P4 | 1 | aggregate input into json | Blocked: no JSON type. |
| `json_array` | fn | ❌ | P4 | 0 | — | Blocked: no JSON type. |
| `json_arrayagg` | fn | ❌ | P4 | 0 | — |  |
| `json_object` | fn | ❌ | P4 | 2 | map text arrays of keys and values to json object |  |
| `json_object_agg` | fn | ❌ | P4 | 1 | aggregate input into a json object | Blocked: no JSON type. |
| `json_object_agg_strict` | fn | ❌ | P4 | 1 | aggregate non-NULL input into a json object |  |
| `json_object_agg_unique` | fn | ❌ | P4 | 1 | aggregate input into a json object with unique keys |  |
| `json_object_agg_unique_strict` | fn | ❌ | P4 | 1 | aggregate non-NULL input into a json object with unique keys |  |
| `json_objectagg` | fn | ❌ | P4 | 0 | — |  |
| `jsonb_agg` | fn | ❌ | P4 | 1 | aggregate input into jsonb | Blocked: no JSON type. |
| `jsonb_agg_strict` | fn | ❌ | P4 | 1 | aggregate input into jsonb skipping nulls |  |
| `jsonb_object_agg` | fn | ❌ | P4 | 1 | aggregate inputs into jsonb object |  |
| `jsonb_object_agg_strict` | fn | ❌ | P4 | 1 | aggregate non-NULL inputs into jsonb object |  |
| `jsonb_object_agg_unique` | fn | ❌ | P4 | 1 | aggregate inputs into jsonb object checking key uniqueness |  |
| `jsonb_object_agg_unique_strict` | fn | ❌ | P4 | 1 | aggregate non-NULL inputs into jsonb object checking key uniqueness |  |
| `max` | fn | ✅ | — | 24 | maximum value of all timestamp with time zone input values | Native DataFusion |
| `min` | fn | ✅ | — | 24 | minimum value of all timestamp with time zone input values | Native DataFusion |
| `mode` | fn | 🚧 | P3 | 1 | most common value | Ordered-set aggregate. |
| `percent_rank` | fn | ✅ | — | 2 | fractional rank of hypothetical row | Native DataFusion |
| `percentile_cont` | fn | 🚧 | P3 | 4 | continuous distribution percentile | Ordered-set aggregate; DF has approx variant. Native DataFusion |
| `percentile_disc` | fn | 🚧 | P3 | 2 | multiple discrete percentiles |  |
| `range_agg` | fn | ❌ | P4 | 2 | combine aggregate input into a multirange | Blocked: no range type. |
| `range_intersect_agg` | fn | ❌ | P4 | 2 | range aggregate by intersecting |  |
| `rank` | fn | ✅ | — | 2 | rank of hypothetical row | Native DataFusion |
| `regr_avgx` | fn | ✅ | — | 1 | average of the independent variable (sum(X)/N) | Native DataFusion |
| `regr_avgy` | fn | ✅ | — | 1 | average of the dependent variable (sum(Y)/N) | Native DataFusion |
| `regr_count` | fn | ✅ | — | 1 | number of input rows in which both expressions are not null | Native DataFusion |
| `regr_intercept` | fn | ✅ | — | 1 | y-intercept of the least-squares-fit linear equation determined by the (X, Y) pairs | Native DataFusion |
| `regr_r2` | fn | ✅ | — | 1 | square of the correlation coefficient | Native DataFusion |
| `regr_slope` | fn | ✅ | — | 1 | slope of the least-squares-fit linear equation determined by the (X, Y) pairs | Native DataFusion |
| `regr_sxx` | fn | ✅ | — | 1 | sum of squares of the independent variable (sum(X^2) - sum(X)^2/N) | Native DataFusion |
| `regr_sxy` | fn | ✅ | — | 1 | sum of products of independent times dependent variable (sum(X*Y) - sum(X) * sum(Y)/N) | Native DataFusion |
| `regr_syy` | fn | ✅ | — | 1 | sum of squares of the dependent variable (sum(Y^2) - sum(Y)^2/N) | Native DataFusion |
| `some` | fn | 🚧 | P3 | 0 | — | PG keyword form of `bool_or`. |
| `stddev` | fn | ✅ | — | 6 | historical alias for stddev_samp | Native DataFusion |
| `stddev_pop` | fn | ✅ | — | 6 | population standard deviation of smallint input values | Native DataFusion |
| `stddev_samp` | fn | ✅ | — | 6 | sample standard deviation of smallint input values | Native DataFusion |
| `string_agg` | fn | ✅ | — | 2 | concatenate aggregate input into a string | Native DataFusion |
| `sum` | fn | ✅ | — | 8 | sum as interval across all interval input values | Native DataFusion |
| `to_json` | fn | ❌ | P4 | 1 | map input to json | Blocked: no JSON type. |
| `to_jsonb` | fn | ❌ | P4 | 1 | map input to jsonb | Blocked: no JSON type. |
| `var_pop` | fn | ✅ | — | 6 | population variance of smallint input values (square of the population standard deviation) | Native DataFusion |
| `var_samp` | fn | ✅ | — | 6 | sample variance of smallint input values (square of the sample standard deviation) | Native DataFusion |
| `variance` | fn | 🚧 | P1 | 6 | historical alias for var_samp | Alias of `var_samp`. |
| `xmlagg` | fn | ❌ | P4 | 1 | concatenate XML values | Blocked: no XML type. |

## Window Functions

*Section `functions-window` · (windows; mostly DataFusion core)*

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `cume_dist` | fn | ✅ | — | 2 | cumulative distribution of hypothetical row | Native DataFusion |
| `dense_rank` | fn | ✅ | — | 2 | rank of hypothetical row without gaps | Native DataFusion |
| `first_value` | fn | ✅ | — | 1 | fetch the first row value | Native DataFusion |
| `lag` | fn | ✅ | — | 3 | fetch the Nth preceding row value with default | Native DataFusion |
| `last_value` | fn | ✅ | — | 1 | fetch the last row value | Native DataFusion |
| `lead` | fn | ✅ | — | 3 | fetch the Nth following row value with default | Native DataFusion |
| `nth_value` | fn | ✅ | — | 1 | fetch the Nth row value | Native DataFusion |
| `ntile` | fn | ✅ | — | 1 | split rows into N groups | Native DataFusion |
| `percent_rank` | fn | ✅ | — | 2 | fractional rank of hypothetical row | Native DataFusion |
| `rank` | fn | ✅ | — | 2 | rank of hypothetical row | Native DataFusion |
| `row_number` | fn | ✅ | — | 1 | row number within partition | Native DataFusion |

## Merge Support Functions

*Section `functions-merge-support` · Module: [`system`](src/system.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `merge_action` | fn | ❌ | P4 | 0 | — | Out of scope: MERGE statement support. |

## Statistics Information Functions

*Section `functions-statistics` · Module: [`system`](src/system.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `pg_mcv_list_items` | fn | ❌ | P4 | 1 | details about MCV list items | Out of scope: planner internals. |

## System Information Functions and Operators

*Section `functions-info` · Module: [`system`](src/system.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `acldefault` | fn | 🚧 | P3 | 1 | show hardwired default privileges, primarily for use by the information schema |  |
| `aclexplode` | fn | 🚧 | P3 | 1 | convert ACL item array to table, primarily for use by information schema |  |
| `age` | fn | 🚧 | P2 | 5 | age of a transaction ID, in transactions before current transaction |  |
| `col_description` | fn | 🚧 | P3 | 1 | get description for table column |  |
| `collation for` | fn | 🚧 | P3 | 0 | Collation of expression |  |
| `current_catalog` | fn | 🚧 | P2 | 0 | Name of current database | Catalog name as constant. |
| `current_database` | fn | 🔧 | — | 1 | name of the current database | In datafusion-pg-catalog |
| `current_query` | fn | 🚧 | P3 | 1 | get the currently executing query |  |
| `current_role` | fn | 🚧 | P2 | 0 | Equivalent to current_user | Alias of `current_user`. |
| `current_schema` | fn | 🔧 | — | 1 | current schema name | In datafusion-pg-catalog |
| `current_schemas` | fn | 🔧 | — | 1 | current schema search list | In datafusion-pg-catalog |
| `current_user` | fn | 🚧 | P2 | 1 | current user name | Session constant. |
| `format_type` | fn | 🔧 | — | 1 | format a type oid and atttypmod to canonical SQL | In datafusion-pg-catalog |
| `has_any_column_privilege` | fn | 🔧 | — | 6 | user privilege on any column by user oid, rel name | In datafusion-pg-catalog |
| `has_column_privilege` | fn | 🚧 | P3 | 12 | user privilege on column by user oid, rel name, col attnum |  |
| `has_database_privilege` | fn | 🔧 | — | 6 | user privilege on database by user oid, database name | In datafusion-pg-catalog |
| `has_foreign_data_wrapper_privilege` | fn | 🚧 | P3 | 6 | user privilege on foreign data wrapper by user oid, foreign data wrapper name |  |
| `has_function_privilege` | fn | 🚧 | P3 | 6 | user privilege on function by user oid, function name |  |
| `has_language_privilege` | fn | 🚧 | P3 | 6 | user privilege on language by user oid, language name |  |
| `has_largeobject_privilege` | fn | 🚧 | P3 | 3 | user privilege on large object by user oid, large object oid |  |
| `has_parameter_privilege` | fn | 🚧 | P3 | 3 | user privilege on parameter by user oid, parameter name |  |
| `has_schema_privilege` | fn | 🔧 | — | 6 | user privilege on schema by user oid, schema name | In datafusion-pg-catalog |
| `has_sequence_privilege` | fn | 🚧 | P3 | 6 | user privilege on sequence by user oid, seq name |  |
| `has_server_privilege` | fn | 🚧 | P3 | 6 | user privilege on server by user oid, server name |  |
| `has_table_privilege` | fn | 🔧 | — | 6 | user privilege on relation by user oid, rel name | In datafusion-pg-catalog |
| `has_tablespace_privilege` | fn | 🚧 | P3 | 6 | user privilege on tablespace by user oid, tablespace name |  |
| `has_type_privilege` | fn | 🚧 | P3 | 6 | user privilege on type by user oid, type name |  |
| `icu_unicode_version` | fn | ❌ | P4 | 1 | Unicode version used by ICU, if enabled | ICU integration. |
| `inet_client_addr` | fn | ❌ | P4 | 1 | inet address of the client | Blocked: no inet type. |
| `inet_client_port` | fn | ❌ | P4 | 1 | client's port number for this connection | Blocked: no inet type. |
| `inet_server_addr` | fn | ❌ | P4 | 1 | inet address of the server | Blocked: no inet type. |
| `inet_server_port` | fn | ❌ | P4 | 1 | server's port number for this connection | Blocked: no inet type. |
| `makeaclitem` | fn | ❌ | P4 | 1 | make ACL item | ACL internals. |
| `mxid_age` | fn | ❌ | P4 | 1 | age of a multi-transaction ID, in multi-transactions before current multi-transaction | Multi-transaction machinery. |
| `obj_description` | fn | 🚧 | P3 | 2 | get description for object id and catalog name |  |
| `pg_available_wal_summaries` | fn | ❌ | P4 | 1 | list of available WAL summary files | WAL internals. |
| `pg_backend_pid` | fn | 🔧 | — | 1 | statistics: current backend PID | In datafusion-pg-catalog |
| `pg_basetype` | fn | ❌ | P4 | 1 | base type of a domain type | Domain type machinery. |
| `pg_blocking_pids` | fn | ❌ | P4 | 1 | get array of PIDs of sessions blocking specified backend PID from acquiring a heavyweight lock | Backend introspection. |
| `pg_char_to_encoding` | fn | 🚧 | P3 | 1 | convert encoding name to encoding id |  |
| `pg_collation_is_visible` | fn | 🚧 | P3 | 1 | is collation visible in search path? |  |
| `pg_conf_load_time` | fn | ❌ | P4 | 1 | configuration load time | Server uptime. |
| `pg_control_checkpoint` | fn | ❌ | P4 | 1 | pg_controldata checkpoint state information as a function | Control file. |
| `pg_control_init` | fn | ❌ | P4 | 1 | pg_controldata init state information as a function | Control file. |
| `pg_control_recovery` | fn | ❌ | P4 | 1 | pg_controldata recovery state information as a function | Control file. |
| `pg_control_system` | fn | ❌ | P4 | 1 | pg_controldata general state information as a function | Control file. |
| `pg_conversion_is_visible` | fn | 🚧 | P3 | 1 | is conversion visible in search path? |  |
| `pg_current_logfile` | fn | ❌ | P4 | 2 | current logging collector file location | Server log file access. |
| `pg_current_snapshot` | fn | ❌ | P4 | 1 | get current snapshot | Transaction snapshot. |
| `pg_current_xact_id` | fn | ❌ | P4 | 1 | get current transaction ID | No MVCC transaction IDs. |
| `pg_current_xact_id_if_assigned` | fn | ❌ | P4 | 1 | get current transaction ID |  |
| `pg_describe_object` | fn | ❌ | P4 | 1 | get identification of SQL object | Catalog introspection. |
| `pg_encoding_to_char` | fn | 🔧 | — | 1 | convert encoding id to encoding name | In datafusion-pg-catalog |
| `pg_function_is_visible` | fn | 🚧 | P3 | 1 | is function visible in search path? |  |
| `pg_get_acl` | fn | 🚧 | P3 | 1 | get ACL for SQL object |  |
| `pg_get_catalog_foreign_keys` | fn | 🚧 | P3 | 1 | list of catalog foreign key relationships |  |
| `pg_get_constraintdef` | fn | 🔧 | — | 2 | constraint description with pretty-print option | In datafusion-pg-catalog |
| `pg_get_expr` | fn | 🔧 | — | 2 | deparse an encoded expression with pretty-print option | In datafusion-pg-catalog |
| `pg_get_function_arguments` | fn | 🚧 | P3 | 1 | argument list of a function |  |
| `pg_get_function_identity_arguments` | fn | 🚧 | P3 | 1 | identity argument list of a function |  |
| `pg_get_function_result` | fn | 🚧 | P3 | 1 | result type of a function |  |
| `pg_get_functiondef` | fn | 🚧 | P3 | 1 | definition of a function |  |
| `pg_get_indexdef` | fn | 🚧 | P3 | 2 | index description (full create statement or single expression) with pretty-print option |  |
| `pg_get_keywords` | fn | 🚧 | P2 | 1 | list of SQL keywords | Static lookup table. |
| `pg_get_loaded_modules` | fn | ❌ | P4 | 1 | get info about loaded modules |  |
| `pg_get_multixact_members` | fn | ❌ | P4 | 1 | view members of a multixactid |  |
| `pg_get_object_address` | fn | ❌ | P4 | 1 | get OID-based object address from name/args arrays |  |
| `pg_get_owned_sequence` | fn | ❌ | P4 | 0 | — | Sequence internals. |
| `pg_get_partition_constraintdef` | fn | 🚧 | P3 | 1 | partition constraint description |  |
| `pg_get_partkeydef` | fn | 🔧 | — | 1 | partition key description | In datafusion-pg-catalog |
| `pg_get_ruledef` | fn | 🚧 | P3 | 2 | source text of a rule with pretty-print option |  |
| `pg_get_serial_sequence` | fn | 🚧 | P3 | 1 | name of sequence for a serial column |  |
| `pg_get_statisticsobjdef` | fn | 🚧 | P3 | 1 | extended statistics object description |  |
| `pg_get_triggerdef` | fn | ❌ | P4 | 2 | trigger description with pretty-print option | Trigger internals. |
| `pg_get_userbyid` | fn | 🔧 | — | 1 | role name by OID (with fallback) | In datafusion-pg-catalog |
| `pg_get_viewdef` | fn | 🚧 | P3 | 5 | select statement of a view with pretty-printing and specified line wrapping |  |
| `pg_get_wal_summarizer_state` | fn | ❌ | P4 | 1 | WAL summarizer state | WAL internals. |
| `pg_has_role` | fn | 🚧 | P3 | 6 | user privilege on role by user oid, role name |  |
| `pg_identify_object` | fn | ❌ | P4 | 1 | get machine-parseable identification of SQL object |  |
| `pg_identify_object_as_address` | fn | ❌ | P4 | 1 | get identification of SQL object for pg_get_object_address() |  |
| `pg_index_column_has_property` | fn | ❌ | P4 | 1 | test property of an index column | Index introspection. |
| `pg_index_has_property` | fn | ❌ | P4 | 1 | test property of an index |  |
| `pg_indexam_has_property` | fn | ❌ | P4 | 1 | test property of an index access method |  |
| `pg_input_error_info` | fn | 🚧 | P2 | 1 | get error details if string is not valid input for data type |  |
| `pg_input_is_valid` | fn | 🚧 | P2 | 1 | test whether string is valid input for data type | Try-cast; feasible. |
| `pg_is_other_temp_schema` | fn | ❌ | P4 | 1 | is schema another session's temp schema? | Temp schema. |
| `pg_jit_available` | fn | ❌ | P4 | 1 | Is JIT compilation available in this session? | No JIT in DataFusion. |
| `pg_last_committed_xact` | fn | ❌ | P4 | 1 | get transaction Id, commit timestamp and replication origin of latest transaction commit | Commit timestamp infrastructure. |
| `pg_listening_channels` | fn | ❌ | P4 | 1 | get the channels that the current backend listens to | LISTEN/NOTIFY; not applicable. |
| `pg_my_temp_schema` | fn | ❌ | P4 | 1 | get OID of current session's temp schema, if any | Temp schema. |
| `pg_notification_queue_usage` | fn | ❌ | P4 | 1 | get the fraction of the asynchronous notification queue currently in use | LISTEN/NOTIFY; not applicable. |
| `pg_numa_available` | fn | ❌ | P4 | 1 | Is NUMA support available? | Server topology. |
| `pg_opclass_is_visible` | fn | 🚧 | P3 | 1 | is opclass visible in search path? |  |
| `pg_operator_is_visible` | fn | 🚧 | P3 | 1 | is operator visible in search path? |  |
| `pg_opfamily_is_visible` | fn | 🚧 | P3 | 1 | is opfamily visible in search path? |  |
| `pg_options_to_table` | fn | ❌ | P4 | 1 | convert generic options array to name/value table | Foreign-data-wrapper internals. |
| `pg_postmaster_start_time` | fn | ❌ | P4 | 1 | postmaster start time | Server uptime. |
| `pg_safe_snapshot_blocking_pids` | fn | ❌ | P4 | 1 | get array of PIDs of sessions blocking specified backend PID from acquiring a safe snapshot | Backend introspection. |
| `pg_settings_get_flags` | fn | ❌ | P4 | 1 | return flags for specified GUC |  |
| `pg_snapshot_xip` | fn | ❌ | P4 | 1 | get set of in-progress transactions in snapshot | Transaction snapshot. |
| `pg_snapshot_xmax` | fn | ❌ | P4 | 1 | get xmax of snapshot | Transaction snapshot. |
| `pg_snapshot_xmin` | fn | ❌ | P4 | 1 | get xmin of snapshot | Transaction snapshot. |
| `pg_statistics_obj_is_visible` | fn | 🚧 | P3 | 1 | is statistics object visible in search path? |  |
| `pg_table_is_visible` | fn | 🔧 | — | 1 | is table visible in search path? | In datafusion-pg-catalog |
| `pg_tablespace_databases` | fn | ❌ | P4 | 1 | get OIDs of databases in a tablespace |  |
| `pg_tablespace_location` | fn | ❌ | P4 | 1 | tablespace location |  |
| `pg_trigger_depth` | fn | ❌ | P4 | 1 | current trigger depth | Trigger internals. |
| `pg_ts_config_is_visible` | fn | 🚧 | P3 | 1 | is text search configuration visible in search path? |  |
| `pg_ts_dict_is_visible` | fn | 🚧 | P3 | 1 | is text search dictionary visible in search path? |  |
| `pg_ts_parser_is_visible` | fn | 🚧 | P3 | 1 | is text search parser visible in search path? |  |
| `pg_ts_template_is_visible` | fn | 🚧 | P3 | 1 | is text search template visible in search path? |  |
| `pg_type_is_visible` | fn | 🚧 | P3 | 1 | is type visible in search path? |  |
| `pg_typeof` | fn | 🚧 | P2 | 1 | type of the argument | High-value; introspects expression type. |
| `pg_visible_in_snapshot` | fn | ❌ | P4 | 1 | is xid8 visible in snapshot? | Snapshot machinery. |
| `pg_wal_summary_contents` | fn | ❌ | P4 | 1 | contents of a WAL summary file | WAL internals. |
| `pg_xact_commit_timestamp` | fn | ❌ | P4 | 1 | get commit timestamp of a transaction |  |
| `pg_xact_commit_timestamp_origin` | fn | ❌ | P4 | 1 | get commit timestamp and replication origin of a transaction |  |
| `pg_xact_status` | fn | ❌ | P4 | 1 | commit status of transaction | Transaction status. |
| `row_security_active` | fn | ❌ | P4 | 2 | row security for current context active on table by table name | Row security machinery. |
| `session_user` | fn | 🔧 | — | 1 | session user name | In datafusion-pg-catalog |
| `shobj_description` | fn | 🚧 | P3 | 1 | get description for object id and shared catalog name |  |
| `system_user` | fn | ❌ | P4 | 1 | system user name | Requires auth integration. |
| `to_regclass` | fn | 🚧 | P3 | 1 | convert classname to regclass |  |
| `to_regcollation` | fn | 🚧 | P3 | 1 | convert classname to regcollation |  |
| `to_regnamespace` | fn | 🚧 | P3 | 1 | convert namespace name to regnamespace |  |
| `to_regoper` | fn | 🚧 | P3 | 1 | convert operator name to regoper |  |
| `to_regoperator` | fn | 🚧 | P3 | 1 | convert operator name to regoperator |  |
| `to_regproc` | fn | 🚧 | P3 | 1 | convert proname to regproc |  |
| `to_regprocedure` | fn | 🚧 | P3 | 1 | convert proname to regprocedure |  |
| `to_regrole` | fn | 🚧 | P3 | 1 | convert role name to regrole |  |
| `to_regtype` | fn | 🚧 | P3 | 1 | convert type name to regtype |  |
| `to_regtypemod` | fn | 🚧 | P3 | 1 | convert type name to type modifier |  |
| `txid_current` | fn | ❌ | P4 | 1 | get current transaction ID | No MVCC transaction IDs. |
| `txid_current_if_assigned` | fn | ❌ | P4 | 1 | get current transaction ID |  |
| `txid_current_snapshot` | fn | ❌ | P4 | 1 | get current snapshot |  |
| `txid_snapshot_xip` | fn | ❌ | P4 | 1 | get set of in-progress txids in snapshot |  |
| `txid_snapshot_xmax` | fn | ❌ | P4 | 1 | get xmax of snapshot |  |
| `txid_snapshot_xmin` | fn | ❌ | P4 | 1 | get xmin of snapshot |  |
| `txid_status` | fn | ❌ | P4 | 1 | commit status of transaction |  |
| `txid_visible_in_snapshot` | fn | ❌ | P4 | 1 | is txid visible in snapshot? |  |
| `unicode_version` | fn | 🚧 | P3 | 1 | Unicode version used by Postgres |  |
| `user` | fn | 🚧 | P1 | 0 | Equivalent to current_user | Alias of `current_user`. |
| `version` | fn | ✅ | — | 1 | PostgreSQL version string | Native DataFusion |

## System Administration Functions

*Section `functions-admin` · Module: [`system`](src/system.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `brin_desummarize_range` | fn | ❌ | P4 | 1 | brin: desummarize page range | BRIN index internals. |
| `brin_summarize_new_values` | fn | ❌ | P4 | 1 | brin: standalone scan new table pages |  |
| `brin_summarize_range` | fn | ❌ | P4 | 1 | brin: standalone scan new table pages |  |
| `convert_from` | fn | 🚧 | P4 | 1 | convert string with specified source encoding name |  |
| `current_setting` | fn | 🚧 | P2 | 2 | SHOW X as a function, optionally no error for missing variable | Shim-able via DataFusion `SessionConfig`. |
| `gin_clean_pending_list` | fn | ❌ | P4 | 1 | clean up GIN pending list | GIN index internals. |
| `pg_advisory_lock` | fn | ❌ | P4 | 2 | obtain exclusive advisory lock | Advisory locks; no shared lock manager. |
| `pg_advisory_lock_shared` | fn | ❌ | P4 | 2 | obtain shared advisory lock | Advisory locks; no shared lock manager. |
| `pg_advisory_unlock` | fn | ❌ | P4 | 2 | release exclusive advisory lock | Advisory locks; no shared lock manager. |
| `pg_advisory_unlock_all` | fn | ❌ | P4 | 1 | release all advisory locks | Advisory locks; no shared lock manager. |
| `pg_advisory_unlock_shared` | fn | ❌ | P4 | 2 | release shared advisory lock | Advisory locks; no shared lock manager. |
| `pg_advisory_xact_lock` | fn | ❌ | P4 | 2 | obtain exclusive advisory lock | Advisory locks; no shared lock manager. |
| `pg_advisory_xact_lock_shared` | fn | ❌ | P4 | 2 | obtain shared advisory lock | Advisory locks; no shared lock manager. |
| `pg_backup_start` | fn | ❌ | P4 | 1 | prepare for taking an online backup | Online backup machinery. |
| `pg_backup_stop` | fn | ❌ | P4 | 1 | finish taking an online backup |  |
| `pg_cancel_backend` | fn | ❌ | P4 | 1 | cancel a server process' current query | Backend control; no multi-process backend. |
| `pg_clear_attribute_stats` | fn | ❌ | P4 | 1 | clear statistics on attribute |  |
| `pg_clear_relation_stats` | fn | ❌ | P4 | 1 | clear statistics on relation |  |
| `pg_collation_actual_version` | fn | ❌ | P4 | 1 | get actual version of collation from operating system |  |
| `pg_column_compression` | fn | ❌ | P4 | 1 | compression method for the compressed datum | Storage internals. |
| `pg_column_size` | fn | 🚧 | P2 | 1 | bytes required to store the value, perhaps with compression | Use arrow array byte-size. |
| `pg_column_toast_chunk_id` | fn | ❌ | P4 | 1 | chunk ID of on-disk TOASTed value | TOAST internals. |
| `pg_copy_logical_replication_slot` | fn | ❌ | P4 | 3 | copy a logical replication slot, changing temporality and plugin |  |
| `pg_copy_physical_replication_slot` | fn | ❌ | P4 | 2 | copy a physical replication slot, changing temporality |  |
| `pg_create_logical_replication_slot` | fn | ❌ | P4 | 1 | set up a logical replication slot | Logical decoding. |
| `pg_create_physical_replication_slot` | fn | ❌ | P4 | 1 | create a physical replication slot | Replication. |
| `pg_create_restore_point` | fn | ❌ | P4 | 1 | create a named restore point | WAL. |
| `pg_current_wal_flush_lsn` | fn | ❌ | P4 | 1 | current wal flush location | WAL/replication internals. |
| `pg_current_wal_insert_lsn` | fn | ❌ | P4 | 1 | current wal insert location | WAL/replication internals. |
| `pg_current_wal_lsn` | fn | ❌ | P4 | 1 | current wal write location | WAL/replication internals. |
| `pg_database_collation_actual_version` | fn | ❌ | P4 | 1 | get actual version of database collation from operating system |  |
| `pg_database_size` | fn | ❌ | P4 | 2 | total disk space usage for the specified database |  |
| `pg_drop_replication_slot` | fn | ❌ | P4 | 1 | drop a replication slot | Replication. |
| `pg_export_snapshot` | fn | ❌ | P4 | 1 | export a snapshot | Snapshot export. |
| `pg_filenode_relation` | fn | ❌ | P4 | 1 | relation OID for filenode and tablespace |  |
| `pg_get_wal_replay_pause_state` | fn | ❌ | P4 | 1 | get wal replay pause state | WAL internals. |
| `pg_get_wal_resource_managers` | fn | ❌ | P4 | 1 | get resource managers loaded in system | WAL internals. |
| `pg_import_system_collations` | fn | ❌ | P4 | 1 | import collations from operating system |  |
| `pg_indexes_size` | fn | ❌ | P4 | 1 | disk space usage for all indexes attached to the specified table |  |
| `pg_is_in_recovery` | fn | ❌ | P4 | 1 | true if server is in recovery | Replication mode. |
| `pg_is_wal_replay_paused` | fn | ❌ | P4 | 1 | true if wal replay is paused | WAL replay. |
| `pg_last_wal_receive_lsn` | fn | ❌ | P4 | 1 | current wal flush location | WAL/replication internals. |
| `pg_last_wal_replay_lsn` | fn | ❌ | P4 | 1 | last wal replay location | WAL/replication internals. |
| `pg_last_xact_replay_timestamp` | fn | 🚧 | P4 | 1 | timestamp of last replay xact |  |
| `pg_log_backend_memory_contexts` | fn | ❌ | P4 | 1 | log memory contexts of the specified backend | Backend introspection. |
| `pg_log_standby_snapshot` | fn | 🚧 | P4 | 1 | log details of the current snapshot to WAL |  |
| `pg_logical_emit_message` | fn | ❌ | P4 | 2 | emit a textual logical decoding message | Logical decoding. |
| `pg_logical_slot_get_binary_changes` | fn | ❌ | P4 | 1 | get binary changes from replication slot |  |
| `pg_logical_slot_get_changes` | fn | ❌ | P4 | 1 | get changes from replication slot |  |
| `pg_logical_slot_peek_binary_changes` | fn | ❌ | P4 | 1 | peek at binary changes from replication slot |  |
| `pg_logical_slot_peek_changes` | fn | ❌ | P4 | 1 | peek at changes from replication slot |  |
| `pg_ls_archive_statusdir` | fn | ❌ | P4 | 1 | list of files in the archive_status directory | Server filesystem access. |
| `pg_ls_dir` | fn | ❌ | P4 | 2 | list all files in a directory | Server filesystem access. |
| `pg_ls_logdir` | fn | ❌ | P4 | 1 | list files in the log directory | Server filesystem access. |
| `pg_ls_logicalmapdir` | fn | ❌ | P4 | 1 | list of files in the pg_logical/mappings directory | Server filesystem access. |
| `pg_ls_logicalsnapdir` | fn | ❌ | P4 | 1 | list of files in the pg_logical/snapshots directory | Server filesystem access. |
| `pg_ls_replslotdir` | fn | ❌ | P4 | 1 | list of files in the pg_replslot/slot_name directory | Server filesystem access. |
| `pg_ls_summariesdir` | fn | ❌ | P4 | 1 | list of files in the pg_wal/summaries directory | Server filesystem access. |
| `pg_ls_tmpdir` | fn | ❌ | P4 | 2 | list files in the pgsql_tmp directory | Server filesystem access. |
| `pg_ls_waldir` | fn | ❌ | P4 | 1 | list of files in the WAL directory | Server filesystem access. |
| `pg_partition_ancestors` | fn | ❌ | P4 | 1 | view ancestors of the partition | Partitioning introspection. |
| `pg_partition_root` | fn | ❌ | P4 | 1 | get top-most partition root parent |  |
| `pg_partition_tree` | fn | ❌ | P4 | 1 | view partition tree tables |  |
| `pg_promote` | fn | ❌ | P4 | 1 | promote standby server | Standby promotion. |
| `pg_read_binary_file` | fn | ❌ | P4 | 4 | read bytea from a file | Server filesystem access. |
| `pg_read_file` | fn | ❌ | P4 | 4 | read text from a file | Server filesystem access. |
| `pg_relation_filenode` | fn | ❌ | P4 | 1 | filenode identifier of relation | Storage internals. |
| `pg_relation_filepath` | fn | ❌ | P4 | 1 | file path of relation |  |
| `pg_relation_size` | fn | 🔧 | — | 2 | disk space usage for the main fork of the specified table or index | In datafusion-pg-catalog |
| `pg_reload_conf` | fn | ❌ | P4 | 1 | reload configuration files | Server admin. |
| `pg_replication_origin_advance` | fn | ❌ | P4 | 1 | advance replication origin to specific location | Replication. |
| `pg_replication_origin_create` | fn | ❌ | P4 | 1 | create a replication origin | Replication. |
| `pg_replication_origin_drop` | fn | ❌ | P4 | 1 | drop replication origin identified by its name | Replication. |
| `pg_replication_origin_oid` | fn | ❌ | P4 | 1 | translate the replication origin's name to its id | Replication. |
| `pg_replication_origin_progress` | fn | ❌ | P4 | 1 | get an individual replication origin's replication progress | Replication. |
| `pg_replication_origin_session_is_setup` | fn | ❌ | P4 | 1 | is a replication origin configured in this session | Replication. |
| `pg_replication_origin_session_progress` | fn | ❌ | P4 | 1 | get the replication progress of the current session | Replication. |
| `pg_replication_origin_session_reset` | fn | ❌ | P4 | 1 | teardown configured replication progress tracking | Replication. |
| `pg_replication_origin_session_setup` | fn | ❌ | P4 | 1 | configure session to maintain replication progress tracking for the passed in origin | Replication. |
| `pg_replication_origin_xact_reset` | fn | ❌ | P4 | 1 | reset the transaction's origin lsn and timestamp | Replication. |
| `pg_replication_origin_xact_setup` | fn | ❌ | P4 | 1 | setup the transaction's origin lsn and timestamp | Replication. |
| `pg_replication_slot_advance` | fn | ❌ | P4 | 1 | advance logical replication slot | Replication. |
| `pg_restore_attribute_stats` | fn | ❌ | P4 | 1 | restore statistics on attribute |  |
| `pg_restore_relation_stats` | fn | ❌ | P4 | 1 | restore statistics on relation |  |
| `pg_rotate_logfile` | fn | ❌ | P4 | 1 | rotate log file | Server admin. |
| `pg_size_bytes` | fn | 🚧 | P2 | 1 | convert a size in human-readable format with size units into bytes | Pure parsing. |
| `pg_size_pretty` | fn | 🚧 | P2 | 2 | convert a long int to a human readable text using size units | Pure formatting. |
| `pg_split_walfile_name` | fn | 🚧 | P4 | 1 | sequence number and timeline ID given a wal filename |  |
| `pg_stat_file` | fn | ❌ | P4 | 2 | get information about file | Server filesystem access. |
| `pg_switch_wal` | fn | ❌ | P4 | 1 | switch to new wal file | WAL. |
| `pg_sync_replication_slots` | fn | ❌ | P4 | 1 | sync replication slots from the primary to the standby |  |
| `pg_table_size` | fn | ❌ | P4 | 1 | disk space usage for the specified table, including TOAST, free space and visibility map | Storage layout. |
| `pg_tablespace_size` | fn | ❌ | P4 | 2 | total disk space usage for the specified tablespace |  |
| `pg_terminate_backend` | fn | ❌ | P4 | 1 | terminate a server process | Backend control; no multi-process backend. |
| `pg_total_relation_size` | fn | 🔧 | — | 1 | total disk space usage for the specified table and associated indexes | In datafusion-pg-catalog |
| `pg_try_advisory_lock` | fn | ❌ | P4 | 2 | obtain exclusive advisory lock if available | Advisory locks. |
| `pg_try_advisory_lock_shared` | fn | ❌ | P4 | 2 | obtain shared advisory lock if available |  |
| `pg_try_advisory_xact_lock` | fn | ❌ | P4 | 2 | obtain exclusive advisory lock if available |  |
| `pg_try_advisory_xact_lock_shared` | fn | ❌ | P4 | 2 | obtain shared advisory lock if available |  |
| `pg_wal_lsn_diff` | fn | ❌ | P4 | 1 | difference in bytes, given two wal locations | WAL/replication internals. |
| `pg_wal_replay_pause` | fn | ❌ | P4 | 1 | pause wal replay | WAL replay. |
| `pg_wal_replay_resume` | fn | ❌ | P4 | 1 | resume wal replay, if it was paused | WAL replay. |
| `pg_walfile_name` | fn | ❌ | P4 | 1 | wal filename, given a wal location | WAL. |
| `pg_walfile_name_offset` | fn | ❌ | P4 | 1 | wal filename and byte offset, given a wal location | WAL. |
| `set_config` | fn | 🚧 | P2 | 1 | SET X as a function | Shim-able via DataFusion `SessionConfig`. |

## Trigger Functions

*Section `functions-trigger` · Module: [`system`](src/system.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `suppress_redundant_updates_trigger` | fn | ❌ | P4 | 1 | trigger to suppress updates when new and old records match | Out of scope: PL/pgSQL trigger machinery. |
| `tsvector_update_trigger` | fn | ❌ | P4 | 1 | trigger for automatic update of tsvector column | Out of scope: PL/pgSQL trigger machinery. |
| `tsvector_update_trigger_column` | fn | ❌ | P4 | 1 | trigger for automatic update of tsvector column | Out of scope: PL/pgSQL trigger machinery. |

## Event Trigger Functions

*Section `functions-event-triggers` · Module: [`system`](src/system.rs)

| Function | Kind | Status | Pri | #overloads | Description | Notes |
|---|:---:|:---:|:---:|---:|---|---|
| `pg_event_trigger_ddl_commands` | fn | ❌ | P4 | 1 | list DDL actions being executed by the current command | Out of scope: event trigger machinery. |
| `pg_event_trigger_dropped_objects` | fn | ❌ | P4 | 1 | list objects dropped by the current command | Out of scope: event trigger machinery. |
| `pg_event_trigger_table_rewrite_oid` | fn | ❌ | P4 | 1 | return Oid of the table getting rewritten | Out of scope: event trigger machinery. |
| `pg_event_trigger_table_rewrite_reason` | fn | ❌ | P4 | 1 | return reason code for table getting rewritten | Out of scope: event trigger machinery. |
| `pg_get_object_address` | fn | ❌ | P4 | 1 | get OID-based object address from name/args arrays | Out of scope |

---


## Conventions for new implementations

1. **One UDF per file** under the appropriate category module. Each file
   exports a `*UDF` struct, a `Default` impl, a `ScalarUDFImpl` impl, and a
   `create_*_udf()` constructor.
2. **Add the constructor to the module's `register`** function so it shows up
   in `register_all`.
3. **Document Postgres compatibility** at the top of the file: link to the PG
   manual page and call out any deviation from upstream semantics.
4. **Unit tests** must cover at least: NULL input, empty input, the boundary
   conditions described in the PG docs (1-based indices, off-by-one cases,
   negative offsets, etc.), and a row-wise vectorized batch.
5. **Update this file** by flipping the row's status from 🚧 P*x* to 🔧 — once
   the UDF is registered by the crate.

## References

- PostgreSQL Functions and Operators — https://www.postgresql.org/docs/current/functions.html
- PostgreSQL Aggregate Functions — https://www.postgresql.org/docs/current/functions-aggregate.html
- PostgreSQL Window Functions — https://www.postgresql.org/docs/current/functions-window.html
- PostgreSQL JSON Functions — https://www.postgresql.org/docs/current/functions-json.html
- PostgreSQL `pg_proc` reference — https://www.postgresql.org/docs/current/catalog-pg-proc.html
- Apache DataFusion Functions — https://datafusion.apache.org/library-user-guide/functions.html

