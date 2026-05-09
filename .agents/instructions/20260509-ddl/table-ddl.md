# Table DDL and External Table Catalog

This document describes the initial DDL/catalog API for external tables in
Mangrobe. The API is intended to be used by `mangrobe-api-server` and by
`mangrobe-db`, which can use the returned structured metadata to build
DataFusion, Arrow Flight SQL, and ADBC-facing metadata.

## Goals

- Support external table creation for DataFusion-backed query execution.
- Represent table names as `catalog.schema.table`.
- Store table metadata needed for DataFusion external tables: location, file
  format, columns, partitions, and an optional comment.
- Support table listing and full table lookup.
- Support explicit schema evolution and explicit column drops.
- Keep protobuf messages structured. Protobuf fields must not use JSON blobs,
  opaque bytes, or `google.protobuf.Struct` to represent table definitions.

## Non-goals

- SQL DDL parsing.
- Table replacement.
- Table definition versioning or history.
- Automatic schema evolution during file registration.
- Generic options/properties fields.
- Local filesystem table locations.
- Credential references or embedded credentials.
- Test implementation as part of this specification-writing task.

## DataDefinitionService API

`DataDefinitionService` should keep the existing `CreateTable` RPC until the new
external table RPCs are implemented. After the new RPCs are available, the
legacy `CreateTable` RPC and its legacy request/response messages should be
removed.

New RPCs:

- `CreateExternalTable`
- `GetTable`
- `ListTables`
- `EvolveTableSchema`
- `DropTableColumns`

The RPCs should be implemented in that order.

### CreateExternalTable

Creates a new external table.

The request should include:

- table identifier: `catalog_name`, `schema_name`, `table_name`
- `skip_if_exists`
- external object-store location
- file format
- columns
- partition fields
- optional comment

`skip_if_exists` is the only supported conflict mode. There is no replace mode in
the initial design.

When the target table already exists:

- if `skip_if_exists` is true, return the existing table definition
- otherwise, return an already-exists error

The response should return the full table definition, equivalent to `GetTable`.

`location` and `format` are required. `columns` and `partition fields` may be
empty.

At the protobuf level, the create request should be shaped around a
`TableDefinition` plus `skip_if_exists`.

### GetTable

Returns the full table definition for one table.

The response should include all metadata needed for `mangrobe-db` to construct
DataFusion table registration and Arrow Flight SQL/ADBC metadata responses.

### ListTables

Lists table summaries.

The request should support:

- optional catalog filter
- optional schema filter

The request should not include pagination. Arrow Flight SQL table-list metadata
does not use pagination, so `ListTables` returns all matching table summaries.

The response should include table summaries, not full table definitions.

Each summary should include at least:

- table identifier
- optional comment

List ordering should be by catalog name, then schema name, then table name, all
ascending. Filters are limited to catalog name and schema name. Supplying only a
schema filter is allowed and returns matching tables across all catalogs.

### EvolveTableSchema

Receives a full proposed schema and merges compatible changes into the current
schema.

The request should include:

- table identifier
- proposed columns

Evolution behavior:

- columns that do not exist are added
- existing columns that are absent from the proposed schema are left unchanged
- existing columns that are present with the same definition are left unchanged
- existing columns that are present with compatible widening are updated
- existing columns that are present with incompatible definitions are rejected

Compatible widening should initially be conservative:

- nullable widening is allowed
- signed integer width widening is allowed
- unsigned integer width widening is allowed
- float width widening is allowed
- nested field addition is allowed recursively through structs, lists, and maps

Decimal widening and other complex type changes are out of scope for the initial
implementation.

This RPC does not drop columns, rename columns, narrow types, or implicitly
remove columns missing from the proposed schema.

An empty proposed schema is allowed and should succeed as a no-op. Duplicate
column names in the proposed schema are invalid.

The response should return the updated full table definition.

### DropTableColumns

Explicitly drops named columns from a table.

The request should include:

- table identifier
- column paths

Column drop is intentionally separate from schema evolution so that missing
columns in `EvolveTableSchema` never mean deletion.

Nested fields can be dropped. Column paths should be represented as explicit
path segments, not dot-separated strings.

If any requested column path does not exist, reject the whole request. If a
requested column is used as a partition source, reject the request.

The response should return the updated full table definition.

Duplicate column paths in one request are invalid.

Dropping nested fields may leave an empty struct. Empty structs are allowed.

## Protobuf Data Model

All protobuf messages should be structured. Do not add JSON string, opaque
bytes, or `google.protobuf.Struct` fields to represent table definitions.

### Table Identifier

Use a structured identifier:

- `catalog_name`
- `schema_name`
- `table_name`

These names are required for new external table APIs.

There is no default catalog or schema. All three identifier fields must be
specified.

Identifier names must match `[A-Za-z_][A-Za-z0-9_]*`.

### Table Definition

A full table definition should include:

- table identifier
- external object-store location
- file format
- columns
- partition fields
- optional comment

The table definition does not include a table type. This API defines tables
only; future views should use separate view-specific RPCs.

The shared full-definition protobuf message should be named `TableDefinition`.
Create, get, evolve, and drop responses should all return this full definition.

List responses should return lightweight table summaries rather than full
definitions. A table summary should include only the table identifier and an
optional comment.

### File Format

Initial supported file formats:

- `PARQUET`
- `VORTEX`

Unsupported file formats should be rejected during request validation.

The file format is required when creating a table.

### Location

Location should describe where the stored data exists. Only object-store style
locations are in scope.

Location fields:

- storage scheme
- optional bucket or container
- optional prefix or path
- optional endpoint
- optional region

Do not include local filesystem paths, credential references, or embedded
credentials.

The initial supported storage scheme is S3. For S3 locations, bucket is
required by validation. Prefix, endpoint, and region are optional. POSIX/local
storage may be added later as a separate storage scheme.

The storage location is required when creating a table.

The storage scheme should be represented as an enum. Bucket, prefix, endpoint,
and region should be optional at the protobuf level. Validation should require
bucket only for S3 locations.

## Column Type System

Mangrobe should define a generic type system, not an Arrow-specific wire type.
The type system must be convertible to Arrow/DataFusion types by `mangrobe-db`.

Initial supported type categories:

- boolean
- signed integer
- floating point
- string
- date
- time

The initial scalar-only implementation supports only:

- `BOOL`
- `INT64`
- `FLOAT64`
- `STRING`
- `DATE`
- `TIME`

Additional signed integer widths, unsigned integers, `FLOAT32`, decimal,
binary, timestamp, list, struct, and map types are intentionally deferred until
after the scalar-only RPCs are complete.

Each column should include:

- name
- type
- nullability
- optional comment

Nested types should also be structured. For example, `struct` contains named
fields and `list` contains an element type.

Column nullability is required in proto input and should be represented as an
explicit true/false value in proto, domain, and database representations.

Integers and floating point types should be represented as concrete enum values
such as `INT64` and `FLOAT64`, not as free-form bit widths. Time types should
carry a time unit. Date types do not need a time unit. Future timestamp types
should carry a time unit and may carry a timezone.

The protobuf `DataType` should use a structured union-style shape. Time is a
structured message because it has a time unit. Future decimal, timestamp, list,
struct, and map types should also be structured messages because they have
parameters or nested types.

Time units should include second, millisecond, microsecond, and nanosecond.
Future timestamp timezone should be optional.

Future map key types must remain fixed during schema evolution. Future map
value types and list element types may evolve recursively according to the
schema evolution rules.

Column names and column path segments must match `[A-Za-z_][A-Za-z0-9_]*`.

## Partition Model

Mangrobe stores the meaning of partition values supplied by file-registration
operations. It does not scan file contents to derive partition values.

Partition definitions should be structured fields with:

- source column, represented as `src_column`
- optional destination column, represented as `dst_column`
- transform
- result type

Initial transforms:

- `IDENTITY`
- `HOUR`
- `DAY`
- `MONTH`
- `YEAR`

`src_column` is required and must refer to an existing top-level table
column. Partition sources do not support nested column paths in the initial
implementation.

`dst_column` is optional. If it is unset, the partition field is virtual: the
value is tracked as partition metadata but has no physical table column. If it
is set, it must refer to an existing top-level table column that stores the
computed partition value.

If `dst_column` is set, it must be different from `src_column`. Writing the
same column name is unnecessary, including for `IDENTITY` transforms. For
`IDENTITY`, `dst_column` may be unset or may name a different column containing
the same value.

Examples:

- `src_column = created_at`, `dst_column` unset, transform `IDENTITY`
- `src_column = created_at`, `dst_column = created_at_copy`, transform
  `IDENTITY`
- `src_column = created_at`, `dst_column = created_hour`, transform `HOUR`
- `src_column = created_at`, `dst_column` unset, transform `DAY`

## Database Storage Model

The existing `user_tables` table should be extended instead of introducing a
separate external table metadata table. Existing data compatibility is not
required for this development flow.

The existing initial migration file
`mangrobe-api-server/migration/src/m20251103_033827_initialize.rs` should be
updated directly for this work. A separate compatibility migration is not
required.

After updating the migration, regenerate database-derived code by running:

```sh
make migrate/fresh && make generate
```

The table metadata table should include at least:

- `id`
- `catalog_name`
- `schema_name`
- `name`
- `location`
- `format`
- `columns`
- `partitions`
- `comment`
- `created_at`
- `updated_at`

Use a unique constraint on:

- `catalog_name`
- `schema_name`
- `name`

The existing `name` column remains the physical database column for the API's
`table_name`.

Definition-part columns such as `location`, `columns`, and `partitions` may use
JSONB internally. This is a database storage choice only. The protobuf API must
still use structured messages.

`columns` is expected to be JSONB in the initial implementation because nested
types and schema evolution are easier to manage as a single structured
definition document.

The `format` column should follow the repository's existing enum persistence
style and be stored as a numeric enum value.

## Flight SQL and ADBC Compatibility

Mangrobe API does not directly return Arrow schema bytes or Flight SQL response
payloads.

However, `GetTable` must return enough structured information for `mangrobe-db`
to construct Flight SQL and ADBC metadata responses, including:

- catalog name
- schema name
- table name
- column names
- column types
- column nullability
- column comments
- table comment

`ListTables` can be used to build table-list metadata responses. `GetTable`
can be used when a detailed schema is required.
