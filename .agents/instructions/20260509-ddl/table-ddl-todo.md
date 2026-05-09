# Table DDL TODO

Use `.agents/instructions/20260509-ddl/table-ddl.md` as the source of truth for behavior and data model
details.

When resuming with "continue from table-ddl-todo.md", start from the first
unchecked item. Follow repository rules: edit one file at a time and ask before
editing each file.

## RPC Implementation Order

- [x] `CreateExternalTable` with scalar types only.
- [x] `GetTable` with scalar types only.
- [x] `ListTables` with scalar types only.
- [x] `EvolveTableSchema` with scalar types only.
- [ ] Remove the legacy `CreateTable` RPC and its legacy request/response messages.
- [ ] `DropTableColumns` with scalar types only.
- [ ] Add `struct`, `map`, and `list` type support.

## Per-RPC Steps

For each RPC, proceed in this order unless the user gives a different
instruction:

- Update `spec/proto/api.proto`.
- Regenerate protobuf output through the existing build flow.
- Add or update DB migration/entity code.
- Add or update gRPC and core API/application DTO/use-case/model/service/repository code.
