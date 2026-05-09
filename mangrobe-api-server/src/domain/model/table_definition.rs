use thiserror::Error;

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DbObjectIdentifier(String);

impl TryFrom<String> for DbObjectIdentifier {
    type Error = DbObjectIdentifierError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let mut chars = value.chars();
        let Some(first) = chars.next() else {
            return Err(DbObjectIdentifierError::Empty);
        };

        if !is_identifier_start(first) {
            return Err(DbObjectIdentifierError::InvalidFirstChar {
                value,
                invalid_char: first,
            });
        }

        for c in chars {
            if !is_identifier_part(c) {
                return Err(DbObjectIdentifierError::InvalidChar {
                    value,
                    invalid_char: c,
                });
            }
        }

        Ok(Self(value))
    }
}

impl DbObjectIdentifier {
    pub fn val(&self) -> String {
        self.0.clone()
    }
}

#[derive(Error, Debug)]
pub(crate) enum DbObjectIdentifierError {
    #[error("identifier must not be empty")]
    Empty,

    #[error("identifier '{value}' first character must match [A-Za-z_], but got '{invalid_char}'")]
    InvalidFirstChar { value: String, invalid_char: char },

    #[error("identifier '{value}' character must match [A-Za-z0-9_], but got '{invalid_char}'")]
    InvalidChar { value: String, invalid_char: char },
}

#[derive(Error, Debug)]
pub(crate) enum TableDefinitionError {
    #[error("S3 location requires non-empty bucket")]
    S3BucketRequired,

    #[error("partition dst_column must be unset or different from src_column")]
    InvalidPartitionDestination,

    #[error("duplicate column name: '{0}'")]
    DuplicateColumnName(String),

    #[error("partition src_column references unknown column: '{0}'")]
    UnknownPartitionSourceColumn(String),

    #[error("partition dst_column references unknown column: '{0}'")]
    UnknownPartitionDestinationColumn(String),
}

fn is_identifier_start(c: char) -> bool {
    c == '_' || c.is_ascii_alphabetic()
}

fn is_identifier_part(c: char) -> bool {
    c == '_' || c.is_ascii_alphanumeric()
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TableDefinition {
    pub identifier: TableIdentifier,
    pub location: ExternalLocation,
    pub format: FileFormat,
    pub columns: Vec<Column>,
    pub partition_fields: Vec<PartitionField>,
    pub comment: Option<String>,
}

impl TableDefinition {
    pub fn new(
        identifier: TableIdentifier,
        location: ExternalLocation,
        format: FileFormat,
        columns: Vec<Column>,
        partition_fields: Vec<PartitionField>,
        comment: Option<String>,
    ) -> Result<Self, TableDefinitionError> {
        let mut column_names = std::collections::HashSet::new();
        for column in &columns {
            if !column_names.insert(column.name.val()) {
                return Err(TableDefinitionError::DuplicateColumnName(column.name.val()));
            }
        }

        for partition_field in &partition_fields {
            if !column_names.contains(&partition_field.src_column.val()) {
                return Err(TableDefinitionError::UnknownPartitionSourceColumn(
                    partition_field.src_column.val(),
                ));
            }
            if let Some(dst_column) = &partition_field.dst_column
                && !column_names.contains(&dst_column.val()) {
                    return Err(TableDefinitionError::UnknownPartitionDestinationColumn(
                        dst_column.val(),
                    ));
                }
        }

        Ok(Self {
            identifier,
            location,
            format,
            columns,
            partition_fields,
            comment,
        })
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TableIdentifier {
    pub catalog_name: DbObjectIdentifier,
    pub schema_name: DbObjectIdentifier,
    pub table_name: DbObjectIdentifier,
}

impl TableIdentifier {
    pub fn new(
        catalog_name: DbObjectIdentifier,
        schema_name: DbObjectIdentifier,
        table_name: DbObjectIdentifier,
    ) -> Self {
        Self {
            catalog_name,
            schema_name,
            table_name,
        }
    }

    pub fn full_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.catalog_name.val(),
            self.schema_name.val(),
            self.table_name.val()
        )
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ExternalLocation {
    pub storage_scheme: StorageScheme,
    pub bucket: Option<String>,
    pub prefix: Option<String>,
    pub endpoint: Option<String>,
    pub region: Option<String>,
}

impl ExternalLocation {
    pub fn new(
        storage_scheme: StorageScheme,
        bucket: Option<String>,
        prefix: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
    ) -> Result<Self, TableDefinitionError> {
        if matches!(storage_scheme, StorageScheme::S3)
            && bucket.as_ref().is_none_or(String::is_empty)
        {
            return Err(TableDefinitionError::S3BucketRequired);
        }

        Ok(Self {
            storage_scheme,
            bucket,
            prefix,
            endpoint,
            region,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StorageScheme {
    S3,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FileFormat {
    Parquet,
    Vortex,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Column {
    pub name: DbObjectIdentifier,
    pub data_type: DataType,
    pub nullable: bool,
    pub comment: Option<String>,
}

impl Column {
    pub fn new(
        name: DbObjectIdentifier,
        data_type: DataType,
        nullable: bool,
        comment: Option<String>,
    ) -> Self {
        Self {
            name,
            data_type,
            nullable,
            comment,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum DataType {
    Scalar(ScalarType),
    Time(TimeType),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScalarType {
    Bool,
    Int64,
    Float64,
    String,
    Date,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TimeType {
    pub unit: TimeUnit,
}

impl TimeType {
    pub fn new(unit: TimeUnit) -> Self {
        Self { unit }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TimeUnit {
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct PartitionField {
    pub src_column: DbObjectIdentifier,
    pub dst_column: Option<DbObjectIdentifier>,
    pub transform: PartitionTransform,
    pub result_type: DataType,
}

impl PartitionField {
    pub fn new(
        src_column: DbObjectIdentifier,
        dst_column: Option<DbObjectIdentifier>,
        transform: PartitionTransform,
        result_type: DataType,
    ) -> Result<Self, TableDefinitionError> {
        if dst_column.as_ref() == Some(&src_column) {
            return Err(TableDefinitionError::InvalidPartitionDestination);
        }

        Ok(Self {
            src_column,
            dst_column,
            transform,
            result_type,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PartitionTransform {
    Identity,
    Hour,
    Day,
    Month,
    Year,
}
