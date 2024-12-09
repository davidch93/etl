# ETL Core Module

The `etl-core` module is the foundational component for running and managing Extract, Transform, Load (ETL) operations.
It provides core utilities, schema handling, email notifications, and configuration management essential
for building robust data processing pipelines.

## Table of Contents

- [Requirements](#requirements)
- [Features](#features)
    - [Schema Management](#1-schema-management)
    - [Configuration Management](#2-configuration-management)
    - [Email Notification](#3-email-notification)
- [Usage](#usage)
    - [Schema](#schema)
    - [Emails](#emails)

## Requirements

- **Java Version**: 17 or higher
- **Build Tool**: Maven or Gradle
- SMTP server credentials for email notifications.

## Features

### 1. Schema Management

- **Schema Loader**: Load and parse table schemas from JSON files for use in ETL operations.
- **Model Definitions**:
    - `Table`: Represents table-level metadata.
    - `TableSchema`: Defines the schema structure.
    - `Field`: Represents individual fields within the schema, supporting validation rules.

### 2. Configuration Management

- **Config Loader**: Load application-specific configurations from JSON files with custom-defined models.

### 3. Email Notification

- **Email Utility**: Send email notifications via SMTP for monitoring and alerts.
- Supports system property-based configuration for SMTP settings.

## Usage

### Schema

Define a table schema definition stored in Filesystem with the following structure.

```json
{
  "table_name": "github_staging_orders",
  "source_type": "MYSQL",
  "schema": {
    "type": "record",
    "name": "orders",
    "doc": "Schema for the orders table",
    "fields": [
      {
        "name": "id",
        "type": "INTEGER",
        "nullable": false,
        "doc": "Unique identifier for the order",
        "rules": [
          {
            "rule": "IS_PRIMARY_KEY",
            "hint": "The value of this column must be unique and not NULL!"
          }
        ]
      },
      {
        "name": "amount",
        "type": "DOUBLE",
        "nullable": true,
        "doc": "Total amount of the order",
        "rules": [
          {
            "rule": "IS_NON_NEGATIVE",
            "hint": "The value of this column must be positive!"
          },
          {
            "rule": "HAS_COMPLETENESS",
            "assertion": "0.85",
            "hint": "NULL values of this column should be below 0.15!"
          }
        ]
      },
      {
        "name": "status",
        "type": "STRING",
        "nullable": true,
        "doc": "Status of the order",
        "rules": [
          {
            "rule": "IS_CONTAINED_IN",
            "values": [
              "PAID",
              "CANCELLED",
              "REMITTED"
            ],
            "hint": "The value of this column must contain PAID, CANCELLED, and REMITTED only"
          }
        ]
      },
      {
        "name": "is_expired",
        "type": "BOOLEAN",
        "nullable": true,
        "doc": "Whether the order expires or not"
      },
      {
        "name": "created_at",
        "type": "INTEGER",
        "nullable": false,
        "doc": "Date of the order",
        "rules": [
          {
            "rule": "IS_COMPLETE",
            "hint": "The value of this column must not be NULL!"
          }
        ]
      }
    ]
  },
  "constraint_keys": [
    "id"
  ],
  "table_partition": {
    "source_column": "created_at",
    "partition_column": "order_timestamp",
    "partition_type": "DAY",
    "partition_filter_required": false,
    "doc": "A partition column indicating the date of the order was created"
  },
  "clustered_columns": [
    "id"
  ]
}
```

### Emails

Configure SMTP settings as system properties:

```bash
-Dspark.yarn.appMasterEnv.SMTP_HOST=smtp.example.com
-Dspark.yarn.appMasterEnv.SMTP_PORT=587
-Dspark.yarn.appMasterEnv.SMTP_STARTTLS=true
-Dspark.yarn.appMasterEnv.SMTP_SSL=false
-Dspark.yarn.appMasterEnv.SMTP_MAIL_SENDER=no-reply@example.com
-Dspark.yarn.appMasterEnv.SMTP_USER=username
-Dspark.yarn.appMasterEnv.SMTP_PASSWORD=base64encoded-password
-Dspark.yarn.appMasterEnv.MAILING_LIST=recipient@example.com
```
