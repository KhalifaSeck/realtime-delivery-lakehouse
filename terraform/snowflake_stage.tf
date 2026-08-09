# ============================================================
# Stage externe Snowflake pointant sur ADLS Gen2.
#
# Un stage externe est un "signet" qui donne à Snowflake un accès
# à un container cloud (ADLS/S3/GCS). Il faut aussi définir un
# format de fichier (ici, Parquet).
#
# Une fois le stage créé, on pourra faire :
#   COPY INTO RAW.GPS_EVENTS FROM @DELIVERY_STAGE/gps/
# ============================================================

# ---------- File Format : Parquet ----------
# Décrit comment Snowflake doit lire les fichiers Parquet.
# On garde les valeurs par défaut : les Parquet ont un schema
# auto-descriptif (contrairement au CSV).
resource "snowflake_file_format" "parquet" {
  database    = snowflake_database.main.name
  schema      = snowflake_schema.raw.name
  name        = "PARQUET_FORMAT"
  format_type = "PARQUET"

  comment = "Format Parquet pour ingérer les fichiers du lake."
}

# ---------- Stage externe pointant sur ADLS Gen2 ----------
resource "snowflake_stage" "adls" {
  database = snowflake_database.main.name
  schema   = snowflake_schema.raw.name
  name     = "DELIVERY_STAGE"

  # URL du container ADLS Gen2, format Azure : azure://<account>.blob.core.windows.net/<container>/
  url = "azure://${azurerm_storage_account.lake.name}.blob.core.windows.net/${azurerm_storage_container.raw_events.name}/"

  # Credentials : SAS token généré par le block data ci-dessus.
  credentials = "AZURE_SAS_TOKEN='${data.azurerm_storage_account_sas.lake_read.sas}'"

  # Format par défaut appliqué aux COPY INTO.
  file_format = "FORMAT_NAME = ${snowflake_database.main.name}.${snowflake_schema.raw.name}.${snowflake_file_format.parquet.name}"

  comment = "Stage externe pointant sur ADLS Gen2 (container raw-events)."
}

# ---------- Grants ----------
# Le role DELIVERY_ROLE doit pouvoir utiliser le stage et le format.
resource "snowflake_grant_privileges_to_account_role" "stage_usage" {
  account_role_name = snowflake_account_role.delivery.name
  privileges        = ["USAGE", "READ"]

  on_schema_object {
    object_type = "STAGE"
    object_name = "${snowflake_database.main.name}.${snowflake_schema.raw.name}.${snowflake_stage.adls.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "file_format_usage" {
  account_role_name = snowflake_account_role.delivery.name
  privileges        = ["USAGE"]

  on_schema_object {
    object_type = "FILE FORMAT"
    object_name = "${snowflake_database.main.name}.${snowflake_schema.raw.name}.${snowflake_file_format.parquet.name}"
  }
}

# ---------- Output ----------
output "snowflake_stage" {
  description = "Nom du stage externe."
  value       = "${snowflake_database.main.name}.${snowflake_schema.raw.name}.${snowflake_stage.adls.name}"
}