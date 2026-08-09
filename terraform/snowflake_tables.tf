# ============================================================
# Tables RAW du projet delivery lakehouse.
#
# Une table par type d'événement, structure alignée sur les schémas
# Pydantic du simulateur (Brique 1) et le lake Parquet (Brique 2-4).
#
# Convention :
#   - Types Snowflake standards (VARCHAR, TIMESTAMP_TZ, FLOAT)
#   - Colonnes techniques ajoutées : LOAD_TIMESTAMP (audit d'ingestion)
#   - Pas de contraintes (RAW = données brutes, on ne bloque pas
#     l'ingestion sur des règles métier — c'est le rôle de dbt/GX)
# ============================================================

# ---------- Table gps_events ----------
resource "snowflake_table" "gps_events" {
  database = snowflake_database.main.name
  schema   = snowflake_schema.raw.name
  name     = "GPS_EVENTS"

  comment = "Positions GPS des véhicules (ingérées depuis lake/events/gps/)."

  column {
    name = "VEHICLE_ID"
    type = "VARCHAR(64)"
  }
  column {
    name = "DRIVER_ID"
    type = "VARCHAR(64)"
  }
  column {
    name = "LAT"
    type = "FLOAT"
  }
  column {
    name = "LON"
    type = "FLOAT"
  }
  column {
    name = "SPEED_KMH"
    type = "FLOAT"
  }
  column {
    name = "EVENT_TIME"
    type = "TIMESTAMP_TZ"
  }
  column {
    name = "LOAD_TIMESTAMP"
    type = "TIMESTAMP_TZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

# ---------- Table delivery_events ----------
resource "snowflake_table" "delivery_events" {
  database = snowflake_database.main.name
  schema   = snowflake_schema.raw.name
  name     = "DELIVERY_EVENTS"

  comment = "Événements de livraison (ingérés depuis lake/events/delivery/)."

  column {
    name = "PACKAGE_ID"
    type = "VARCHAR(64)"
  }
  column {
    name = "ORDER_ID"
    type = "VARCHAR(64)"
  }
  column {
    name = "VEHICLE_ID"
    type = "VARCHAR(64)"
  }
  column {
    name = "DRIVER_ID"
    type = "VARCHAR(64)"
  }
  column {
    name = "STATUS"
    type = "VARCHAR(32)"
  }
  column {
    name = "EVENT_TIME"
    type = "TIMESTAMP_TZ"
  }
  column {
    name = "LOAD_TIMESTAMP"
    type = "TIMESTAMP_TZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

# ---------- Table orders ----------
resource "snowflake_table" "orders" {
  database = snowflake_database.main.name
  schema   = snowflake_schema.raw.name
  name     = "ORDERS"

  comment = "Commandes créées (ingérées depuis lake/events/order/)."

  column {
    name = "ORDER_ID"
    type = "VARCHAR(64)"
  }
  column {
    name = "PACKAGE_ID"
    type = "VARCHAR(64)"
  }
  column {
    name = "STATUS"
    type = "VARCHAR(32)"
  }
  column {
    name = "EVENT_TIME"
    type = "TIMESTAMP_TZ"
  }
  column {
    name = "LOAD_TIMESTAMP"
    type = "TIMESTAMP_TZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

# ---------- Table driver_events ----------
resource "snowflake_table" "driver_events" {
  database = snowflake_database.main.name
  schema   = snowflake_schema.raw.name
  name     = "DRIVER_EVENTS"

  comment = "Événements chauffeurs (ingérés depuis lake/events/driver/)."

  column {
    name = "DRIVER_ID"
    type = "VARCHAR(64)"
  }
  column {
    name = "VEHICLE_ID"
    type = "VARCHAR(64)"
  }
  column {
    name = "STATUS"
    type = "VARCHAR(32)"
  }
  column {
    name = "EVENT_TIME"
    type = "TIMESTAMP_TZ"
  }
  column {
    name = "LOAD_TIMESTAMP"
    type = "TIMESTAMP_TZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
}

# ---------- Grants sur les tables ----------
# Le role DELIVERY_ROLE a besoin de SELECT et INSERT/UPDATE sur ces tables.
# On utilise ALL PRIVILEGES pour la couche RAW (contrôlée par Terraform + ingestion).
resource "snowflake_grant_privileges_to_account_role" "raw_tables_all" {
  account_role_name = snowflake_account_role.delivery.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]

  on_schema_object {
    all {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.main.name}.${snowflake_schema.raw.name}"
    }
  }
}

# Applique aussi aux futures tables créées dans RAW.
resource "snowflake_grant_privileges_to_account_role" "raw_future_tables" {
  account_role_name = snowflake_account_role.delivery.name
  privileges        = ["SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE"]

  on_schema_object {
    future {
      object_type_plural = "TABLES"
      in_schema          = "${snowflake_database.main.name}.${snowflake_schema.raw.name}"
    }
  }
}