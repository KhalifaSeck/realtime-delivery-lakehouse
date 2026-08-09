# ============================================================
# Structure Snowflake principale de la plateforme delivery.
#
# Objets créés :
#   - Warehouse   : DELIVERY_WH (moteur de calcul, taille XS)
#   - Database    : DELIVERY_LAKEHOUSE
#   - Schema RAW  : données brutes ingérées d'ADLS
#   - Role        : DELIVERY_ROLE (droits sur la base)
#   - Grants      : le role reçoit les droits nécessaires
#
# Convention de nommage : UPPERCASE_WITH_UNDERSCORES (standard Snowflake).
# ============================================================

# ---------- Warehouse (moteur de calcul) ----------
resource "snowflake_warehouse" "main" {
  name           = "${upper(var.project_name)}_WH"
  warehouse_size = "XSMALL"    # Le plus petit, largement suffisant pour du dev
  auto_suspend   = 60          # Se met en pause après 60s d'inactivité (coût)
  auto_resume    = true        # Redémarre automatiquement à la première query

  comment = "Warehouse dédié au projet delivery lakehouse."
}

# ---------- Database ----------
resource "snowflake_database" "main" {
  name    = "${upper(var.project_name)}_LAKEHOUSE"
  comment = "Base de données principale du projet delivery lakehouse."
}

# ---------- Schema RAW (données brutes) ----------
# On aura d'autres schemas plus tard (STAGING, MARTS) avec dbt.
resource "snowflake_schema" "raw" {
  name     = "RAW"
  database = snowflake_database.main.name
  comment  = "Données brutes ingérées du lake ADLS Gen2."
}

# ---------- Role (droits d'accès) ----------
resource "snowflake_account_role" "delivery" {
  name    = "${upper(var.project_name)}_ROLE"
  comment = "Role principal pour opérer sur le projet delivery."
}

# ---------- Grants : le role peut utiliser le warehouse ----------
resource "snowflake_grant_privileges_to_account_role" "warehouse_usage" {
  account_role_name = snowflake_account_role.delivery.name
  privileges        = ["USAGE", "OPERATE"]

  on_account_object {
    object_type = "WAREHOUSE"
    object_name = snowflake_warehouse.main.name
  }
}

# ---------- Grants : le role peut utiliser la database ----------
resource "snowflake_grant_privileges_to_account_role" "database_usage" {
  account_role_name = snowflake_account_role.delivery.name
  privileges        = ["USAGE"]

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.main.name
  }
}


# ---------- Grants : le role a tous les droits sur le schema RAW ----------
resource "snowflake_grant_privileges_to_account_role" "raw_schema_all" {
  account_role_name = snowflake_account_role.delivery.name
  privileges = [
    "USAGE",
    "CREATE TABLE",
    "CREATE STAGE",
    "CREATE FILE FORMAT",
    "CREATE VIEW",
  ]

  on_schema {
    schema_name = "${snowflake_database.main.name}.${snowflake_schema.raw.name}"
  }
}
# ---------- Grant : créer des schemas (nécessaire pour dbt) ----------
# dbt crée les schemas STAGING, INTERMEDIATE, MARTS.
resource "snowflake_grant_privileges_to_account_role" "database_create_schema" {
  account_role_name = snowflake_account_role.delivery.name
  privileges        = ["CREATE SCHEMA"]

  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.main.name
  }
}

# ---------- Attribution du role à ton user ----------
resource "snowflake_grant_account_role" "delivery_to_user" {
  role_name = snowflake_account_role.delivery.name
  user_name = var.snowflake_user
}

# ---------- Outputs ----------
output "snowflake_database" {
  description = "Nom de la database Snowflake."
  value       = snowflake_database.main.name
}

output "snowflake_schema" {
  description = "Nom du schema RAW."
  value       = "${snowflake_database.main.name}.${snowflake_schema.raw.name}"
}

output "snowflake_warehouse" {
  description = "Nom du warehouse."
  value       = snowflake_warehouse.main.name
}

output "snowflake_role" {
  description = "Nom du role principal."
  value       = snowflake_account_role.delivery.name
}