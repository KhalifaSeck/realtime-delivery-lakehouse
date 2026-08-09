# ============================================================
# Providers Terraform pour la plateforme delivery lakehouse.
#
# On utilise deux providers :
#  - azurerm  : pour provisionner ADLS Gen2 (stockage cloud)
#  - snowflake : pour provisionner la base, schema, warehouse,
#                stage externe, tables RAW
#
# Terraform gère l'infra de bout en bout, du stockage aux tables.
# ============================================================

terraform {
  required_version = ">= 1.5"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    snowflake = {
      source  = "Snowflake-Labs/snowflake"
      version = "~> 1.0"
    }
  }
}

# Provider Azure — utilise le token du 'az login' pour s'authentifier.
provider "azurerm" {
  features {}
  subscription_id = var.azure_subscription_id
}

# Provider Snowflake — s'authentifie via login/password.
# En production on utiliserait une key pair ou OAuth, mais pour un
# projet portfolio le user/password est acceptable.
provider "snowflake" {
  organization_name = var.snowflake_organization_name
  account_name      = var.snowflake_account_name
  user              = var.snowflake_user
  password          = var.snowflake_password
  role              = var.snowflake_role
  warehouse         = var.snowflake_warehouse

  # Active TOUTES les preview features en une fois.
  preview_features_enabled = [
    "snowflake_table_resource",
    "snowflake_file_format_resource",
    "snowflake_stage_resource",
  ]
}