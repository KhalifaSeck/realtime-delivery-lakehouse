# ============================================================
# Variables Terraform.
#
# Toutes les valeurs qui pourraient changer (nom de projet, région,
# credentials Snowflake) sont ici. Les vraies valeurs vont dans
# terraform.tfvars (fichier .gitignore, jamais commité).
# ============================================================

# ---------- Métadonnées projet ----------
variable "project_name" {
  description = "Nom du projet (préfixe des ressources)."
  type        = string
  default     = "delivery"
}

variable "environment" {
  description = "Environnement (dev, prod, ...)."
  type        = string
  default     = "dev"
}

variable "azure_location" {
  description = "Région Azure pour les ressources."
  type        = string
  default     = "canadacentral"   # Le plus proche de Montréal
}

# ---------- Azure ----------
variable "azure_subscription_id" {
  description = "Subscription ID Azure (visible via 'az account show')."
  type        = string
  # Pas de default : à fournir dans terraform.tfvars
}

# ---------- Snowflake ----------
# Structure Snowflake moderne :
# https://{ORG}-{ACCOUNT}.snowflakecomputing.com
variable "snowflake_organization_name" {
  description = "Organization name Snowflake (première partie de l'URL)."
  type        = string
}

variable "snowflake_account_name" {
  description = "Account name Snowflake (deuxième partie de l'URL)."
  type        = string
}

variable "snowflake_user" {
  description = "User Snowflake (BAKISSECK96 pour toi)."
  type        = string
}

variable "snowflake_password" {
  description = "Password Snowflake."
  type        = string
  sensitive   = true   # Terraform le masquera dans les logs
}

variable "snowflake_role" {
  description = "Role Snowflake pour Terraform."
  type        = string
  default     = "ACCOUNTADMIN"
}

variable "snowflake_warehouse" {
  description = "Warehouse par défaut."
  type        = string
  default     = "COMPUTE_WH"   # Warehouse par défaut du trial
}