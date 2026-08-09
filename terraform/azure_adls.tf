# ============================================================
# Azure ADLS Gen2 pour le lake delivery.
#
# Architecture créée :
#   Resource Group
#     └─ Storage Account (avec Hierarchical Namespace = ADLS Gen2)
#          └─ Container "raw-events" (équivalent d'un bucket S3)
#
# Le container "raw-events" contiendra les Parquet uploadés depuis
# le lake local Windows. Snowflake s'y connectera via un stage externe.
# ============================================================

# ---------- Resource Group ----------
resource "azurerm_resource_group" "main" {
  name     = "${var.project_name}-${var.environment}-rg"
  location = var.azure_location

  tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
}

# ---------- Storage Account (ADLS Gen2) ----------
# Un Storage Account avec 'is_hns_enabled = true' devient ADLS Gen2.
# HNS = Hierarchical Namespace, indispensable pour supporter les
# "dossiers" (comme sur un vrai filesystem).
resource "azurerm_storage_account" "lake" {
  name = "${var.project_name}${var.environment}lakesa"
  # Le nom doit être :
  #   - unique dans tout Azure (globalement)
  #   - 3 à 24 caractères
  #   - minuscules et chiffres uniquement (pas de tirets)

  resource_group_name = azurerm_resource_group.main.name
  location            = azurerm_resource_group.main.location

  account_tier             = "Standard"
  account_replication_type = "LRS"  # Locally Redundant Storage (le moins cher)
  account_kind             = "StorageV2"

  # Le drapeau qui transforme un Storage Account en ADLS Gen2.
  is_hns_enabled = true

  # Pour rendre l'accès via SAS token possible.
  shared_access_key_enabled = true

  tags = azurerm_resource_group.main.tags
}

# ---------- Container "raw-events" ----------
# Un container est l'équivalent d'un bucket S3 ou d'un top-level
# folder ADLS. On y déposera les Parquet du lake.
resource "azurerm_storage_container" "raw_events" {
  name                  = "raw-events"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# ---------- Outputs (valeurs affichées après 'terraform apply') ----------
output "adls_account_name" {
  description = "Nom du Storage Account ADLS Gen2 créé."
  value       = azurerm_storage_account.lake.name
}

output "adls_container_name" {
  description = "Nom du container raw-events."
  value       = azurerm_storage_container.raw_events.name
}

output "adls_endpoint" {
  description = "URL DFS du Storage Account (pour Snowflake stage)."
  value       = azurerm_storage_account.lake.primary_dfs_endpoint
}

# ============================================================
# SAS token pour l'accès Snowflake au container raw-events.
#
# Snowflake s'authentifie auprès d'ADLS via ce token temporaire.
# Le SAS a une durée de vie limitée (1 an ici) et donne uniquement
# les droits nécessaires : read + list sur le container.
#
# En production : on utiliserait une Storage Integration Azure AD
# (impossible avec compte étudiant, mais documenté dans README).
# ============================================================

data "azurerm_storage_account_sas" "lake_read" {
  connection_string = azurerm_storage_account.lake.primary_connection_string
  https_only        = true
  signed_version    = "2022-11-02"

  resource_types {
    service   = false
    container = true    # accès aux containers
    object    = true    # accès aux objets (fichiers Parquet)
  }

  services {
    blob  = true        # ADLS Gen2 utilise l'API Blob
    queue = false
    table = false
    file  = false
  }

  # Validité : 1 an à partir de la création.
  start  = "2026-08-08T00:00:00Z"
  expiry = "2027-08-08T00:00:00Z"

  permissions {
    read    = true
    write   = false     # Snowflake lit seulement, n'écrit pas
    delete  = false
    list    = true      # pour lister les fichiers du container
    add     = false
    create  = false
    update  = false
    process = false
    tag     = false
    filter  = false
  }
}

# Output du SAS (sensible, masqué dans les logs Terraform).
output "adls_sas_token" {
  description = "SAS token pour Snowflake (READ+LIST sur raw-events)."
  value       = data.azurerm_storage_account_sas.lake_read.sas
  sensitive   = true
}

# ============================================================
# SAS token pour l'ingestion Python (WRITE + LIST).
#
# Séparé du SAS Snowflake pour respecter le principe du least
# privilege : chaque acteur a exactement les droits nécessaires.
#
#   - Snowflake : READ + LIST (via data.azurerm_storage_account_sas.lake_read)
#   - Python    : WRITE + LIST (via ce nouveau bloc)
# ============================================================

data "azurerm_storage_account_sas" "lake_write" {
  connection_string = azurerm_storage_account.lake.primary_connection_string
  https_only        = true
  signed_version    = "2022-11-02"

  resource_types {
    service   = false
    container = true
    object    = true
  }

  services {
    blob  = true
    queue = false
    table = false
    file  = false
  }

  start  = "2026-08-08T00:00:00Z"
  expiry = "2027-08-08T00:00:00Z"

  permissions {
    read    = true
    write   = true      # écriture pour ingestion Python
    delete  = false     # on ne veut pas supprimer accidentellement
    list    = true
    add     = true      # append blocks
    create  = true      # créer de nouveaux blobs
    update  = false
    process = false
    tag     = false
    filter  = false
  }
}

# Output du SAS write (sensible).
output "adls_sas_token_write" {
  description = "SAS token pour l'ingestion Python (WRITE+LIST sur raw-events)."
  value       = data.azurerm_storage_account_sas.lake_write.sas
  sensitive   = true
}