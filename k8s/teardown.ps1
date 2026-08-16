# ============================================================
# Supprime tout le stack delivery de Kubernetes.
# Usage : .\k8s\teardown.ps1
# ============================================================

Write-Host "=== Suppression du namespace delivery ===" -ForegroundColor Red
kubectl delete namespace delivery
Write-Host "=== Namespace supprime ===" -ForegroundColor Green