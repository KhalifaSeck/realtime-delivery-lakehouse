# ============================================================
# Deploiement complet du stack delivery sur Kubernetes.
# Usage : .\k8s\deploy.ps1
# ============================================================

Write-Host "=== Build des images Docker ===" -ForegroundColor Green

# Build des images custom
docker build -t delivery-api:latest -f api/Dockerfile .
docker build -t delivery-simulator:latest -f simulator/Dockerfile .
docker build -t delivery-spark:latest -f streaming/Dockerfile .

Write-Host "=== Deploiement sur Kubernetes ===" -ForegroundColor Green

# 1. Namespace
kubectl apply -f k8s\namespace.yaml

# 2. Config et secrets
kubectl apply -f k8s\configmap.yaml
kubectl apply -f k8s\secrets.yaml

# 3. Infra (Kafka, Redis)
kubectl apply -f k8s\kafka\
kubectl apply -f k8s\redis\

# 4. Attendre que Kafka soit ready
Write-Host "Attente de Kafka..." -ForegroundColor Yellow
kubectl wait --for=condition=ready pod -l app=kafka -n delivery --timeout=180s

# 5. Observabilite (Prometheus, Grafana)
kubectl apply -f k8s\prometheus\
kubectl apply -f k8s\grafana\

# 6. API
kubectl apply -f k8s\api\

# 7. Spark Streaming (doit démarrer après Kafka et Redis)
Write-Host "Attente de Redis..." -ForegroundColor Yellow
kubectl wait --for=condition=ready pod -l app=redis -n delivery --timeout=60s
kubectl apply -f k8s\spark\

# 8. Simulateur (démarre en dernier, quand tout est prêt)
Write-Host "Attente de Spark..." -ForegroundColor Yellow
Start-Sleep -Seconds 15
kubectl apply -f k8s\simulator\
# 9. Pipeline CronJob
Write-Host "Pipeline CronJob..." -ForegroundColor Yellow
docker build -t delivery-pipeline:latest -f pipeline/Dockerfile .
kubectl apply -f k8s\pipeline\

Write-Host ""
Write-Host "=== Deploiement termine ===" -ForegroundColor Green
Write-Host ""
Write-Host "Services accessibles :" -ForegroundColor Cyan
Write-Host "  Prometheus : http://localhost:30090"
Write-Host "  Grafana    : http://localhost:30030"
Write-Host "  API        : http://localhost:30080"
Write-Host ""
Write-Host "Port-forwarding si necessaire :" -ForegroundColor Cyan
Write-Host "  kubectl port-forward svc/kafka-service 9092:9092 -n delivery"
Write-Host "  kubectl port-forward svc/redis-service 6379:6379 -n delivery"
Write-Host ""
kubectl get pods -n delivery