"""
Application FastAPI de suivi de livraisons en temps réel.

Assemble les routers (vehicles, health) en une API qui lit l'état
courant des véhicules dans Redis — état maintenu par le pipeline Spark.

Lancement : uvicorn api.main:app --reload
Doc interactive : http://localhost:8000/docs
"""
from fastapi import FastAPI

from api.routers import vehicles, health

app = FastAPI(
    title="Realtime Delivery API",
    description="Suivi temps réel des véhicules de livraison (lecture Redis).",
    version="0.1.0",
)

# Enregistrement des routers.
app.include_router(health.router)
app.include_router(vehicles.router)


@app.get("/", tags=["root"])
def root() -> dict:
    """Point d'entrée : redirige mentalement vers la doc."""
    return {
        "service": "Realtime Delivery API",
        "docs": "/docs",
        "endpoints": ["/health", "/vehicles", "/vehicles/{vehicle_id}"],
    }