from fastapi import FastAPI, status
from fastapi.responses import Response

app = FastAPI(
    title="Teemo - ETL",
    description="Basic ETL (Extract, Transform, Load) system for data migration",
    version="0.0.1",
)


@app.get("/health", status_code=status.HTTP_200_OK)
def health_check():
    return {"message": "Server healthy"}
