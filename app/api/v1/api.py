from fastapi import APIRouter

from app.api.v1.endpoints import documents

router = APIRouter()


router.include_router(documents.router, prefix="/documents", tags=["Documents"])
