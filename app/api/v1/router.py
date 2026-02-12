from api.v1.endpoints import documents, utilities
from fastapi import APIRouter

router = APIRouter()


router.include_router(documents.router, prefix="/documents", tags=["Documents"])
router.include_router(utilities.router, prefix="/utilities", tags=["Utilities"])
