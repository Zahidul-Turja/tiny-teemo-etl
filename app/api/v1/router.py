from app.api.v1.endpoints import files, utilities
from fastapi import APIRouter

router = APIRouter()


router.include_router(files.router, prefix="/documents", tags=["Documents"])
router.include_router(utilities.router, prefix="/utilities", tags=["Utilities"])
