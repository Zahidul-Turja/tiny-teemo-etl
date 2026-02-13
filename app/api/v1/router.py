from fastapi import APIRouter

from app.api.v1.endpoints import files, utilities

router = APIRouter()


router.include_router(files.router, prefix="/files", tags=["files"])
router.include_router(utilities.router, prefix="/utilities", tags=["Utilities"])
