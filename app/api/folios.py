from fastapi import APIRouter, Depends

from app.api.deps import get_folio_service
from app.schemas.folio import FolioOut
from app.services.folio_service import FolioService

router = APIRouter(prefix="/api/folios", tags=["folios"])


@router.get("", response_model=list[FolioOut])
async def list_folios(service: FolioService = Depends(get_folio_service)):
    return await service.list_folios()


@router.get("/{folio_id}", response_model=FolioOut)
async def get_folio(folio_id: str, service: FolioService = Depends(get_folio_service)):
    return await service.get_folio_view(folio_id)
