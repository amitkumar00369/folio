from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.api.deps import get_db
from app.schemas.folio import FolioOut
from app.services import folio_service

router = APIRouter(prefix="/api/folios", tags=["folios"])


@router.get("", response_model=list[FolioOut])
def list_folios(db: Session = Depends(get_db)):
    return folio_service.list_folios(db)


@router.get("/{folio_id}", response_model=FolioOut)
def get_folio(folio_id: str, db: Session = Depends(get_db)):
    return folio_service.get_folio(db, folio_id)
