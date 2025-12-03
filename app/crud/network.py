from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.models.network import NetworkData
from app.schemas.network import NetworkDataCreate


def create_network_data(db: Session, item: NetworkDataCreate) -> NetworkData | None:
    db_obj = NetworkData(**item.dict())

    try:
        db.add(db_obj)
        db.commit()
        db.refresh(db_obj)
        return db_obj

    except IntegrityError:
        db.rollback()
        # duplicate row: skip safely
        return None


def bulk_insert_network_data(db: Session, items: list[NetworkDataCreate]):
    inserted = []
    for item in items:
        obj = create_network_data(db, item)
        if obj:
            inserted.append(obj)
    return inserted
