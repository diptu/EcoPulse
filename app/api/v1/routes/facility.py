# app/api/v1/routes/facility.py


from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.facility import facility_crud
from app.db.session import get_db
from app.schemas.facility import FacilityCreate, FacilityRead

router = APIRouter(prefix="/facilities", tags=["facilities"])


# -------------------------------------------------
# Create Facility
# -------------------------------------------------
@router.post(
    "/",
    response_model=FacilityRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_facility(
    request: Request, facility_in: FacilityCreate, db: AsyncSession = Depends(get_db)
) -> FacilityRead:
    # Optional: uniqueness check (e.g., code or name)
    existing = await facility_crud.get_by_code(db, facility_in.code)
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Facility with code {facility_in.code} already exists.",
        )

    facility = await facility_crud.create(db, facility_in)

    return facility


# # -------------------------------------------------
# # List Facilities
# # -------------------------------------------------
# @router.get(
#     "/",
#     response_model=APIResponse,
#     dependencies=[Depends(require_permissions(["facility.read"]))],
#     openapi_extra={**LIST_FACILITY_DOCS, "security": [{"BearerAuth": []}]},
# )
# async def list_facilities(
#     request: Request,
#     skip: int = Query(0, ge=0),
#     limit: int = Query(settings.DEFAULT_PAGE_LIMIT, ge=1),
#     db: AsyncSession = Depends(get_db),
#     current_user=Depends(get_cached_current_user),
# ) -> APIResponse:
#     total = await facility_crud.count(db)
#     facilities = await facility_crud.get_all(db, skip=skip, limit=limit)

#     items = [FacilityRead.model_validate(f) for f in facilities]
#     pagination = paginate(skip=skip, limit=limit, total=total)

#     return build_api_response(
#         request=request,
#         current_user=current_user,
#         result={"facilities": items, **pagination},
#         status_code=status.HTTP_200_OK,
#         meta_extra={"source": "analytics_service"},
#     )


# # -------------------------------------------------
# # Get Facility by ID
# # -------------------------------------------------
# @router.get(
#     "/{facility_id}",
#     response_model=APIResponse,
#     dependencies=[Depends(require_permissions(["facility.read"]))],
#     openapi_extra={**GET_FACILITY_DOCS, "security": [{"BearerAuth": []}]},
# )
# async def get_facility(
#     facility_id: UUID,
#     request: Request,
#     db: AsyncSession = Depends(get_db),
#     current_user=Depends(get_cached_current_user),
# ) -> APIResponse:
#     facility = await fetch_or_404(facility_crud.get, db, facility_id)

#     return build_api_response(
#         request=request,
#         current_user=current_user,
#         result=FacilityRead.model_validate(facility),
#         status_code=status.HTTP_200_OK,
#     )


# # -------------------------------------------------
# # Update Facility
# # -------------------------------------------------
# @router.put(
#     "/{facility_id}",
#     response_model=APIResponse,
#     dependencies=[Depends(require_permissions(["facility.update"]))],
#     openapi_extra={**UPDATE_FACILITY_DOCS, "security": [{"BearerAuth": []}]},
# )
# async def update_facility(
#     facility_id: UUID,
#     facility_in: FacilityUpdate,
#     request: Request,
#     db: AsyncSession = Depends(get_db),
#     current_user=Depends(get_cached_current_user),
# ) -> APIResponse:
#     facility = await fetch_or_404(facility_crud.get, db, facility_id)
#     facility = await facility_crud.update(db, facility, facility_in)

#     return build_api_response(
#         request=request,
#         current_user=current_user,
#         result=FacilityRead.model_validate(facility),
#         status_code=status.HTTP_200_OK,
#     )


# # -------------------------------------------------
# # Delete Facility
# # -------------------------------------------------
# @router.delete(
#     "/{facility_id}",
#     response_model=APIResponse,
#     dependencies=[Depends(require_permissions(["facility.delete"]))],
#     openapi_extra={**DELETE_FACILITY_DOCS, "security": [{"BearerAuth": []}]},
# )
# async def delete_facility(
#     facility_id: UUID,
#     request: Request,
#     db: AsyncSession = Depends(get_db),
#     current_user=Depends(get_cached_current_user),
# ):
#     facility = await fetch_or_404(facility_crud.get, db, facility_id)
#     await facility_crud.delete(db, facility.id)

#     return build_api_response(
#         request=request,
#         current_user=current_user,
#         result={"message": f"Facility {facility.code} deleted successfully"},
#         include_user_context=False,
#         status_code=200,
#     )


# # -------------------------------------------------
# # Activate Facility
# # -------------------------------------------------
# @router.post(
#     "/{facility_id}/activate",
#     response_model=APIResponse,
#     dependencies=[Depends(require_permissions(["facility.update"]))],
#     openapi_extra={**ACTIVATE_FACILITY_DOCS, "security": [{"BearerAuth": []}]},
# )
# async def activate_facility(
#     facility_id: UUID,
#     request: Request,
#     db: AsyncSession = Depends(get_db),
#     current_user=Depends(get_cached_current_user),
# ):
#     facility = await fetch_or_404(facility_crud.get, db, facility_id)
#     facility = await facility_crud.activate(db, facility.id)

#     return build_api_response(
#         request=request,
#         current_user=current_user,
#         result={"message": f"Facility {facility.code} activated successfully"},
#         status_code=200,
#     )


# # -------------------------------------------------
# # Deactivate Facility
# # -------------------------------------------------
# @router.put(
#     "/{facility_id}/deactivate",
#     response_model=APIResponse,
#     dependencies=[Depends(require_permissions(["facility.update"]))],
#     openapi_extra={**DEACTIVATE_FACILITY_DOCS, "security": [{"BearerAuth": []}]},
# )
# async def deactivate_facility(
#     facility_id: UUID,
#     request: Request,
#     db: AsyncSession = Depends(get_db),
#     current_user=Depends(get_cached_current_user),
# ):
#     facility = await fetch_or_404(facility_crud.get, db, facility_id)
#     facility = await facility_crud.deactivate(db, facility.id)

#     return build_api_response(
#         request=request,
#         current_user=current_user,
#         result={"message": f"Facility {facility.code} deactivated successfully"},
#         status_code=200,
#     )
