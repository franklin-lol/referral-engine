from fastapi import APIRouter
from api.v1 import users, referrals, distributions

api_router = APIRouter()
api_router.include_router(users.router,         prefix="/users",         tags=["users"])
api_router.include_router(referrals.router,     prefix="/referrals",     tags=["referrals"])
api_router.include_router(distributions.router, prefix="/distributions", tags=["distributions"])
