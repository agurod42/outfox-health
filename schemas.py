from typing import Optional

from pydantic import BaseModel, Field


class ProviderOut(BaseModel):
    provider_id: str
    provider_name: str
    city: Optional[str] = None
    state: Optional[str] = None
    zip: str
    ms_drg_code: Optional[str] = None
    ms_drg_description: Optional[str] = None
    total_discharges: Optional[int] = None
    avg_covered_charges: Optional[float] = Field(default=None, description="USD")
    avg_total_payments: Optional[float] = Field(default=None, description="USD")
    avg_medicare_payments: Optional[float] = Field(default=None, description="USD")
    rating: Optional[int] = None


class AskRequest(BaseModel):
    question: Optional[str] = None
    include_sql: Optional[bool] = False


class AskResponse(BaseModel):
    answer: str
    results: list[ProviderOut] = []
    follow_up: Optional[str] = None
    sql: Optional[str] = None


