from pydantic import BaseModel
from typing import List, Optional
from enum import Enum


class StatusEnum(str, Enum):
    pending = "pending"
    submitted = "submitted"
    graded = "graded"

class Apprenticeship(BaseModel):
    id: int
    name: str
    programme_id: int
    modified_date: str
    first_name: str
    last_name: str
    email_address: str

class Project(BaseModel):
    id: int
    title: str
    description: str
    status: StatusEnum

class Programme(BaseModel):
    id: int
    name: str
    duration: int
    description: str

class Pagination(BaseModel):
    next_token: Optional[str]
    previous_token: Optional[str]
    page_size: int

class ApprenticeshipResponse(BaseModel):
    data: List[Apprenticeship]
    pagination: Pagination

class ProjectResponse(BaseModel):
    data: List[Project]
    pagination: Pagination

class ProgrammeResponse(BaseModel):
    data: List[Programme]
    pagination: Pagination
