from fastapi import FastAPI, HTTPException, Depends, Query
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Optional
import pandas as pd
from cryptography.fernet import Fernet
import base64

# Dummy data models for demonstration purposes
class Apprenticeship(BaseModel):
    id: int
    name: str
    modified_date: str

class Project(BaseModel):
    id: int
    apprenticeship_id: int
    name: str

class Programme(BaseModel):
    id: int
    name: str

class ApprenticeshipResponse(BaseModel):
    data: List[Apprenticeship]
    pagination: Optional[dict]

class ProjectResponse(BaseModel):
    data: List[Project]
    pagination: Optional[dict]

class ProgrammeResponse(BaseModel):
    data: List[Programme]
    pagination: Optional[dict]

USERNAME = "multiverse"
PASSWORD = "mult1v3r53"

app = FastAPI(title="Multiverse Data Engineering Task API")

# CORS
origins = [
    "http://localhost"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Load data from CSV files
apprenticeships_df = pd.read_csv('data/apprenticeships.csv')
projects_df = pd.read_csv('data/projects.csv')
programmes_df = pd.read_csv('data/programmes.csv')

# OAuth2 scheme
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="login")

# OAuth2 authentication endpoint
@app.post("/login")
async def login(form_data: OAuth2PasswordRequestForm = Depends()):
    if form_data.username == USERNAME and form_data.password == PASSWORD:
        return {"access_token": "session_token", "token_type": "bearer"}
    raise HTTPException(status_code=400, detail="Invalid username or password")

@app.options("/login")
async def login_options():
    return {"msg": "OPTIONS request on /login endpoint"}

# Dependencies
async def get_current_user(token: str = Depends(oauth2_scheme)):
    if token != "session_token":
        raise HTTPException(status_code=401, detail="Invalid token")
    return {"username": USERNAME}

key = Fernet.generate_key()
cipher_suite = Fernet(key)

def encode_pagination_token(token: int) -> str:
    token_bytes = token.to_bytes((token.bit_length() + 7) // 8, 'big')
    encrypted_token = cipher_suite.encrypt(token_bytes)
    return base64.urlsafe_b64encode(encrypted_token).decode()

def decode_pagination_token(token: str) -> int:
    encrypted_token = base64.urlsafe_b64decode(token.encode())
    decrypted_token = cipher_suite.decrypt(encrypted_token)
    return int.from_bytes(decrypted_token, 'big')

def paginate(data, page_size, pagination_token):
    start_index = 0 if not pagination_token else decode_pagination_token(pagination_token)
    end_index = start_index + page_size
    next_token = encode_pagination_token(end_index) if end_index < len(data) else None
    previous_token = encode_pagination_token(start_index - page_size) if start_index > 0 else None
    return data[start_index:end_index], next_token, previous_token

# Get all apprenticeships
@app.get("/apprenticeships", response_model=ApprenticeshipResponse, dependencies=[Depends(get_current_user)])
async def get_apprenticeships(modified_after: Optional[str] = None, page_size: int = 20, pagination_token: Optional[str] = None):
    data = apprenticeships_df
    if modified_after:
        data = data[data['modified_date'] > modified_after]
    data = data.to_dict('records')
    paginated_data, next_token, previous_token = paginate(data, page_size, pagination_token)
    return {"data": paginated_data, "pagination": {"next_token": next_token, "previous_token": previous_token, "page_size": page_size}}

# Get projects for a specific apprenticeship
@app.get("/apprenticeships/{id}/projects", response_model=ProjectResponse, dependencies=[Depends(get_current_user)])
async def get_projects(id: int, page_size: int = 20, pagination_token: Optional[str] = None):
    data = projects_df[projects_df['apprenticeship_id'] == id].to_dict('records')
    paginated_data, next_token, previous_token = paginate(data, page_size, pagination_token)
    return {"data": paginated_data, "pagination": {"next_token": next_token, "previous_token": previous_token, "page_size": page_size}}

# Get all programmes
@app.get("/programmes", response_model=ProgrammeResponse, dependencies=[Depends(get_current_user)])
async def get_programmes(page_size: int = 20, pagination_token: Optional[str] = None):
    data = programmes_df.to_dict('records')
    paginated_data, next_token, previous_token = paginate(data, page_size, pagination_token)
    return {"data": paginated_data, "pagination": {"next_token": next_token, "previous_token": previous_token, "page_size": page_size}}

@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="API Docs", oauth2_redirect_url="/docs/oauth2-redirect")
