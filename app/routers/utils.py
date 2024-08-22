from app.database.connect import SessionLocal
from typing import Optional
from pydantic import BaseModel
from typing import List, Optional



def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

class InsertRequest(BaseModel):
    columns: Optional[List[str]] = None
    values: List

    class Config:
        schema_extra = {
            "example": {
                "columns": ["name", "email", "password"],
                "values": ["Aditya", "aditya.as@somaiya.edu", "hashed_password"]
            }
        }
        
class DeleteRequest(BaseModel):
    condition: dict
    
class ColumnSchema(BaseModel):
    name: str
    type: str
    nullable: bool = True
    autoincrement: bool = False
    length: int = None  # Only used if type is String
    default: any = None
    unique: bool = False
    index: bool = False
    precision: int = None  # For Decimal types
    scale: int = None  # For Decimal types
    onupdate: any = None
    server_default: str = None
    comment: str = ""
    
    class Config:
        arbitrary_types_allowed = True

class ForeignKeySchema(BaseModel):
    column: str
    referenced_table: str
    referenced_column: str
    ondelete: str = None
    onupdate: str = None

class CreateTableRequest(BaseModel):
    table_name: str
    columns: list[ColumnSchema]
    primary_key: list[str] = None
    foreign_keys: list[ForeignKeySchema] = None