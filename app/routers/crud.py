# routers/crud.py

from fastapi import APIRouter, Depends , HTTPException
from sqlalchemy.orm import Session
from app.database.crud import read_table , insert_record , update_table , delete_records , create_table
import json
from typing import Optional
from sqlalchemy.exc import IntegrityError
from typing import Optional
import traceback
from app.routers.utils import get_db, InsertRequest, DeleteRequest, CreateTableRequest
from app.utils.logger import log_performance

router = APIRouter()

# Dependency to get the database session

@log_performance
@router.get("/read_table/")
def read_table_route(
    table_name: str, 
    columns: Optional[str] = None, 
    condition: Optional[str] = None, 
    db: Session = Depends(get_db)
):  
    """
        API endpoint to read records from a specified table based on complex conditions.
        
        Args :
        table_name : The name of the table to read records from.
        columns : A list of columns to select from the table.
        condition : A dictionary of conditions to filter the rows to read.
        db : SQLAlchemy session.
        
        Returns :
        A list of dictionaries representing the selected columns of the records that match the condition.
    """
    
    try:
        # Parse the columns if provided
        columns_list = columns.split(',') if columns else None

        # Parse the condition if provided
        condition_dict = json.loads(condition) if condition else None
        
        # Fetch data using the dynamic_read function
        data = read_table(db, table_name, columns=columns_list, condition=condition_dict)
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@log_performance
@router.post("/insert/{table_name}")
def insert_record_route(table_name: str, request: InsertRequest, db: Session = Depends(get_db)):
    """
        API endpoint to insert records into a specified table.
        
        Args :
        table_name : The name of the table.
        request : Request body containing the values to insert.
        db : Database session.
        
        Returns :
        A message indicating the success of the operation.
    """
    
    try:
        result = insert_record(db, table_name, request.values, request.columns)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except IntegrityError as e:
        raise HTTPException(status_code=400, detail=f"Integrity error: {str(e)}")
    except Exception as e:
        traceback_str = traceback.format_exc()
        print(traceback_str)
        raise HTTPException(status_code=500, detail="An unexpected error occurred")


@log_performance
@router.put("/update/{table_name}")
def update_table_route(table_name: str, updates: dict, condition: dict = None, db: Session = Depends(get_db)):
    """
        API endpoint to update records in a specified table.
        
        Args : 
        table_name : The name of the table.
        updates : A dictionary of columns to update and their new values.
        condition : A dictionary of conditions to filter the rows to update.
        db : Database session.
        
        Returns :
        A message indicating the success of the operation.
    """
    result = update_table(db, table_name, updates, condition)
    
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    
    return result

@log_performance
@router.delete("/delete/{table_name}")
def delete_records_route(table_name: str, delete_request: DeleteRequest, db: Session = Depends(get_db)):
    """
        API endpoint to delete records from a specified table based on complex conditions.
        
        Args :
        table_name : The name of the table to delete records from.
        delete_request : Request body containing the conditions to filter the rows to delete.
        db : SQLAlchemy session.
        
        Returns :
        A message indicating the success of the operation.
    """
    try:
        condition = delete_request.condition
        result = delete_records(db, table_name, condition)
        return {"message": "Records deleted successfully", "deleted_rows": result["deleted_rows"]}
    except ValueError as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))    
    
@log_performance
@router.post("/create_table")
def create_table_route(create_table_request: CreateTableRequest, db: Session = Depends(get_db)):
    """
        API endpoint to create a new table in the database.
        
        Args :
        create_table_request : Request body containing the table schema.
        db : SQLAlchemy session.
        
        Returns :
        A message indicating the success of the operation.
    """
    try:
        result = create_table(
            db,
            table_name=create_table_request.table_name,
            columns=create_table_request.columns,
            primary_key=create_table_request.primary_key,
            foreign_keys=create_table_request.foreign_keys,
        )
        return result
    except ValueError as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        print(traceback.format_exc())
        raise HTTPException(status_code=500, detail=str(e))