# database/crud.py

from sqlalchemy import Table , update , delete
from sqlalchemy.orm import Session
from sqlalchemy.sql import select
from app.database.connect import metadata
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy import Column, Integer, String, ForeignKey, Float, Boolean, Text, DateTime, Date, DECIMAL
from sqlalchemy.exc import SQLAlchemyError
from app.database.connect import engine
from sqlalchemy.sql import func
from app.database.conditions import read_build_conditions, update_build_conditions, delete_build_conditions


def read_table(db: Session, table_name: str, columns: list = None, condition: dict = None):
    # Reflect the table from the database
    table = Table(table_name, metadata, autoload_with=db.bind)
    
    # If no columns are specified, select all columns
    if columns is None:
        selected_columns = table.c
    else:
        selected_columns = [table.c[column] for column in columns]
    
    # Build the query
    query = select(*selected_columns)  # Unpack the columns list
    
    # Apply condition if provided
    if condition:
        condition_clause = read_build_conditions(table, condition)
        query = query.where(condition_clause)
    
    # Execute the query and fetch the results
    result = db.execute(query).fetchall()
    
    # Convert the result rows to dictionaries
    return [dict(row._mapping) for row in result]


def insert_record(db: Session, table_name: str, values: list, columns: list = None):
    # Reflect the table from the database
    table = Table(table_name, metadata, autoload_with=db.bind)

    # If no columns are specified, use all columns
    if columns is None:
        columns = [col.name for col in table.columns]
    
    # Ensure that each value entry is a dictionary mapping column names to values
    if not isinstance(values[0], dict):
        values = [dict(zip(columns, val)) for val in values]
    
    # Prepare the insert statement
    insert_stmt = table.insert().values(values)

    # Execute the insert statement
    db.execute(insert_stmt)
    db.commit()

    return {"message": "Records inserted successfully"}

def update_table(db: Session, table_name: str, updates: dict, condition: dict = None):
    """
    Updates records in the given table dynamically based on complex conditions.
    
    :param db: SQLAlchemy session.
    :param table_name: The name of the table to update records.
    :param updates: A dictionary of columns and their new values.
    :param condition: A dictionary of conditions to filter the rows to update.
    :return: Number of rows updated or an error message.
    """
    try:
        # Reflect the table from the database
        table = Table(table_name, metadata, autoload_with=db.bind)
        
        # Build the update query with the new values
        query = update(table).values(**updates)
        
        # Apply complex conditions if provided
        if condition:
            condition_clause = update_build_conditions(table, condition)
            query = query.where(condition_clause)
        
        # Execute the update query
        result = db.execute(query)
        db.commit()
        
        return {"rows_updated": result.rowcount}
    
    except SQLAlchemyError as e:
        db.rollback()
        return {"error": str(e)}

def delete_records(db: Session, table_name: str, condition: dict):
    """
    Deletes records from the specified table dynamically based on complex conditions.
    
    :param db: SQLAlchemy session.
    :param table_name: The name of the table from which to delete records.
    :param condition: A dictionary of conditions to filter the rows to delete.
    :return: The number of rows deleted or an error message.
    """
    try:
        # Reflect the table from the database
        table = Table(table_name, metadata, autoload_with=db.bind)
        
        # Build the delete query
        delete_query = delete(table)
        
        # Apply complex conditions if provided
        if condition:
            condition_clause = delete_build_conditions(table, condition)
            delete_query = delete_query.where(condition_clause)
        else:
            raise ValueError("Condition is required for deletion to avoid accidental data loss.")
        
        # Execute the delete query
        result = db.execute(delete_query)
        db.commit()
        
        return {"deleted_rows": result.rowcount}
    
    except SQLAlchemyError as e:
        db.rollback()
        return {"error": str(e)}

def create_table(db: Session, table_name: str, columns: list, primary_key: list = None, foreign_keys: list = None):
    # Define a list to hold Column objects
    table_columns = []

    for column in columns:
        column_name = column.name  # Use dot notation to access attributes
        column_type = column.type
        nullable = getattr(column, 'nullable', True)
        autoincrement = getattr(column, 'autoincrement', False)
        default = getattr(column, 'default', None)
        unique = getattr(column, 'unique', False)
        index = getattr(column, 'index', False)
        onupdate = getattr(column, 'onupdate', None)
        server_default = getattr(column, 'server_default', None)
        comment = getattr(column, 'comment', None)

        # Handle data types dynamically
        if column_type == 'Integer':
            col_type = Integer
        elif column_type == 'String':
            col_type = String(getattr(column, 'length', 255))
        elif column_type == 'Float':
            col_type = Float
        elif column_type == 'Boolean':
            col_type = Boolean
        elif column_type == 'Text':
            col_type = Text
        elif column_type == 'DateTime':
            col_type = DateTime
            if server_default == "CURRENT_TIMESTAMP":
                server_default = func.current_timestamp()
        elif column_type == 'Date':
            col_type = Date
        elif column_type == 'Decimal':
            col_type = DECIMAL(precision=getattr(column, 'precision', 10), scale=getattr(column, 'scale', 2))
        else:
            raise ValueError(f"Unsupported column type: {column_type}")

        # Define the column
        col = Column(column_name, col_type, nullable=nullable, autoincrement=autoincrement, unique=unique, index=index,
                     server_default=server_default, onupdate=onupdate, comment=comment)

        table_columns.append(col)

    # Handle primary keys
    if primary_key:
        for pk in primary_key:
            for col in table_columns:
                if col.name == pk:
                    col.primary_key = True

    # Handle foreign keys
    if foreign_keys:
        for fk in foreign_keys:
            referenced_table = fk.referenced_table  # Use dot notation to access attributes
            referenced_column = fk.referenced_column
            ondelete = getattr(fk, 'ondelete', None)
            onupdate = getattr(fk, 'onupdate', None)
            for col in table_columns:
                if col.name == fk.column:
                    col.append_foreign_key(ForeignKey(f'{referenced_table}.{referenced_column}', ondelete=ondelete, onupdate=onupdate))

    # Dynamically create the table
    table = Table(table_name, metadata, *table_columns, extend_existing=True)

    try:
        metadata.create_all(engine)  # Create the table in the database
        return {"message": f"Table '{table_name}' created successfully"}
    except SQLAlchemyError as e:
        raise ValueError(str(e))
