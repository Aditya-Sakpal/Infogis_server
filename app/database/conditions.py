#This file contains the functions to build the conditions for the read, update and delete operations.

from sqlalchemy import or_, and_
from app.utils.logger import log_performance

@log_performance
def read_build_conditions(table, conditions):
    
    """
        This function is used to build the conditions for the read operation.
        Args : 
            table : SQLAlchemy Table object.
            conditions : Dictionary of conditions.
        Returns :
            SQLAlchemy condition clause.
    """
    
    if isinstance(conditions, dict):
        logic = conditions.get("logic", "and").lower()  # Default to 'and' logic
        subconditions = conditions.get("conditions", [])
        condition_clauses = []

        for subcondition in subconditions:
            if "logic" in subcondition:  # Nested condition
                condition_clauses.append(read_build_conditions(table, subcondition))
            else:  # Simple condition
                column_name = subcondition["column"]
                operator = subcondition["operator"]
                value = subcondition["value"]

                column = table.c[column_name]

                if operator == "=":
                    condition_clauses.append(column == value)
                elif operator == "!=":
                    condition_clauses.append(column != value)
                elif operator == ">":
                    condition_clauses.append(column > value)
                elif operator == "<":
                    condition_clauses.append(column < value)
                elif operator == ">=":
                    condition_clauses.append(column >= value)
                elif operator == "<=":
                    condition_clauses.append(column <= value)
                elif operator == "like":
                    condition_clauses.append(column.like(f"%{value}%"))
                elif operator == "in":
                    condition_clauses.append(column.in_(value))
                else:
                    raise ValueError(f"Unsupported operator: {operator}")

        # Combine the clauses based on the logic
        if logic == "or":
            return or_(*condition_clauses)
        else:
            return and_(*condition_clauses)

    return None

@log_performance
def update_build_conditions(table, conditions):
    """
        This function is used to build the conditions for the update operation.
        Args :
            table : SQLAlchemy Table object.
            conditions : Dictionary of conditions.
        Returns :
            SQLAlchemy condition clause.
    """
    if isinstance(conditions, dict):
        logic = conditions.get("$logic", "and").lower()  # Default to 'and' logic
        subconditions = conditions.get("conditions", [])
        condition_clauses = []

        for subcondition in subconditions:
            if "$logic" in subcondition:  # Nested condition
                condition_clauses.append(update_build_conditions(table, subcondition))
            else:  # Simple condition
                column_name = subcondition["column"]
                operator = subcondition["operator"]
                value = subcondition["value"]

                column = table.c[column_name]

                # Handle different operators
                if operator == "=":
                    condition_clauses.append(column == value)
                elif operator == "!=":
                    condition_clauses.append(column != value)
                elif operator == "$gt":
                    condition_clauses.append(column > value)
                elif operator == "$lt":
                    condition_clauses.append(column < value)
                elif operator == "$gte":
                    condition_clauses.append(column >= value)
                elif operator == "$lte":
                    condition_clauses.append(column <= value)
                elif operator == "$like":
                    condition_clauses.append(column.like(f"%{value}%"))
                elif operator == "$in":
                    condition_clauses.append(column.in_(value))
                else:
                    raise ValueError(f"Unsupported operator: {operator}")

        # Combine the clauses based on the logic
        if logic == "or":
            return or_(*condition_clauses)
        else:  # Default is "and"
            return and_(*condition_clauses)
    
    return None

@log_performance
def delete_build_conditions(table, conditions):
    """
        This function is used to build the conditions for the delete operation.
        Args :
            table : SQLAlchemy Table object.
            conditions : Dictionary of conditions.
        Returns :
            SQLAlchemy condition clause
    """
    if isinstance(conditions, dict):
        logic = conditions.get("$logic", "and").lower()  # Default to 'and' logic
        subconditions = conditions.get("conditions", [])
        condition_clauses = []

        for subcondition in subconditions:
            if "$logic" in subcondition:  # Nested condition
                condition_clauses.append(delete_build_conditions(table, subcondition))
            else:  # Simple condition
                column_name = subcondition["column"]
                operator = subcondition["operator"]
                value = subcondition["value"]

                # Check if the column exists in the table
                if column_name not in table.c:
                    raise KeyError(f"Column '{column_name}' not found in table '{table.name}'. Available columns: {list(table.c.keys())}")
                
                column = table.c[column_name]

                # Handle different operators
                if operator == "=":
                    condition_clauses.append(column == value)
                elif operator == "!=":
                    condition_clauses.append(column != value)
                elif operator == "$gt":
                    condition_clauses.append(column > value)
                elif operator == "$lt":
                    condition_clauses.append(column < value)
                elif operator == "$gte":
                    condition_clauses.append(column >= value)
                elif operator == "$lte":
                    condition_clauses.append(column <= value)
                elif operator == "$like":
                    condition_clauses.append(column.like(f"%{value}%"))
                elif operator == "$in":
                    condition_clauses.append(column.in_(value))
                else:
                    raise ValueError(f"Unsupported operator: {operator}")

        # Combine the clauses based on the logic
        if logic == "or":
            return or_(*condition_clauses)
        else:  # Default is "and"
            return and_(*condition_clauses)

    return None