from sqlalchemy import or_, and_

def read_build_conditions(table, conditions):
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


def update_build_conditions(table, conditions):
    """
    Recursively builds SQLAlchemy conditions to support complex query conditions like $or, $and, $gt, $lt, etc.
    
    :param table: SQLAlchemy Table object.
    :param conditions: Dictionary of conditions.
    :return: SQLAlchemy condition clause.
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

def delete_build_conditions(table, conditions):
    """
    Recursively builds SQLAlchemy conditions to support complex query conditions like $or, $and, $gt, $lt, etc.
    
    :param table: SQLAlchemy Table object.
    :param conditions: Dictionary of conditions.
    :return: SQLAlchemy condition clause.
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