import sqlite3
from pathlib import Path

import pandas as pd


def create_assignment_database():
    """
    Creates a SQLite database with dummy data for SQL assignment.
    Requires double join to solve the problem.
    """

    # Create database connection
    db_path = Path("sql_assignment.db")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Drop tables if they exist
    cursor.execute("DROP TABLE IF EXISTS projects")
    cursor.execute("DROP TABLE IF EXISTS employees")
    cursor.execute("DROP TABLE IF EXISTS departments")

    # Create departments table
    cursor.execute(
        """
        CREATE TABLE departments (
            department_id INTEGER PRIMARY KEY,
            department_name TEXT NOT NULL
        )
    """
    )

    # Create employees table
    cursor.execute(
        """
        CREATE TABLE employees (
            employee_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            manager_id INTEGER,
            department_id INTEGER,
            FOREIGN KEY (manager_id) REFERENCES employees(employee_id),
            FOREIGN KEY (department_id) REFERENCES departments(department_id)
        )
    """
    )

    # Create projects table
    cursor.execute(
        """
        CREATE TABLE projects (
            project_id INTEGER PRIMARY KEY,
            project_name TEXT NOT NULL,
            employee_id INTEGER,
            department_id INTEGER,
            FOREIGN KEY (employee_id) REFERENCES employees(employee_id),
            FOREIGN KEY (department_id) REFERENCES departments(department_id)
        )
    """
    )

    # Insert department data
    departments_data = [
        (1, "Technology"),
        (2, "Marketing"),
        (3, "Finance"),
        (4, "Human Resources"),
    ]

    cursor.executemany(
        "INSERT INTO departments (department_id, department_name) VALUES (?, ?)",
        departments_data,
    )

    # Insert employee data (including managers)
    employees_data = [
        (1, "Alice Johnson", None, 1),  # Tech manager
        (2, "Bob Smith", 1, 1),  # Tech employee
        (3, "Carol Davis", 1, 1),  # Tech employee
        (4, "David Wilson", None, 2),  # Marketing manager
        (5, "Emma Brown", 4, 2),  # Marketing employee
        (6, "Frank Miller", None, 3),  # Finance manager
        (7, "Grace Lee", 6, 3),  # Finance employee
        (8, "Henry Taylor", 1, 1),  # Tech employee
        (9, "Ivy Chen", None, 4),  # HR manager
        (10, "Jack Robinson", 9, 4),  # HR employee
    ]

    cursor.executemany(
        "INSERT INTO employees (employee_id, name, manager_id, department_id) VALUES (?, ?, ?, ?)",
        employees_data,
    )

    # Insert project data
    projects_data = [
        (1, "AI Development", 2, 1),  # Bob in Technology
        (2, "Web Platform", 3, 1),  # Carol in Technology
        (3, "Mobile App", 8, 1),  # Henry in Technology
        (4, "Brand Campaign", 5, 2),  # Emma in Marketing
        (5, "Budget Analysis", 7, 3),  # Grace in Finance
        (6, "Cloud Migration", 2, 1),  # Bob in Technology (multiple projects)
        (7, "Social Media", 5, 2),  # Emma in Marketing
        (8, "Recruitment Tool", 10, 4),  # Jack in HR
    ]

    cursor.executemany(
        "INSERT INTO projects (project_id, project_name, employee_id, department_id) VALUES (?, ?, ?, ?)",
        projects_data,
    )

    # Commit changes and close connection
    conn.commit()
    conn.close()

    print(f"Database created successfully at: {db_path.absolute()}")
    print("\nTables created:")
    print("- departments: 4 records")
    print("- employees: 10 records")
    print("- projects: 8 records")

    return db_path


def preview_tables(db_path):
    """Preview the created tables"""
    conn = sqlite3.connect(db_path)

    print("\n" + "=" * 50)
    print("TABLE PREVIEWS")
    print("=" * 50)

    # Preview departments
    print("\nDEPARTMENTS:")
    df = pd.read_sql_query("SELECT * FROM departments", conn)
    print(df.to_string(index=False))

    # Preview employees
    print("\nEMPLOYEES:")
    df = pd.read_sql_query("SELECT * FROM employees", conn)
    print(df.to_string(index=False))

    # Preview projects
    # print("\nPROJECTS:")
    # df = pd.read_sql_query("SELECT * FROM projects", conn)
    # print(df.to_string(index=False))

    conn.close()
