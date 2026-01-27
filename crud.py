import ibm_db

# Database Name: testdb1
# User testuser1
# Passwork password123
# HostName db2i-amitparmar-ypm0c-x86.fyre.ibm.com
# Port - 50000
# ---------------- DB2 CONNECTION ----------------

def get_connection():
    conn_str = (
        "DATABASE=testdb1;""HOSTNAME=db2i-amitparmar-ypm0c-x86.fyre.ibm.com;""PORT=50000;"
        "PROTOCOL=TCPIP;"
        "UID=testuser1;"
        "PWD=password123;"
    )

    print("Connecting to database...")
    conn = ibm_db.connect(conn_str, "", "")
    print("Connected successfully!")
    print(conn)
    return conn

# ---------------- CREATE ----------------
def add_employee(name, email, dept, salary):
    conn = get_connection()
    sql = "INSERT INTO EMPLOYEE (EMP_NAME, EMAIL, DEPARTMENT, SALARY) VALUES (?, ?, ?, ?)"
    stmt = ibm_db.prepare(conn, sql)
    ibm_db.bind_param(stmt, 1, name)
    ibm_db.bind_param(stmt, 2, email)
    ibm_db.bind_param(stmt, 3, dept)
    ibm_db.bind_param(stmt, 4, salary)
    ibm_db.execute(stmt)
    ibm_db.close(conn)
    print("Employee added")

# ---------------- READ ----------------
def view_employees():
    conn = get_connection()
    sql = "SELECT * FROM EMPLOYEE"
    stmt = ibm_db.exec_immediate(conn, sql)

    print("\nEMPLOYEE TABLE:")
    row = ibm_db.fetch_assoc(stmt)
    print('hola amigo')
    while row:
        print(row)
        row = ibm_db.fetch_assoc(stmt)
    
    ibm_db.close(conn)

# ---------------- UPDATE ----------------
def update_salary(emp_id, salary):
    conn = get_connection()
    sql = "UPDATE EMPLOYEE SET SALARY = ? WHERE EMP_ID = ?"
    stmt = ibm_db.prepare(conn, sql)
    ibm_db.bind_param(stmt, 1, salary)
    ibm_db.bind_param(stmt, 2, emp_id)
    ibm_db.execute(stmt)
    ibm_db.close(conn)
    print("Salary updated")

# ---------------- DELETE ----------------
def delete_employee(emp_id):
    conn = get_connection()
    sql = """DELETE 
           FROM EMPLOYEE 
           WHERE EMP_ID = ?"""
    stmt = ibm_db.prepare(conn, sql)
    ibm_db.bind_param(stmt, 1, emp_id)
    ibm_db.execute(stmt)
    ibm_db.close(conn)
    print("Employee deleted")

def create_employee_table():
    conn = get_connection()
    sql = """
    CREATE TABLE EMPLOYEE (
        EMP_ID INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
        EMP_NAME VARCHAR(100),
        EMAIL VARCHAR(100),
        DEPARTMENT VARCHAR(50),
        SALARY DECIMAL(10, 2)
    )
    """
    try:
        ibm_db.exec_immediate(conn, sql)
        print("EMPLOYEE table created successfully.")
    except Exception as e:
        print("Error creating EMPLOYEE table:", e)
    finally:
        ibm_db.close(conn)

def drop_employee_table():
    conn = get_connection()
    sql = "DROP TABLE EMPLOYEE"
    try:
        ibm_db.exec_immediate(conn, sql)
        print("EMPLOYEE table dropped successfully.")
    except Exception as e:
        print("Error dropping EMPLOYEE table:", e)
    finally:
        ibm_db.close(conn)


# Tables
def find_tables(conn):
    sql = """
    SELECT TABSCHEMA, TABNAME
    FROM SYSCAT.TABLES
    WHERE TYPE = 'T'
    """

    stmt = ibm_db.exec_immediate(conn, sql)

    tables = []
    while ibm_db.fetch_row(stmt):
        tables.append({
            "schema": ibm_db.result(stmt, 0),
            "table": ibm_db.result(stmt, 1)
        })

    return tables

def view_schemas():
    conn = get_connection()

    sql = "SELECT schemaname FROM syscat.schemata"
    stmt = ibm_db.exec_immediate(conn, sql)

    print("\nSchemas in the database:")

    row = ibm_db.fetch_assoc(stmt)
    while row:
        print(row['SCHEMANAME'])
        row = ibm_db.fetch_assoc(stmt)

    ibm_db.close(conn)

# ---------------- MAIN ----------------
if __name__ == "__main__":
#    create_employee_table()

    print("---- Viewing Schemas ----")
    conn = get_connection()
    view_schemas()
    # sql = "SELECT CURRENT DATE FROM SYSIBM.SYSDUMMY1"
    # stmt = ibm_db.exec_immediate(conn, sql)
    # row = ibm_db.fetch_assoc(stmt)
    # print(row)



    # tables = find_tables(conn)
    # print("Tables in the database:")
    # for table in tables:
    #     print(f"{table['schema']}.{table['table']}")
    # ibm_db.close(conn)


    # add_employee("Alice Johnson", "alice@gmail.com", "HR", 60000)
    # add_employee("Bob Smith", "booob@ibm.com", "IT", 75000)
    # view_employees()

    # update_salary(1, 65000)
    # view_employees()



    # drop_employee_table()
    # conn = get_connection()
    # tables = find_tables(conn)
    # print("Tables in the database:")
    # for table in tables:
    #     print(f"{table['schema']}.{table['table']}")
    ibm_db.close(conn)





