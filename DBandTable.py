import psycopg2
from psycopg2 import sql

# Function to create a database if it doesn't exist
def create_database(dbname):
    connection = psycopg2.connect(
        dbname="postgres",  # Connecting to the default "postgres" database
        user="postgres", 
        password="password", 
        host="localhost", 
        port="5432"
    )
    connection.autocommit = True  # Allow CREATE DATABASE to work outside of a transaction block
    cursor = connection.cursor()

    # Use parameterized queries to prevent SQL injection
    cursor.execute(sql.SQL("SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s"), [dbname])
    exists = cursor.fetchone()

    if not exists:
        try:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
            print(f"Database '{dbname}' created successfully!")
        except psycopg2.Error as e:
            print(f"Error creating database: {e}")
    else:
        print(f"Database '{dbname}' already exists!")

    cursor.close()
    connection.close()

# Function to create a table in the given database
def create_table():
    # Connect to the created database 'MSdb'
    connection = psycopg2.connect(
        dbname="MSdb",  # Now connecting to the created MSdb database
        user="postgres", 
        password="password", 
        host="localhost", 
        port="5432"
    )
    cursor = connection.cursor()

    # Create the table in the 'MSdb' database if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,       -- Sequential ID
            sender VARCHAR(100),         -- Sender name
            location VARCHAR(100),       -- Sender location
            message VARCHAR(500),        -- Message content
            time TIMESTAMP               -- Timestamp
        );
    """)
    
    connection.commit()
    print("Table 'messages' created successfully in 'MSdb' database!")

    cursor.close()
    connection.close()

# Main function to execute the creation steps
def main():
    dbname = "MSdb"
    
    # Step 1: Create the database if it doesn't exist
    create_database(dbname)
    
    # Step 2: Create the table in the MSdb database
    create_table()

if __name__ == "__main__":
    main()
