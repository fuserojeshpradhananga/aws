import psycopg2

# Modify these parameters with your database details
db_params = {
    "host": "apprentice-training-2023-rds.cth7tqaptja4.us-west-1.rds.amazonaws.com",
    "database": "postgres",
    "user": "postgres",
    "password": "hello123"
}

conn = psycopg2.connect(**db_params)

cursor = conn.cursor()


create_table_query = """
CREATE TABLE IF NOT EXISTS rojesh_etl_assignment (
    id VARCHAR,
    name VARCHAR,
    description TEXT,
    url VARCHAR,
    category VARCHAR,
    language VARCHAR,
    country VARCHAR
);
"""

conn = psycopg2.connect(**db_params)
cursor = conn.cursor()
cursor.execute(create_table_query)

# Commit changes and close connections
conn.commit()
cursor.close()
conn.close()