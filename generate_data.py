import psycopg2
from faker import Faker
import random
from datetime import date

def generate_data():
    fake = Faker()
    
    conn_params = {
        "host": "localhost",
        "port": 5434,
        "database": "postgres", 
        "user": "admin",
        "password": "password" 
    }

    passwords = ["password", "postgres", "admin", "root", ""]
    conn = None
    
    for pwd in passwords:
        try:
            try:
                conn = psycopg2.connect(**{**conn_params, "database": "bigdata", "password": pwd})
                print(f"Connected to database: bigdata with password: {pwd}")
                break
            except:
                conn = psycopg2.connect(**{**conn_params, "password": pwd})
                print(f"Connected to database: postgres with password: {pwd}")
                break
        except Exception as e:
            continue
            
    if not conn:
        print("Failed to connect to PostgreSQL with common passwords.")
        return
            
    try:
        cur = conn.cursor()
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS teste (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255),
                year DATE
            );
        """)
        
        cur.execute("TRUNCATE TABLE teste;")
        
        print("Generating 4000 rows of synthetic data...")
        
        data = []
        for i in range(1, 4001):
            name = fake.name()
            year_val = fake.date_between(start_date='-20y', end_date='today')
            data.append((i, name, year_val))
            
            if i % 1000 == 0:
                print(f"Prepared {i} rows...")

        args_str = ','.join(cur.mogrify("(%s,%s,%s)", x).decode('utf-8') for x in data)
        cur.execute("INSERT INTO teste (id, name, year) VALUES " + args_str)
        
        conn.commit()
        print("Successfully inserted 4000 rows into table 'teste'.")
        
    except Exception as e:
        print(f"Error during database operations: {e}")
        if conn:
            conn.rollback()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == "__main__":
    generate_data()
