import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()




db_url = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@localhost:5432/{os.getenv('DB_NAME')}"


def create_database_engine(db_url=None):
    try:
        if db_url.startswith('jdbc:'):
            db_url = db_url.replace('jdbc:', '')
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        print(f"Error creating database engine: {e}")
        return None
    
engine = create_database_engine(db_url)
if engine is None:
    print("Database engine not created. Exiting.")
    
engine.connect()