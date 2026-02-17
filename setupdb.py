import os
from sqlalchemy import create_engine, text
from datetime import datetime
from dotenv import load_dotenv

# Load credentials from .env file in the same folder as this script
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))


def get_engine():
    """
    Reads database credentials from .env and returns a SQLAlchemy engine.

    MySQL requires a database to exist before you can connect to it by name.
    So we connect first WITHOUT specifying a database, create it if it doesn't
    exist, then reconnect WITH the database name selected.
    """
    DB_USER     = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    DB_HOST     = os.getenv('DB_HOST', 'localhost')
    DB_PORT     = os.getenv('DB_PORT', '3306')
    DB_NAME     = os.getenv('DB_NAME')

    if not all([DB_USER, DB_PASSWORD, DB_NAME]):
        raise ValueError(
            "Missing DB credentials in .env\n"
            "Need: DB_USER, DB_PASSWORD, DB_NAME"
        )

    # Step 1: Connect without a database name (just to the MySQL server itself)
    base_url    = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/'
    base_engine = create_engine(base_url)

    # Step 2: Create the database if it doesn't already exist
    with base_engine.connect() as conn:
        conn.execute(text(f"CREATE DATABASE IF NOT EXISTS `{DB_NAME}`"))
        print(f"  Database '{DB_NAME}' ready")

    base_engine.dispose()  # Close the no-database connection cleanly

    # Step 3: Reconnect with the database name selected
    url    = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    engine = create_engine(url)
    print(f"  Connected to: {DB_NAME}")
    return engine


def run_sql_file(engine, sql_path):
    """
    Reads a .sql file and executes each statement against the database.

    The file is split on semicolons to get individual statements, since
    SQLAlchemy executes one statement at a time rather than a whole script.
    Empty statements (e.g. from trailing semicolons) are skipped.
    """
    print(f"\n  Reading SQL file: {sql_path}")

    with open(sql_path, 'r') as f:
        sql = f.read()

    # Split the file into individual statements on semicolons
    statements = [s.strip() for s in sql.split(';') if s.strip()]
    print(f"  Found {len(statements)} SQL statements to execute\n")

    with engine.connect() as conn:
        for i, statement in enumerate(statements, start=1):
            try:
                conn.execute(text(statement))
                print(f"  [{i}/{len(statements)}] OK")
            except Exception as e:
                print(f"  [{i}/{len(statements)}] FAILED: {e}")
                raise  # Stop immediately if any statement fails
        conn.commit()

    print(f"\n  All statements executed successfully")


def main():
    print("\n" + "="*60)
    print("DATABASE SETUP")
    print("="*60)
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # Read the SQL file path from .env
    SQL_FILE = os.getenv('SQL_FILE')

    if not SQL_FILE:
        raise ValueError(
            "Missing SQL_FILE path in .env\n"
            "Add: SQL_FILE=path/to/your/schema.sql"
        )

    if not os.path.exists(SQL_FILE):
        raise FileNotFoundError(f"SQL file not found: {SQL_FILE}")

    try:
        engine = get_engine()
        run_sql_file(engine, SQL_FILE)

        print("\n" + "="*70)
        print(" DATABASE SETUP COMPLETE!")
        print("="*70)
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        print("You can now run data_loader.py to load your data.\n")

    except FileNotFoundError as e:
        print(f"\n FILE ERROR: {e}")
        print("  Check your SQL_FILE path in .env")

    except Exception as e:
        print(f"\n ERROR: {e}")
        raise


if __name__ == "__main__":
    main()