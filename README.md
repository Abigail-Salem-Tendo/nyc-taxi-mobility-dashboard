# nyc-taxi-mobility-dashboard
Summative assignment to demonstrate our ability to design and develop an enterprise-level fullstack application using real-world urban mobility datasets.

## Tech Stack
- Database: MySQL (Star Schema)

- Backend: Python / Flask / SQLAlchemy

- Data Processing: Pandas (Chunked ETL)

- Visualization:

## How to run

1. Prerequisites

    Ensure you have the following
   * Python 3.8+
   * MySQL 
   * A virtual environment (recommended)

2. Environment Configuration
   
    Create a .env file in the root directory and add your database credentials. This keeps your sensitive information secure.
    ```
   DB_HOST=localhost
    DB_PORT=3306
    DB_USER=your_username
    DB_PASSWORD=your_password
    DB_NAME=nyc_mobility_data
   ```

3. Install Dependencies
    
`pip install -r requirements.txt`

## Execution Pipeline

### Data Cleaning

This processes the raw CSV file and removes outliers

    python3 clean_data.py

### Database Initialization
Creates the Star Schema and Performance Indexes

    python3 setupdb.py

### Data Loading
Loads the cleaned data files into MySQL.

    python3 data_loader.py

### Launch Backend

    python3 app.py

### Launch Frontend
Launch index.html in your server


