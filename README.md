# nyc-taxi-mobility-dashboard
Summative assignment to demonstrate our ability to design and develop an enterprise-level fullstack application using real-world urban mobility datasets.

## The system 
 . cleans raw trip data 
 . stores processed data in a database
 . uses API endpoints to featch data from the database 
 . supports front end dashboard for insights 



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



   * Install Dependencies
    `pip install -r requirements.txt`

2. Environment Configuration
   
    Create a .env file in the root directory and add your database credentials. This keeps your sensitive information secure.
    ```
   DB_HOST=localhost
    DB_PORT=3306
    DB_USER=your_username
    DB_PASSWORD=your_password
    DB_NAME=nyc_mobility_data

   ```
3. Run data cleaning 
  This processes the raw CSV file and removes outliers
  . python3 clean_data.py

4. run setup db
   Creates the Star Schema and Performance Indexes
   . python3 setupdb.py

5. load database 
   Loads the cleaned data files into MySQL.

   . python3 data_loader.py

6. start the backend 

   . python3 app.py

7. Launch Frontend
  
  Launch index.html in your server


### Key Insights 
the dashboard enables analysis of 
. peak hours vs efficiency 
. Short trip and their congestion effects
. Tipping as a quality signal
. Passenger load hotspots




#### Link to the task sheet:  
https://docs.google.com/spreadsheets/d/1SR6UiafecmBD-tgKm2JTfJJTtf6Z6PHrfZjww6iaPk8/edit?usp=sharing

#### link to the video walk through: 
https://drive.google.com/file/d/1nWrEJydg1odyLnPim86KvvHP9jOYr73Q/view?usp=sharing

