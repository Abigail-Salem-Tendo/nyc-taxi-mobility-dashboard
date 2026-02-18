from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

app = Flask(__name__)
CORS(app)

# Database configuration
def get_engine():
    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    db_host = os.getenv('DB_HOST', 'localhost')
    db_port = os.getenv('DB_PORT', '3306')
    db_name = os.getenv('DB_NAME')

    url = f'mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
    return create_engine(url)

engine = get_engine()



if __name__ == '__main__':
    app.run(debug=True, port=5000)