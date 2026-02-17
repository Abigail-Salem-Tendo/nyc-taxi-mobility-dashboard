from flask import Flask, jsonify, request
from flask_cors import CORS
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

app = Flask(__name__)
CORS(app)