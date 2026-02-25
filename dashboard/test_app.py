import sys
import os

# Add project root to Python path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import streamlit as st
import pandas as pd
import mysql.connector
from config.db_config import DB_CONFIG

st.title("Database Connection Test")

conn = mysql.connector.connect(**DB_CONFIG)
df = pd.read_sql("SELECT * FROM clinical_trials LIMIT 10", conn)
conn.close()

st.write(df)