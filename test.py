import os
import glob
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()


database_url = os.getenv('database_url')
conn = psycopg2.connect(database_url)
csv_files = glob.glob('*.csv')
print(csv_files)