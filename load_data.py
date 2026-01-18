import os
import glob
import pandas as pd
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from io import StringIO

load_dotenv()

def import_csv():
    database_url = os.getenv('database_url')
    conn = psycopg2.connect(database_url)
    csv_files = glob.glob('*.csv')

    for file_path in csv_files:
        filename = os.path.basename(file_path)
        table_name = os.path.splitext(filename)[0]
        try:
            cur = conn.cursor()
            df = pd.read_csv(file_path)
            create_columns = []
            for col in df.columns:
                dtype = str(df[col].dtype)
                if 'int' in dtype:
                    pg_type = 'INTEGER'
                elif 'float' in dtype:
                    pg_type = 'DECIMAL'
                elif 'date' in dtype:
                    pg_type = 'DATE'
                else:
                    pg_type = 'VARCHAR(255)'

                create_columns.append(f"{col} {pg_type}")

                cur.execute(sql.SQL("DROP TABLE IF EXISTS {}").format(sql.Identifier(table_name)))

                create_sql = sql.SQL("CREATE TABLE {} (id SERIAL PRIMARY KEY, {})").format(
                sql.Identifier(table_name), sql.SQL(",").join([sql.SQL(col) for col in create_columns]))

            cur.execute(create_sql)
            conn.commit()

            buffer = StringIO()
            df.to_csv(buffer, index = False, header= False)
            buffer.seek(0)
            
            cur.copy_from(buffer, table_name, sep = ',', null='', columns=df.columns.tolist())
            conn.commit()

            cur.close()


        except Exception as e:
            print(f"Error {e}")
    
    conn.close()

if __name__ == "__main__":
    import_csv()