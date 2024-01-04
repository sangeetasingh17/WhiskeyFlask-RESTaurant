# import sqlite3
import pymysql

conn = pymysql.connect(
    host="sql12.freesqldatabase.com",
    database="sql12674767",
    user="sql12674767",
    password="SiGcGNw3G8",
    charset='utf8mb4',
    autocommit=True,  # Autocommit mode
    # isolation_level='READ COMMITTED',  # Set isolation level
)

cursor = conn.cursor()

sql_query = ''' create table if not exists book(
    id INT AUTO_INCREMENT primary key,
    author text NOT NULL,
    language text NOT NULL,
    title text NOT NULL
)
'''

cursor.execute(sql_query)
conn.close()

# cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
# print(cursor.fetchall())
