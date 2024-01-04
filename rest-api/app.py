from flask import Flask, request, jsonify
import pymysql
import time

app = Flask(__name__)

MAX_RETRIES = 3
RETRY_DELAY = 1  # in seconds


def execute_with_retry(cursor, query, params):
    retries = 0
    while retries < MAX_RETRIES:
        try:
            cursor.execute(query, params)
            return
        except pymysql.err.OperationalError as e:
            if e.args[0] == 1205:  # Lock wait timeout
                retries += 1
                time.sleep(RETRY_DELAY)
            else:
                raise
    raise Exception("Max retries reached, transaction failed.")


def db_connection():
    conn = None
    try:
        conn = pymysql.connect(
            host="sql12.freesqldatabase.com",
            database="sql12674767",
            user="sql12674767",
            password="SiGcGNw3G8",
            charset='utf8mb4',
            autocommit=True,  # Autocommit mode
            # isolation_level='READ COMMITTED',  # Set isolation level
            # cursorclass=pymysql.cursors.Dictcursor
        )
    except pymysql.Error as e:
        print(e)
    return conn


@app.route('/books', methods=['GET', 'POST'])
def books():
    # conn = db_connection()
    with db_connection() as conn:
        if conn is None:
            print("Connection to the database failed.")
            return 'Internal Server Error', 500

        cursor = conn.cursor()

        if request.method == 'GET':
            # cursor.execute("SELECT * from book")  # come here
            execute_with_retry(cursor, "SELECT * FROM book", ())

            books = [
                dict(id=row[0], author=row[1],
                     language=row[2], title=row[3])
                for row in cursor.fetchall()
            ]

            if books is not None:
                return jsonify(books)
            return 'Nothing Found', 404

        if request.method == 'POST':
            new_author = request.form['author']
            new_lang = request.form['language']
            new_title = request.form['title']

            execute_with_retry(cursor, "insert into book(author, language, title) values(%s,%s,%s)",
                               (new_author, new_lang, new_title))
            conn.commit()
            return f"Book with the id: {cursor.lastrowid} created successfully!"


@app.route('/book/<int:id>', methods=['GET', 'PUT', 'DELETE'])
def single_book(id):
    with db_connection() as conn:
        if conn is None:
            print("Connection to the database failed.")
            return 'Internal Server Error', 500

        cursor = conn.cursor()
        book = None

        if request.method == "GET":
            execute_with_retry(cursor, "select * from book where id=%s", (id,))
            rows = cursor.fetchall()
            if rows:
                book = rows[0]
            if book is not None:
                return jsonify(book), 200
            return 'Nothing Found', 404

        if request.method == 'PUT':
            new_author = request.form['author']
            new_lang = request.form['language']
            new_title = request.form['title']
            execute_with_retry(cursor, "update book set author=%s, title=%s, language=%s where id=%s",
                               (new_author, new_title, new_lang, id))
            conn.commit()

            if cursor.rowcount > 0:
                return f"Book with id {id} updated successfully!", 200

            return 'Nothing Found', 404

        if request.method == 'DELETE':
            execute_with_retry(cursor, "delete from book where id=%s", (id,))
            conn.commit()
            return "sucess!"


if __name__ == '__main__':
    app.run(debug=True)
