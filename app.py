from flask import Flask, render_template, request, jsonify
import psycopg2
import os

app = Flask(__name__)

# Get DATABASE_URL from environment in production
DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://csc6710:y6HKYcwrYdH3XBfr6m6Wjmkzlh1lG0Kr@dpg-d017lgvgi27c73a2k0i0-a.virginia-postgres.render.com/librarydb_ga0x")

def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn

@app.route('/')
def index():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT title, author, branch, status FROM book LIMIT 10;')
    books = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify({
        "message": "Library Database Connected Successfully",
        "sample_books": [{"title": book[0], "author": book[1], "branch": book[2], "status": book[3]} for book in books]
    })

@app.route('/branches')
def branches():
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute('SELECT branchID, location FROM libraryBranch;')
    branches = cur.fetchall()
    cur.close()
    conn.close()
    return jsonify({
        "branches": [{"id": branch[0], "location": branch[1]} for branch in branches]
    })

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))