from flask import Flask, render_template, jsonify
from flask_cors import CORS
import sqlite3
import pandas as pd

app = Flask(__name__)
CORS(app)

DB = 'database.db'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/data')
def get_data():
    conn = sqlite3.connect(DB)
    df = pd.read_sql_query('SELECT * FROM prices', conn)
    conn.close()
    data = df.to_dict(orient='records')
    return jsonify(data)

@app.route('/alerts')
def get_alerts():
    conn = sqlite3.connect(DB)
    df = pd.read_sql_query('SELECT * FROM alerts ORDER BY timestamp DESC LIMIT 20', conn)
    conn.close()
    data = df.to_dict(orient='records')
    return jsonify(data)

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
