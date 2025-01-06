import os
import re
import duckdb
import json
import time
import tempfile
import hashlib
import base64

from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
from flask_cors import CORS
from cachetools import LRUCache

# Force Self-Service for UI
os.environ['VITE_SELFSERVICE'] = 'true'
# Default path for temp databases
dbpath = os.getenv('DBPATH', '/tmp/')
# Check for custom UI path
custom_ui_path = os.getenv('CUSTOM_UI_PATH')

if custom_ui_path:
    app = Flask(__name__, static_folder=custom_ui_path, static_url_path="")
else:
    app = Flask(__name__, static_folder="public", static_url_path="")

auth = HTTPBasicAuth()
CORS(app)

# Initialize LRU Cache
cache = LRUCache(maxsize=10)

# Global connection
conn = duckdb.connect(':memory:')
conn.install_extension("chsql", repository="community")
conn.install_extension("chsql_native", repository="community")
conn.load_extension("chsql")
conn.load_extension("chsql_native")
# conn = None

@auth.verify_password
def verify(username, password):
    global conn
    if not (username and password):
        print('stateless session')
        # conn = duckdb.connect(':memory:')

    else:
        global path
        os.makedirs(path, exist_ok=True)
        user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
        db_file = os.path.join(dbpath, f"{user_pass_hash}.db")
        print(f'stateful session {db_file}')
        conn = duckdb.connect(db_file)
        con.load_extension("chsql")
        con.load_extension("chsql_native")
    return True

def convert_to_ndjson(result):
    columns = result.description
    data = result.fetchall()
    ndjson_lines = []
    for row in data:
        row_dict = {columns[i][0]: row[i] for i in range(len(columns))}
        ndjson_lines.append(json.dumps(row_dict))
    return '\n'.join(ndjson_lines).encode()

def convert_to_clickhouse_jsoncompact(result, query_time):
    columns = result.description
    data = result.fetchall()
    meta = [{"name": col[0], "type": col[1]} for col in columns]
    json_result = {
        "meta": meta,
        "data": data,
        "rows": len(data),
        "rows_before_limit_at_least": len(data),
        "statistics": {
            "elapsed": query_time,
            "rows_read": len(data),
            "bytes_read": sum(len(str(item)) for row in data for item in row)
        }
    }
    return json.dumps(json_result)

def convert_to_clickhouse_json(result, query_time):
    columns = result.description
    data = result.fetchall()
    meta = [{"name": col[0], "type": col[1]} for col in columns]
    data_list = []
    for row in data:
        row_dict = {columns[i][0]: row[i] for i in range(len(columns))}
        data_list.append(row_dict)
    json_result = {
        "meta": meta,
        "data": data_list,
        "rows": len(data),
        "statistics": {
            "elapsed": query_time,
            "rows_read": len(data),
            "bytes_read": sum(len(str(item)) for row in data for item in row)
        }
    }
    return json.dumps(json_result)

def convert_to_csv_tsv(result, delimiter=','):
    columns = result.description
    data = result.fetchall()
    lines = []
    header = delimiter.join([col[0] for col in columns])
    lines.append(header)
    for row in data:
        line = delimiter.join([str(item) for item in row])
        lines.append(line)
    return '\n'.join(lines).encode()

def handle_insert_query(query, format, data=None):
    table_name = query.split("INTO")[1].split()[0].strip()
    temp_file_name = None
    if format.lower() == 'jsoneachrow' and data is not None:
        temp_file_name = save_to_tempfile(data)
    if temp_file_name:
        try:
            ingest_query = f"COPY {table_name} FROM '{temp_file_name}' (FORMAT 'json')"
            conn.execute(ingest_query)
        except Exception as e:
            return b"", str(e).encode()
        finally:
            os.remove(temp_file_name)
    return b"Ok", b""

def save_to_tempfile(data):
    temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w+', encoding='utf-8')
    temp_file.write(data)
    temp_file.flush()
    temp_file.close()
    return temp_file.name

def duckdb_query_with_errmsg(query, format='JSONCompact', data=None, request_method="GET"):
    try:
        if request_method == "POST" and query.strip().lower().startswith('insert into') and data:
            return handle_insert_query(query, format, data)
        start_time = time.time()
        result = conn.execute(query)
        query_time = time.time() - start_time
        if format.lower() == 'jsoncompact':
            output = convert_to_clickhouse_jsoncompact(result, query_time)
        elif format.lower() == 'json':
            output = convert_to_clickhouse_json(result, query_time)
        elif format.lower() == 'jsoneachrow':
            output = convert_to_ndjson(result)
        elif format.lower() == 'tsv':
            output = convert_to_csv_tsv(result, delimiter='\t')
        elif format.lower() == 'csv':
            output = convert_to_csv_tsv(result, delimiter=',')
        else:
            output = result.fetchall()
        if isinstance(output, list):
            output = json.dumps(output).encode()
        return output, b""
    except Exception as e:
        return b"", str(e).encode()

def sanitize_query(query):
    pattern = re.compile(r"(?i)\s*FORMAT\s+(\w+)\s*")
    match = re.search(pattern, query)
    if match:
        format_value = match.group(1).lower()
        query = re.sub(pattern, ' ', query).strip()
        return query, format_value.lower()
    return query, None

@app.route('/', methods=["GET", "HEAD"])
@auth.login_required
def clickhouse():
    query = request.args.get('query', default="", type=str)
    format = request.args.get('default_format', default="JSONCompact", type=str)
    database = request.args.get('database', default="", type=str)
    query_id = request.args.get('query_id', default=None, type=str)
    data = None
    query, sanitized_format = sanitize_query(query)
    if sanitized_format:
        format = sanitized_format
    print(f"Received request: method={request.method}, query={query}, format={format}, database={database}")
    if query_id is not None and not query:
        if query_id in cache:
            return cache[query_id], 200
    if not query:
        return app.send_static_file('index.html')
    if request.method == "POST":
        data = request.get_data(as_text=True)
    if database:
        query = f"ATTACH '{database}' AS db; USE db; {query}"
    result, errmsg = duckdb_query_with_errmsg(query.strip(), format, data, request.method)
    if query_id and len(errmsg) == 0:
        cache[query_id] = result
    if len(errmsg) == 0:
        if request.method == "HEAD":
            response = app.response_class(status=200)
            response.headers['Content-Type'] = 'application/json'
            response.headers['Accept-Ranges'] = 'bytes'
            content_length = len(result) if isinstance(result, bytes) else len(result.decode())
            response.headers['Content-Length'] = content_length
            return response
        return result, 200
    if len(result) > 0:
        print("warning:", errmsg)
        return result, 200
    print("Error occurred:", errmsg)
    return errmsg, 400

@app.route('/', methods=["POST"])
@auth.login_required
def play():
    query = request.args.get('query', default=None, type=str)
    body = request.get_data() or None
    format = request.args.get('default_format', default="JSONCompact", type=str)
    database = request.args.get('database', default="", type=str)
    query_id = request.args.get('query_id', default=None, type=str)
    if query_id is not None and not query:
        if query_id in cache:
            return cache[query_id], 200
    if query is None:
        query = ""
    if body is not None:
        data = " ".join(body.decode('utf-8').strip().splitlines())
        query = f"{query} {data}"
    if not query:
        return "Error: no query parameter provided", 400
    if database:
        query = f"ATTACH '{database}' AS db; USE db; {query}"
    query, sanitized_format = sanitize_query(query)
    if sanitized_format:
        format = sanitized_format
    print("DEBUG POST", query, format)
    result, errmsg = duckdb_query_with_errmsg(query.strip(), format)
    if len(errmsg) == 0:
        return result, 200
    if len(result) > 0:
        print("warning:", errmsg)
        return result, 200
    return errmsg, 400

@app.route('/play', methods=["GET"])
def handle_play():
    return app.send_static_file('index.html')

@app.route('/ping', methods=["GET"])
def handle_ping():
    return "Ok", 200

@app.errorhandler(404)
def handle_404(e):
    return app.send_static_file('index.html')

host = os.getenv('HOST', '0.0.0.0')
port = os.getenv('PORT', 8123)
path = os.getenv('DATA', '.duckdb_data')

if __name__ == '__main__':
    app.run(host=host, port=port)
