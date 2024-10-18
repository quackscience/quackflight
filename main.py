import os
import duckdb
import json
import time
from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth

app = Flask(__name__, static_folder="public", static_url_path="")
auth = HTTPBasicAuth()

# Global connection
conn = None

@auth.verify_password
def verify(username, password):
    global conn
    if not (username and password):
        print('stateless session')
        conn = duckdb.connect(':memory:')
    else:
        path = f"{globals()['path']}/{hash(username + password)}.db"
        print(f'stateful session {path}')
        conn = duckdb.connect(path)
    return True

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

def duckdb_query_with_errmsg(query, format):
    try:
        start_time = time.time()
        result = conn.execute(query)
        query_time = time.time() - start_time
        
        if format.lower() == 'jsoncompact':
            output = convert_to_clickhouse_jsoncompact(result, query_time)
        elif format.lower() == 'jsoneachrow':
            rows = result.fetchall()
            output = '\n'.join(json.dumps(row) for row in rows).encode()
        elif format.lower() == 'tsv':
            output = result.df().to_csv(sep='\t', index=False)
        else:
            output = result.fetchall()
        
        # Ensure output is in bytes before returning
        if isinstance(output, list):
            output = json.dumps(output).encode()  # Convert list to JSON string and then encode
        
        return output, b""
    except Exception as e:
        return b"", str(e).encode()

@app.route('/', methods=["GET", "HEAD"])
@auth.login_required
def clickhouse():
    query = request.args.get('query', default="", type=str)
    format = request.args.get('default_format', default="JSONCompact", type=str)
    database = request.args.get('database', default="", type=str)

    # Log incoming request data for debugging
    print(f"Received request: method={request.method}, query={query}, format={format}, database={database}")

    if not query:
        return app.send_static_file('play.html')

    if database:
        query = f"ATTACH '{database}' AS db; USE db; {query}"

    # Execute the query and capture the result and error message
    result, errmsg = duckdb_query_with_errmsg(query.strip(), format)

    # Handle response for HEAD requests
    if len(errmsg) == 0:
        if request.method == "HEAD":
            response = app.response_class(status=200)
            response.headers['Content-Type'] = 'application/json'
            response.headers['Accept-Ranges'] = 'bytes'  # Indicate that range requests are supported
            
            # Set Content-Length for HEAD request
            content_length = len(result) if isinstance(result, bytes) else len(result.decode())
            response.headers['Content-Length'] = content_length
            
            return response
        
        return result, 200

    # Log any warnings or errors
    if len(result) > 0:
        print("warning:", errmsg)
        return result, 200

    print("Error occurred:", errmsg)  # Log the actual error message for debugging
    return errmsg, 400

@app.route('/', methods=["POST"])
@auth.login_required
def play():
    query = request.args.get('query', default=None, type=str)
    body = request.get_data() or None
    format = request.args.get('default_format', default="JSONCompact", type=str)
    database = request.args.get('database', default="", type=str)
    
    if query is None:
        query = ""
    
    if body is not None:
        data = " ".join(body.decode('utf-8').strip().splitlines())
        query = f"{query} {data}"
    
    if not query:
        return "Error: no query parameter provided", 400
    
    if database:
        query = f"ATTACH '{database}' AS db; USE db; {query}"
    
    result, errmsg = duckdb_query_with_errmsg(query.strip(), format)
    if len(errmsg) == 0:
        return result, 200
    if len(result) > 0:
        print("warning:", errmsg)
        return result, 200
    return errmsg, 400

@app.route('/play', methods=["GET"])
def handle_play():
    return app.send_static_file('play.html')

@app.route('/ping', methods=["GET"])
def handle_ping():
    return "Ok", 200

@app.errorhandler(404)
def handle_404(e):
    return app.send_static_file('play.html')

host = os.getenv('HOST', '0.0.0.0')
port = os.getenv('PORT', 8123)
path = os.getenv('DATA', '.duckdb_data')

if __name__ == '__main__':
    app.run(host=host, port=port)

