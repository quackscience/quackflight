import os
import re
import duckdb
import json
import time
import tempfile
import hashlib
import base64
import threading
import msgpack
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from flask import Flask, request, jsonify, make_response
from flask_httpauth import HTTPBasicAuth
from flask_cors import CORS
from cachetools import LRUCache
import pyarrow as pa
import pyarrow.flight as flight
import zstandard as zstd

import signal
import threading
import logging
import sys
from threading import Lock
from queue import Queue  # Correct import for Queue


# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(threadName)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('server.log')
    ]
)
logger = logging.getLogger('server')
# Global flag for server state
running = True


def signal_handler(signum, frame):
    """Handle shutdown signals"""
    global running
    logger.info("Shutdown signal received")
    running = False


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


# Connection Pool Implementation
class ConnectionPool:
    def __init__(self, db_file, max_connections=5):
        self.db_file = db_file
        self.max_connections = max_connections
        self._pool = Queue(maxsize=max_connections)
        self._lock = Lock()
        for _ in range(max_connections):
            self._pool.put(self._create_connection())

    def _create_connection(self):
        conn = duckdb.connect(self.db_file)
        self._setup_extensions(conn)
        return conn

    def _setup_extensions(self, conn: duckdb.DuckDBPyConnection):
        """Set up required extensions for a connection"""
        try:
            conn.install_extension("chsql", repository="community")
            conn.install_extension("chsql_native", repository="community")
            conn.load_extension("chsql")
            conn.load_extension("chsql_native")
        except Exception as e:
            logger.warning(f"Failed to initialize extensions: {e}")

    def get_connection(self):
        """Acquire a connection from the pool."""
        with self._lock:
            return self._pool.get()

    def return_connection(self, conn):
        """Return a connection to the pool."""
        with self._lock:
            self._pool.put(conn)

    def close_all_connections(self):
        """Close all connections in the pool."""
        while not self._pool.empty():
            conn = self._pool.get()
            conn.close()


class ConnectionManager:
    def __init__(self):
        self._connection_pools: Dict[str, ConnectionPool] = {}
        self._lock = Lock()
        self._default_pool = ConnectionPool(':memory:')

    def get_connection(self, auth_hash: Optional[str] = None) -> duckdb.DuckDBPyConnection:
        """Get or create a connection for the given auth hash"""
        if not auth_hash:
            return self._default_pool.get_connection()

        with self._lock:
            if auth_hash not in self._connection_pools:
                db_file = os.path.join(dbpath, f"{auth_hash}.db")
                logger.info(f'Creating new connection pool for {db_file}')
                pool = ConnectionPool(db_file)
                self._connection_pools[auth_hash] = pool
            return self._connection_pools[auth_hash].get_connection()

    def return_connection(self, conn: duckdb.DuckDBPyConnection, auth_hash: Optional[str] = None):
        """Return a connection to the pool"""
        with self._lock:
            if not auth_hash:
                self._default_pool.return_connection(conn)
            elif auth_hash in self._connection_pools:
                self._connection_pools[auth_hash].return_connection(conn)
            else:
                logger.warning(f'Pool not found for {auth_hash}, closing connection')
                conn.close()


# Create global connection manager
connection_manager = ConnectionManager()


def get_current_connection() -> duckdb.DuckDBPyConnection:
    """Get the current connection based on authentication"""
    auth_obj = request.authorization if hasattr(request, 'authorization') else None
    if auth_obj and auth_obj.username and auth_obj.password:
        user_pass_hash = hashlib.sha256((auth_obj.username + auth_obj.password).encode()).hexdigest()
        return connection_manager.get_connection(user_pass_hash)
    return connection_manager.get_connection()


@property
def conn():
    return get_current_connection()


@auth.verify_password
def verify(username, password):
    if not (username and password):
        logger.debug('Using stateless session')
        return True

    logger.info(f"Using http auth: {username}:{password}")
    user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
    connection_manager.get_connection(user_pass_hash)
    return True


def convert_to_ndjson(result):
    columns = result.description
    data = result.fetchall()
    ndjson_lines = []
    for row in data:
        row_dict = {columns[i][0]: row[i] for i in range(len(columns))}
        ndjson_lines.append(json.dumps(row_dict, ensure_ascii=False))
    return '\n'.join(ndjson_lines).encode('utf-8')


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
    return json.dumps(json_result, ensure_ascii=False)


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
    return json.dumps(json_result, ensure_ascii=False)


def convert_to_csv_tsv(result, delimiter=','):
    columns = result.description
    data = result.fetchall()
    lines = []
    header = delimiter.join([col[0] for col in columns])
    lines.append(header)
    for row in data:
        line = delimiter.join([str(item) for item in row])
        lines.append(line)
    return '\n'.join(lines).encode('utf-8')


def handle_insert_query(query, format, data=None, current_conn=None):
    if current_conn is None:
        current_conn = get_current_connection()
    table_name = query.split("INTO")[1].split()[0].strip()
    temp_file_name = None
    if format.lower() == 'jsoneachrow' and data is not None:
        temp_file_name = save_to_tempfile(data)
    if temp_file_name:
        try:
            ingest_query = f"COPY {table_name} FROM '{temp_file_name}' (FORMAT 'json')"
            current_conn.execute(ingest_query)
        except Exception as e:
            return b"", str(e).encode('utf-8')
        finally:
            os.remove(temp_file_name)
    return b"Ok", b""


def save_to_tempfile(data):
    temp_file = tempfile.NamedTemporaryFile(
        delete=False, mode='w+', encoding='utf-8')
    temp_file.write(data)
    temp_file.flush()
    temp_file.close()
    return temp_file.name


def duckdb_query_with_errmsg(query, format='JSONCompact', data=None, request_method="GET"):
    current_conn = None
    try:
        current_conn = get_current_connection()

        if request_method == "POST" and query.strip().lower().startswith('insert into') and data:
            return handle_insert_query(query, format, data, current_conn)
        start_time = time.time()
        result = current_conn.execute(query)
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
            output = json.dumps(output, ensure_ascii=False).encode('utf-8')
        return output, b""
    except Exception as e:
        logger.error(f"Query execution error: {e}", exc_info=True)
        return b"", str(e).encode('utf-8')
    finally:
        if current_conn:
            auth_obj = request.authorization if hasattr(request, 'authorization') else None
            user_pass_hash = hashlib.sha256((auth_obj.username + auth_obj.password).encode()).hexdigest() if auth_obj and auth_obj.username and auth_obj.password else None
            connection_manager.return_connection(current_conn, user_pass_hash)


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
    format = request.args.get(
        'default_format', default="JSONCompact", type=str)
    database = request.args.get('database', default="", type=str)
    query_id = request.args.get('query_id', default=None, type=str)
    data = None
    query, sanitized_format = sanitize_query(query)
    if sanitized_format:
        format = sanitized_format
    logger.debug(
        f"Received request: method={request.method}, query={query}, format={format}, database={database}")
    if query_id is not None and not query:
        if query_id in cache:
            cached_response = cache[query_id]
            logger.debug(f"Cache hit for query_id: {query_id}")
            return cached_response, 200
    if not query:
        return app.send_static_file('index.html')
    if request.method == "POST":
        data = request.get_data(as_text=True)
    if database:
        query = f"ATTACH '{database}' AS db; USE db; {query}"
    result, errmsg = duckdb_query_with_errmsg(
        query.strip(), format, data, request.method)
    if query_id and len(errmsg) == 0:
        cache[query_id] = result
        logger.debug(f"Cache set for query_id: {query_id}")
    if len(errmsg) == 0:
        if request.method == "HEAD":
            response = app.response_class(status=200)
            response.headers['Content-Type'] = 'application/json'
            response.headers['Accept-Ranges'] = 'bytes'
            content_length = len(result) if isinstance(
                result, bytes) else len(result.decode('utf-8'))
            response.headers['Content-Length'] = content_length
            return response
        response = make_response(result, 200)
        content_type = 'application/json'
        if format.lower() == 'csv':
            content_type = 'text/csv'
        elif format.lower() == 'tsv':
            content_type = 'text/tab-separated-values'
        elif format.lower() == 'jsoneachrow' or format.lower() == 'json' or format.lower() == 'jsoncompact':
            content_type = 'application/json'
        response.headers['Content-Type'] = content_type
        return response
    if len(errmsg) > 0:
        logger.warning(f"Query warning: {errmsg}")
        response = make_response(result, 200)
        response.headers['Content-Type'] = 'application/json'
        return response
    logger.error(f"Query error: {errmsg}")
    return errmsg.decode('utf-8'), 400


@app.route('/', methods=["POST"])
@auth.login_required
def play():
    query = request.args.get('query', default=None, type=str)
    body = request.get_data() or None
    format = request.args.get(
        'default_format', default="JSONCompact", type=str)
    database = request.args.get('database', default="", type=str)
    query_id = request.args.get('query_id', default=None, type=str)
    if query_id is not None and not query:
        if query_id in cache:
            cached_response = cache[query_id]
            logger.debug(f"Cache hit for query_id: {query_id}")
            return cached_response, 200
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
    logger.debug(f"DEBUG POST query: {query}, format: {format}")
    result, errmsg = duckdb_query_with_errmsg(query.strip(), format)
    if len(errmsg) == 0:
        response = make_response(result, 200)
        content_type = 'application/json'
        if format.lower() == 'csv':
            content_type = 'text/csv'
        elif format.lower() == 'tsv':
            content_type = 'text/tab-separated-values'
        elif format.lower() == 'jsoneachrow' or format.lower() == 'json' or format.lower() == 'jsoncompact':
            content_type = 'application/json'
        response.headers['Content-Type'] = content_type
        return response
    if len(errmsg) > 0:
        logger.warning(f"Query warning: {errmsg}")
        response = make_response(result, 200)
        response.headers['Content-Type'] = 'application/json'
        return response
    logger.error(f"Query error: {errmsg}")
    return errmsg.decode('utf-8'), 400


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
port = int(os.getenv('PORT', 8123))
flight_host = os.getenv('FLIGHT_HOST', 'localhost')
flight_port = int(os.getenv('FLIGHT_PORT', 8815))
path = os.getenv('DATA', '.duckdb_data')


def parse_ticket(ticket):
    try:
        ticket_obj = json.loads(ticket.ticket.decode("utf-8"))
        if isinstance(ticket_obj, str):
            ticket_obj = json.loads(ticket_obj)
        if "query" in ticket_obj:
            return ticket_obj["query"]
    except (json.JSONDecodeError, AttributeError):
        return ticket.ticket.decode("utf-8")


@dataclass
class FlightSchemaMetadata:
    type: str
    catalog: str
    schema: str
    name: str
    comment: Optional[str]
    input_schema: pa.Schema
    description: Optional[str] = None
    action_name: Optional[str] = None

    def serialize(self) -> bytes:
        metadata = {
            'type': self.type,
            'catalog': self.catalog,
            'schema': self.schema,
            'name': self.name,
            'comment': self.comment,
            'input_schema': self.input_schema.serialize().to_pybytes()
        }
        if self.description:
            metadata['description'] = self.description
        if self.action_name:
            metadata['action_name'] = self.action_name
        return msgpack.packb(metadata)

    @classmethod
    def deserialize(cls, data: bytes) -> 'FlightSchemaMetadata':
        metadata = msgpack.unpackb(data)
        metadata['input_schema'] = pa.ipc.read_schema(metadata['input_schema'])
        return cls(**metadata)


@dataclass
class SerializedSchema:
    schema: str
    description: str
    tags: Dict[str, str]
    contents: Dict[str, Optional[str]]
    type: str

    def to_dict(self) -> Dict:
        return {
            "schema": self.schema,
            "description": self.description,
            "tags": self.tags,
            "type": self.type,
            "contents": {
                "url": None,
                "sha256": None,
                "serialized": None
            }
        }


if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal_handler)

    def run_flask():
        """Run Flask server"""
        logger.info("Starting Flask server")
        try:
            app.run(host=host, port=port, use_reloader=False)
        except Exception as e:
            logger.exception("Flask server error")
        finally:
            logger.info("Flask server stopped")

    def run_flight_server():
        """Run Flight server"""

        class HeaderMiddleware(flight.ServerMiddleware):
            def __init__(self):
                self.authorization = None
                self.headers = {}

            def call_completed(self, exception=None):
                pass

        class HeaderMiddlewareFactory(flight.ServerMiddlewareFactory):
            def start_call(self, info, headers):
                logger.debug(f"Info received: {info}")
                logger.debug(f"Headers received: {headers}")
                middleware = HeaderMiddleware()
                middleware.headers = headers
                if "authorization" in headers:
                    auth_header = headers["authorization"][0]
                    auth_header = auth_header[7:] if auth_header.startswith('Bearer ') else auth_header
                    middleware.authorization = auth_header
                return middleware

        class DuckDBFlightServer(flight.FlightServerBase):
            def __init__(self, location=f"grpc://{flight_host}:{flight_port}", db_path=":memory:"):
                middleware = {"auth": HeaderMiddlewareFactory()}
                super().__init__(location=location, middleware=middleware)
                self._location = location
                logger.info(f"Initializing Flight server at {location}")
                self.conn = duckdb.connect(db_path)

                catalog_schema = pa.schema([
                    ('catalog_name', pa.string()),
                    ('schema_name', pa.string()),
                    ('description', pa.string())
                ])
                table_schema = pa.schema([
                    ('table_name', pa.string()),
                    ('schema_name', pa.string()),
                    ('catalog_name', pa.string()),
                    ('table_type', pa.string())
                ])

                self.flights = [
                    {
                        "command": "show_databases",
                        "ticket": flight.Ticket("SHOW DATABASES".encode("utf-8")),
                        "location": [self._location],
                        "schema": catalog_schema
                    },
                    {
                        "command": "show_tables",
                        "ticket": flight.Ticket("SHOW TABLES".encode("utf-8")),
                        "location": [self._location],
                        "schema": table_schema
                    },
                    {
                        "command": "show_version",
                        "ticket": flight.Ticket("SELECT version()".encode("utf-8")),
                        "location": [self._location],
                        "schema": pa.schema([('version', pa.string())])
                    },
                    {
                        "command": "list_schemas",
                        "ticket": flight.Ticket("SHOW ALL TABLES".encode("utf-8")),
                        "location": [self._location],
                        "schema": table_schema
                    }
                ]

            def _get_connection_from_context(self, context) -> duckdb.DuckDBPyConnection:
                """Get the appropriate connection based on Flight context using ConnectionManager"""
                middleware = context.get_middleware("auth")
                auth_header = middleware.authorization if middleware and middleware.authorization else None
                user_pass_hash = None
                if auth_header:
                    if isinstance(auth_header, str):
                        if ':' in auth_header:
                            username, password = auth_header.split(':', 1)
                            user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
                        else:
                            user_pass_hash = auth_header
                conn = connection_manager.get_connection(user_pass_hash)
                return conn

            def return_connection_to_pool(self, context, conn: duckdb.DuckDBPyConnection):
                """Return the connection to the pool after use."""
                middleware = context.get_middleware("auth")
                auth_header = middleware.authorization if middleware and middleware.authorization else None
                user_pass_hash = None
                if auth_header:
                    if isinstance(auth_header, str):
                        if ':' in auth_header:
                            username, password = auth_header.split(':', 1)
                            user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
                        else:
                            user_pass_hash = auth_header
                connection_manager.return_connection(conn, user_pass_hash)

            def do_action(self, context, action):
                """Handle Flight actions"""
                logger.debug(f"Action Request: {action}")
                current_conn = None
                try:
                    current_conn = self._get_connection_from_context(context)
                    if action.type == "list_schemas":
                        try:
                            body = json.loads(action.body.to_pybytes().decode('utf-8'))
                            catalog_name = body.get("catalog_name", "main")

                            query = """
                                SELECT
                                    schema_name as schema,
                                    'DuckDB Schema' as description,
                                    '{}' as tags,
                                    'table' as type
                                FROM information_schema.schemata
                                WHERE catalog_name = ?
                            """
                            result = current_conn.execute(query, [catalog_name]).fetchall()

                            schemas = []
                            for row in result:
                                schema = SerializedSchema(
                                    schema=catalog_name,
                                    description=row[1],
                                    tags=json.loads(row[2]),
                                    contents={"url": None, "sha256": None, "serialized": None},
                                    type=row[3]
                                )
                                schemas.append(schema.to_dict())

                            catalog_root = {
                                "contents": {
                                    "url": None,
                                    "sha256": None,
                                    "serialized": None
                                },
                                "schemas": schemas
                            }

                            packed_data = msgpack.packb(catalog_root)
                            compressor = zstd.ZstdCompressor()
                            compressed_data = compressor.compress(packed_data)
                            decompressed_length = len(packed_data)
                            length_bytes = decompressed_length.to_bytes(4, byteorder='little')

                            yield flight.Result(pa.py_buffer(length_bytes))
                            yield flight.Result(pa.py_buffer(compressed_data))

                        except Exception as e:
                            logger.exception("Error in list_schemas action")
                            raise flight.FlightUnavailableError(f"Failed to list schemas: {str(e)}")

                    elif action.type == "create_schema":
                        try:
                            try:
                                body = msgpack.unpackb(action.body.to_pybytes())
                            except:
                                body = action.body.to_pybytes().decode('utf-8')

                            schema_name = body.split('.')[-1] if '.' in body else body
                            query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
                            logger.debug(f"Creating schema with query: {query}")
                            current_conn.execute(query)

                        except Exception as e:
                            logger.exception("Error in create_schema action")
                            raise flight.FlightUnavailableError(f"Failed to create schema: {str(e)}")

                    elif action.type == "create_table":
                        try:
                            body_bytes = action.body.to_pybytes()
                            logger.debug(f"Raw table creation bytes: {body_bytes.hex()}")

                            try:
                                reader = pa.ipc.open_stream(pa.py_buffer(body_bytes))
                                table = reader.read_all()

                                logger.debug(f"Arrow schema: {table.schema}")
                                logger.debug(f"Column names: {table.column_names}")

                                schema_metadata = table.schema.metadata
                                catalog_name = schema_metadata.get(b'catalog_name', b'').decode('utf-8')
                                schema_name = schema_metadata.get(b'schema_name', b'').decode('utf-8')
                                table_name = schema_metadata.get(b'table_name', b'').decode('utf-8')

                                actual_schema = schema_name.split('.')[-1] if '.' in schema_name else schema_name

                                columns = []
                                for field in table.schema:
                                    columns.append({
                                        'name': field.name,
                                        'type': self._arrow_to_duckdb_type(field.type)
                                    })

                                logger.debug(f"Parsed metadata - catalog: {catalog_name}, schema: {schema_name}, table: {table_name}")
                                logger.debug(f"Columns: {columns}")

                                if not actual_schema or not table_name:
                                    raise flight.FlightUnavailableError(
                                        f"Missing schema_name or table_name in request. Found catalog={catalog_name}, schema={schema_name}, table={table_name}")

                                column_defs = []
                                for col in columns:
                                    name = col.get('name')
                                    type_ = col.get('type')
                                    if not name or not type_:
                                        raise flight.FlightUnavailableError(f"Invalid column definition: {col}")
                                    column_defs.append(f"{name} {type_}")

                                query = f"""CREATE TABLE IF NOT EXISTS {actual_schema}.{table_name} (
                                    {', '.join(column_defs)}
                                )"""

                                logger.debug(f"Creating table with query: {query}")
                                current_conn.execute(query)

                                schema_metadata = FlightSchemaMetadata(
                                    type="table",
                                    catalog=catalog_name,
                                    schema=schema_name,
                                    name=table_name,
                                    comment=None,
                                    input_schema=table.schema
                                )

                                flight_info = flight.FlightInfo(
                                    table.schema,
                                    flight.FlightDescriptor.for_path(table_name.encode()),
                                    [flight.FlightEndpoint(
                                        ticket=flight.Ticket(
                                            f"SELECT * FROM {catalog_name}.{schema_name}.{table_name}".encode()
                                        ),
                                        locations=[self._location]
                                    )],
                                    -1,
                                    -1,
                                    schema_metadata.serialize()
                                )

                                yield flight.Result(flight_info.serialize())

                            except Exception as e:
                                logger.exception("Failed to parse Arrow IPC data")
                                raise flight.FlightUnavailableError(f"Invalid Arrow IPC data in request: {str(e)}")

                        except Exception as e:
                            logger.exception("Error in create_table action")
                            raise flight.FlightUnavailableError(f"Failed to create table: {str(e)}")

                    else:
                        raise flight.FlightUnavailableError(f"Action '{action.type}' not implemented")

                except Exception as e:
                    logger.exception("Error in do_action")
                    raise flight.FlightUnavailableError(f"Action failed: {str(e)}")
                finally:
                    if current_conn:
                        self.return_connection_to_pool(context, current_conn)

            def do_get(self, context, ticket):
                """Handle 'GET' requests"""
                logger.debug("do_get called")
                current_conn = None
                try:
                    current_conn = self._get_connection_from_context(context)

                    query = parse_ticket(ticket)

                    if query.lower().startswith("select"):
                        parts = query.split()
                        for i, part in enumerate(parts):
                            if "deltalake." in part.lower():
                                parts[i] = part.split(".", 1)[1]
                        query = " ".join(parts)

                    logger.info(f"Executing query: {query}")
                    result_table = current_conn.execute(query).fetch_arrow_table()
                    batches = result_table.to_batches(max_chunksize=1024)
                    if not batches:
                        logger.debug("No data in result")
                        schema = result_table.schema
                        return flight.RecordBatchStream(pa.Table.from_batches([], schema))
                    logger.debug(f"Returning {len(batches)} batches")
                    return flight.RecordBatchStream(pa.Table.from_batches(batches))
                except Exception as e:
                    logger.exception(f"Query execution error: {str(e)}")
                    raise flight.FlightUnavailableError(f"Query execution failed: {str(e)}")
                finally:
                    if current_conn:
                        self.return_connection_to_pool(context, current_conn)

            def do_put(self, context, descriptor, reader, writer):
                """Handle 'PUT' requests"""
                current_conn = None
                try:
                    current_conn = self._get_connection_from_context(context)
                    table = reader.read_all()
                    table_name = descriptor.path[0].decode('utf-8')
                    current_conn.register("temp_table", table)
                    current_conn.execute(
                        f"INSERT INTO {table_name} SELECT * FROM temp_table")
                except Exception as e:
                    logger.exception("Error in do_put")
                    raise flight.FlightUnavailableError(f"Put operation failed: {str(e)}")
                finally:
                    if current_conn:
                        self.return_connection_to_pool(context, current_conn)

            def get_flight_info(self, context, descriptor):
                """Implement 'get_flight_info'"""
                current_conn = None
                try:
                    current_conn = self._get_connection_from_context(context)

                    if descriptor.command is not None:
                        query = descriptor.command.decode("utf-8")
                        result_table = current_conn.execute(query).fetch_arrow_table()
                        schema = result_table.schema
                        endpoints = [flight.FlightEndpoint(
                            ticket=flight.Ticket(query.encode("utf-8")),
                            locations=[self._location]
                        )]
                        return flight.FlightInfo(schema, descriptor, endpoints, -1, -1)
                    elif descriptor.path is not None:
                        for flight_info in self.flights:
                            if descriptor.path[0].decode("utf-8") == flight_info["command"]:
                                query = flight_info["ticket"].ticket.decode("utf-8")
                                logger.info(f"Attempting flight with query: {query}")
                                try:
                                    result_table = current_conn.execute(query).fetch_arrow_table()
                                    schema = result_table.schema
                                    endpoints = [flight.FlightEndpoint(
                                        ticket=flight.Ticket(query.encode("utf-8")),
                                        locations=[self._location]
                                    )]
                                    return flight.FlightInfo(schema, descriptor, endpoints, -1, -1)

                                except Exception as e:
                                    logger.exception(f"Flight execution error: {str(e)}")
                                    raise flight.FlightUnavailableError("Failed taking off")
                    else:
                        raise flight.FlightUnavailableError(
                            "No command or path provided in the descriptor")
                except Exception as e:
                    logger.exception("Error in get_flight_info")
                    raise flight.FlightUnavailableError(f"Failed to get flight info: {str(e)}")
                finally:
                    if current_conn:
                        self.return_connection_to_pool(context, current_conn)

            def list_flights(self, context, criteria):
                """List available flights with metadata"""
                logger.info("Listing available flights")
                current_conn = None
                try:
                    current_conn = self._get_connection_from_context(context)
                    middleware = context.get_middleware("auth")
                    headers = middleware.headers if middleware else {}
                    catalog_filter = None
                    schema_filter = None

                    if "airport-list-flights-filter-catalog" in headers:
                        catalog_filter = headers["airport-list-flights-filter-catalog"][0]
                    if "airport-list-flights-filter-schema" in headers:
                        schema_filter = headers["airport-list-flights-filter-schema"][0]

                    logger.debug(f"Filtering flights - catalog: {catalog_filter}, schema: {schema_filter}")

                    if catalog_filter and schema_filter:
                        query = f"""
                            SELECT
                                table_name,
                                table_schema as schema_name,
                                table_catalog as catalog_name,
                                table_type,
                                column_name,
                                data_type,
                                is_nullable
                            FROM information_schema.tables
                            JOIN information_schema.columns USING (table_catalog, table_schema, table_name)
                            WHERE table_catalog = '{catalog_filter}'
                            AND table_schema = '{schema_filter}'
                            ORDER BY table_name, ordinal_position
                        """

                        try:
                            result = current_conn.execute(query).fetchall()
                            tables = {}
                            for row in result:
                                table_name = row[0]
                                if table_name not in tables:
                                    tables[table_name] = {
                                        'schema_name': row[1],
                                        'catalog_name': row[2],
                                        'table_type': row[3],
                                        'columns': []
                                    }
                                tables[table_name]['columns'].append({
                                    'name': row[4],
                                    'type': row[5],
                                    'nullable': row[6] == 'YES'
                                })
                            for table_name, table_info in tables.items():
                                fields = []
                                for col in table_info['columns']:
                                    arrow_type = self._duckdb_to_arrow_type(col['type'])
                                    field = pa.field(col['name'], arrow_type, nullable=col['nullable'])
                                    fields.append(field)

                                schema = pa.schema(fields)
                                schema_metadata = FlightSchemaMetadata(
                                    type="table",
                                    catalog=table_info['catalog_name'],
                                    schema=table_info['schema_name'],
                                    name=table_name,
                                    comment=None,
                                    input_schema=schema
                                )

                                flight_info_obj = flight.FlightInfo(
                                    schema,
                                    flight.FlightDescriptor.for_path([table_name.encode()]),
                                    [flight.FlightEndpoint(
                                        ticket=flight.Ticket(
                                            f"SELECT * FROM {table_info['catalog_name']}.{table_info['schema_name']}.{table_name}".encode()
                                        ),
                                        locations=[self._location]
                                    )],
                                    -1,
                                    -1,
                                    schema_metadata.serialize()
                                )
                                yield flight_info_obj


                        except Exception as e:
                            logger.exception(f"Error querying tables: {str(e)}")
                            raise flight.FlightUnavailableError(f"Failed to list tables: {str(e)}")

                    else:
                        for flight_info in self.flights:
                            schema_metadata = FlightSchemaMetadata(
                                type="table",
                                catalog="main",
                                schema="public",
                                name=flight_info["command"],
                                comment=None,
                                input_schema=flight_info["schema"]
                            )
                            flight_info_obj = flight.FlightInfo(
                                flight_info["schema"],
                                flight.FlightDescriptor.for_path(flight_info["command"].encode()), # Corrected line - removed extra brackets
                                [flight.FlightEndpoint(
                                    ticket=flight_info["ticket"],
                                    locations=[self._location]
                                )],
                                -1,
                                -1,
                                schema_metadata.serialize()
                            )
                            yield flight_info_obj
                except Exception as e:
                    logger.exception("Error in list_flights")
                    raise flight.FlightUnavailableError(f"Failed to list flights: {str(e)}")
                finally:
                    if current_conn:
                        self.return_connection_to_pool(context, current_conn)

            def _duckdb_to_arrow_type(self, duckdb_type: str) -> pa.DataType:
                """Convert DuckDB type to Arrow type."""
                duckdb_type = duckdb_type.upper()
                if 'VARCHAR' in duckdb_type or 'TEXT' in duckdb_type:
                    return pa.string()
                elif 'INT' in duckdb_type:
                    return pa.int64()
                elif 'DOUBLE' in duckdb_type or 'FLOAT' in duckdb_type:
                    return pa.float64()
                elif 'BOOLEAN' in duckdb_type:
                    return pa.bool_()
                elif 'DATE' in duckdb_type:
                    return pa.date32()
                elif 'TIMESTAMP' in duckdb_type:
                    return pa.timestamp('us')
                elif 'LIST' in duckdb_type:
                    element_type = duckdb_type.replace('LIST[', '').replace(']', '')
                    return pa.list_(self._duckdb_to_arrow_type(element_type))
                else:
                    logger.warning(f"Unknown DuckDB type: {duckdb_type}.  Defaulting to string.")
                    return pa.string()

            def _arrow_to_duckdb_type(self, arrow_type):
                """Convert Arrow type to DuckDB type"""
                if pa.types.is_string(arrow_type):
                    return 'VARCHAR'
                elif pa.types.is_int32(arrow_type):
                    return 'INTEGER'
                elif pa.types.is_int64(arrow_type):
                    return 'BIGINT'
                elif pa.types.is_float32(arrow_type):
                    return 'FLOAT'
                elif pa.types.is_float64(arrow_type):
                    return 'DOUBLE'
                elif pa.types.is_boolean(arrow_type):
                    return 'BOOLEAN'
                elif pa.types.is_list(arrow_type):
                    return f'{self._arrow_to_duckdb_type(arrow_type.value_type)}[]'
                else:
                    return 'VARCHAR'

            def do_exchange(self, context, descriptor, reader, writer):
                """Handle data exchange (PUT/INSERT operations)"""
                logger.debug("do_exchange called")
                current_conn = None
                try:
                    current_conn = self._get_connection_from_context(context)
                    middleware = context.get_middleware("auth")
                    headers = middleware.headers if middleware else {}
                    operation = headers.get("airport-operation", [None])[0]
                    logger.debug(f"Exchange operation: {operation}")

                    if operation == "insert":
                        # Get table path from headers
                        table_path = headers.get("airport-flight-path", [None])[0]
                        if not table_path:
                            raise flight.FlightUnavailableError("No table path provided for insert operation")

                        logger.debug(f"Inserting into table: {table_path}")

                        # Rewrite table_path to remove deltalake catalog prefix if present
                        if "deltalake." in table_path.lower():
                            parts = table_path.split(".")
                            if len(parts) > 2 and parts[0].lower() == "deltalake":
                                table_path = ".".join(parts[1:])
                                logger.debug(f"Rewritten table_path for insert: {table_path}")

                        try:
                            # Read schema from reader
                            schema = reader.schema
                            logger.debug(f"Received schema: {schema}")

                            # Create response schema early
                            response_schema = pa.schema([('rows_inserted', pa.int64())])
                            writer.begin(response_schema)

                            # Process data in batches
                            total_rows = 0
                            batch_num = 0

                            # Read all batches
                            try:
                                while True:
                                    try:
                                        batch, metadata = reader.read_chunk()
                                        if batch is None:
                                            break

                                        batch_num += 1
                                        logger.debug(f"Processing batch {batch_num} with {len(batch)} rows")

                                        # Create temporary table for this batch
                                        temp_table = pa.Table.from_batches([batch])
                                        temp_name = f"temp_insert_table_{batch_num}"

                                        # Register and insert this batch
                                        current_conn.register(temp_name, temp_table)
                                        # actual_schema = table_path.split('.')[0] if '.' in table_path else table_path # Removed - using hardcoded schema
                                        # query = f"INSERT INTO {actual_schema}.{table_path} SELECT * FROM temp_insert_table_1" # Old incorrect query
                                        query = f"INSERT INTO test1.{table_path} SELECT * FROM temp_insert_table_1" # Hardcoded schema 'test1' for now - CORRECTED LINE
                                        logger.debug(f"Executing insert query: {query}")
                                        current_conn.execute(query)

                                        total_rows += len(batch)

                                    except StopIteration:
                                        logger.debug("Reached end of input stream")
                                        break
                            except Exception as e:
                                logger.exception(f"Error reading batch")
                                raise

                            logger.debug(f"Inserted total of {total_rows} rows")

                            # Write response
                            response_table = pa.Table.from_pylist(
                                [{'rows_inserted': total_rows}],
                                schema=response_schema
                            )
                            writer.write_table(response_table)
                            writer.close()

                        except Exception as e:
                            logger.exception("Error during insert operation")
                            raise flight.FlightUnavailableError(f"Insert operation failed: {str(e)}")

                    else:
                        raise flight.FlightUnavailableError(f"Unsupported operation: {operation}")

                except Exception as e:
                    logger.exception("Error in do_exchange")
                    raise flight.FlightUnavailableError(f"Exchange operation failed: {str(e)}")
                finally:
                    if current_conn:
                        self.return_connection_to_pool(context, current_conn)

        server = DuckDBFlightServer()
        logger.info(
            f"Starting DuckDB Flight server on {flight_host}:{flight_port}")
        server.serve()

    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    flight_thread = threading.Thread(target=run_flight_server, daemon=True)
    flight_thread.start()

    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
    finally:
        logger.info("Shutting down...")
