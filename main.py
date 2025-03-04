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

from flask import Flask, request, jsonify
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

# Add near the top of the file, after imports but before app initialization
from threading import Lock
from typing import Dict, Optional

class ConnectionManager:
    def __init__(self):
        self._connections: Dict[str, duckdb.DuckDBPyConnection] = {}
        self._lock = Lock()
        
        # Create default in-memory connection
        self._default_conn = duckdb.connect(':memory:')
        self._setup_extensions(self._default_conn)
    
    def _setup_extensions(self, conn: duckdb.DuckDBPyConnection):
        """Set up required extensions for a connection"""
        try:
            conn.install_extension("chsql", repository="community")
            conn.install_extension("chsql_native", repository="community")
            conn.load_extension("chsql")
            conn.load_extension("chsql_native")
        except Exception as e:
            logger.warning(f"Failed to initialize extensions: {e}")
    
    def get_connection(self, auth_hash: Optional[str] = None) -> duckdb.DuckDBPyConnection:
        """Get or create a connection for the given auth hash"""
        if not auth_hash:
            return self._default_conn
            
        with self._lock:
            if auth_hash not in self._connections:
                db_file = os.path.join(dbpath, f"{auth_hash}.db")
                logger.info(f'Creating new connection for {db_file}')
                conn = duckdb.connect(db_file)
                self._setup_extensions(conn)
                self._connections[auth_hash] = conn
            return self._connections[auth_hash]

# Create global connection manager
connection_manager = ConnectionManager()

# Replace the global conn variable with a property that uses the connection manager
def get_current_connection() -> duckdb.DuckDBPyConnection:
    """Get the current connection based on authentication"""
    auth = request.authorization if hasattr(request, 'authorization') else None
    if auth and auth.username and auth.password:
        user_pass_hash = hashlib.sha256((auth.username + auth.password).encode()).hexdigest()
        return connection_manager.get_connection(user_pass_hash)
    return connection_manager.get_connection()

# Remove the global conn variable and replace with this property
@property
def conn():
    return get_current_connection()


@auth.verify_password
def verify(username, password):
    if not (username and password):
        logger.debug('Using stateless session')
        return True
    
    logger.info(f"Using http auth: {username}:<password>")
    user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
    # Just verify the connection exists/can be created
    connection_manager.get_connection(user_pass_hash)
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


def handle_insert_query(query, format, data=None, conn=None):
    if conn is None:
        conn = get_current_connection()
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
    temp_file = tempfile.NamedTemporaryFile(
        delete=False, mode='w+', encoding='utf-8')
    temp_file.write(data)
    temp_file.flush()
    temp_file.close()
    return temp_file.name


def duckdb_query_with_errmsg(query, format='JSONCompact', data=None, request_method="GET"):
    try:
        # Get connection for current request
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
    format = request.args.get(
        'default_format', default="JSONCompact", type=str)
    database = request.args.get('database', default="", type=str)
    query_id = request.args.get('query_id', default=None, type=str)
    data = None
    query, sanitized_format = sanitize_query(query)
    if sanitized_format:
        format = sanitized_format
    print(
        f"Received request: method={request.method}, query={query}, format={format}, database={database}")
    if query_id is not None and not query:
        if query_id in cache:
            return cache[query_id], 200
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
    if len(errmsg) == 0:
        if request.method == "HEAD":
            response = app.response_class(status=200)
            response.headers['Content-Type'] = 'application/json'
            response.headers['Accept-Ranges'] = 'bytes'
            content_length = len(result) if isinstance(
                result, bytes) else len(result.decode())
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
    format = request.args.get(
        'default_format', default="JSONCompact", type=str)
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
port = int(os.getenv('PORT', 8123))
flight_host = os.getenv('FLIGHT_HOST', 'localhost')
flight_port = int(os.getenv('FLIGHT_PORT', 8815))
path = os.getenv('DATA', '.duckdb_data')

def parse_ticket(ticket):
    try:
        # Try to decode the ticket as a JSON object
        ticket_obj = json.loads(ticket.ticket.decode("utf-8"))
        if isinstance(ticket_obj, str):
            # If the JSON object is a string, parse it again
            ticket_obj = json.loads(ticket_obj)
        if "query" in ticket_obj:
            return ticket_obj["query"]
    except (json.JSONDecodeError, AttributeError):
        # If decoding fails or "query" is not in the object, return the ticket as a string
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


# Add this helper class for schema serialization
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


# Patch the main function where the ticket is processed
if __name__ == '__main__':
    # Set up signal handlers
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
                self.headers = {}  # Store all headers

            def call_completed(self, exception=None):
                pass

        class HeaderMiddlewareFactory(flight.ServerMiddlewareFactory):
            def start_call(self, info, headers):
                logger.debug(f"Info received: {info}")
                logger.debug(f"Headers received: {headers}")
                middleware = HeaderMiddleware()
                
                # Store all headers in the middleware
                middleware.headers = headers
                
                if "authorization" in headers:
                    # Get first value from list
                    auth = headers["authorization"][0]
                    auth = auth[7:] if auth.startswith('Bearer ') else auth
                    middleware.authorization = auth
                    
                return middleware

        class DuckDBFlightServer(flight.FlightServerBase):
            def __init__(self, location=f"grpc://{flight_host}:{flight_port}", db_path=":memory:"):
                middleware = {"auth": HeaderMiddlewareFactory()}
                super().__init__(location=location, middleware=middleware)
                self._location = location
                logger.info(f"Initializing Flight server at {location}")
                self.conn = duckdb.connect(db_path)
                
                # Define schema for catalog listing
                catalog_schema = pa.schema([
                    ('catalog_name', pa.string()),
                    ('schema_name', pa.string()),
                    ('description', pa.string())
                ])
                
                # Define schema for table listing
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
                """Get the appropriate connection based on Flight context"""
                middleware = context.get_middleware("auth")
                if middleware and middleware.authorization:
                    auth_header = middleware.authorization
                    if isinstance(auth_header, str):
                        if ':' in auth_header:
                            username, password = auth_header.split(':', 1)
                            user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
                        else:
                            user_pass_hash = auth_header
                        return connection_manager.get_connection(user_pass_hash)
                return connection_manager.get_connection()

            def do_action(self, context, action):
                """Handle Flight actions"""
                logger.debug(f"Action Request: {action}")
                
                if action.type == "list_schemas":
                    try:
                        # Parse the request body
                        body = json.loads(action.body.to_pybytes().decode('utf-8'))
                        catalog_name = body.get("catalog_name", "main")
                        
                        # Query schemas from DuckDB
                        query = """
                            SELECT 
                                schema_name as schema,
                                'DuckDB Schema' as description,
                                '{}' as tags,
                                'table' as type
                            FROM information_schema.schemata 
                            WHERE catalog_name = ?
                        """
                        result = self.conn.execute(query, [catalog_name]).fetchall()
                        
                        # Convert results to SerializedSchema objects
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
                        
                        # Create the catalog root structure
                        catalog_root = {
                            "contents": {
                                "url": None,
                                "sha256": None,
                                "serialized": None
                            },
                            "schemas": schemas
                        }
                        
                        # Serialize with msgpack
                        packed_data = msgpack.packb(catalog_root)
                        
                        # Compress with zstd
                        compressor = zstd.ZstdCompressor()
                        compressed_data = compressor.compress(packed_data)
                        
                        # Create result with decompressed length and compressed data
                        decompressed_length = len(packed_data)
                        length_bytes = decompressed_length.to_bytes(4, byteorder='little')
                        
                        # Return results as flight.Result objects
                        yield flight.Result(pa.py_buffer(length_bytes))
                        yield flight.Result(pa.py_buffer(compressed_data))
                        
                    except Exception as e:
                        logger.exception("Error in list_schemas action")
                        raise flight.FlightUnavailableError(f"Failed to list schemas: {str(e)}")
                    
                elif action.type == "create_schema":
                    try:
                        # Set up authenticated connection first
                        middleware = context.get_middleware("auth")
                        if middleware and middleware.authorization:
                            auth_header = middleware.authorization
                            logger.info(f"Using authorization from middleware: {auth_header}")
                            if isinstance(auth_header, str):
                                if ':' in auth_header:
                                    username, password = auth_header.split(':', 1)
                                    user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
                                else:
                                    user_pass_hash = auth_header 

                                db_file = os.path.join(dbpath, f"{user_pass_hash}.db")
                                logger.info(f'Using database file: {db_file}')
                                self.conn = duckdb.connect(db_file)

                        # Try msgpack first
                        try:
                            body = msgpack.unpackb(action.body.to_pybytes())
                        except:
                            # Fall back to UTF-8 if msgpack fails
                            body = action.body.to_pybytes().decode('utf-8')
                            
                        # Extract schema name from the full path (e.g., deltalake.test1 -> test1)
                        schema_name = body.split('.')[-1] if '.' in body else body
                        
                        # Create schema in the authenticated database
                        query = f"CREATE SCHEMA IF NOT EXISTS {schema_name}"
                        logger.debug(f"Creating schema with query: {query}")
                        self.conn.execute(query)
                        
                    except Exception as e:
                        logger.exception("Error in create_schema action")
                        raise flight.FlightUnavailableError(f"Failed to create schema: {str(e)}")
                    
                elif action.type == "create_table":
                    try:
                        # Set up authenticated connection first
                        middleware = context.get_middleware("auth")
                        if middleware and middleware.authorization:
                            auth_header = middleware.authorization
                            logger.info(f"Using authorization from middleware: {auth_header}")
                            if isinstance(auth_header, str):
                                if ':' in auth_header:
                                    username, password = auth_header.split(':', 1)
                                    user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
                                else:
                                    user_pass_hash = auth_header 

                                db_file = os.path.join(dbpath, f"{user_pass_hash}.db")
                                logger.info(f'Using database file: {db_file}')
                                self.conn = duckdb.connect(db_file)
                                
                        # Get the raw bytes and parse table info
                        body_bytes = action.body.to_pybytes()
                        logger.debug(f"Raw table creation bytes: {body_bytes.hex()}")
                        
                        try:
                            # Parse Arrow IPC format
                            reader = pa.ipc.open_stream(pa.py_buffer(body_bytes))
                            table = reader.read_all()
                            
                            logger.debug(f"Arrow schema: {table.schema}")
                            logger.debug(f"Column names: {table.column_names}")
                            
                            # Get metadata from schema
                            schema_metadata = table.schema.metadata
                            catalog_name = schema_metadata.get(b'catalog_name', b'').decode('utf-8')
                            schema_name = schema_metadata.get(b'schema_name', b'').decode('utf-8')
                            table_name = schema_metadata.get(b'table_name', b'').decode('utf-8')
                            
                            # Extract actual schema name (e.g., test1 from deltalake.test1)
                            actual_schema = schema_name.split('.')[-1] if '.' in schema_name else schema_name
                            
                            # Get columns from schema
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
                            
                            # Create table in the authenticated database
                            query = f"""CREATE TABLE IF NOT EXISTS {actual_schema}.{table_name} (
                                {', '.join(column_defs)}
                            )"""
                            
                            logger.debug(f"Creating table with query: {query}")
                            self.conn.execute(query)
                            
                            # Create and return FlightInfo for the newly created table
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
                                -1,  # total_records
                                -1,  # total_bytes
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

            def do_get(self, context, ticket):
                """Handle 'GET' requests"""
                logger.debug("do_get called")
                try:
                    # Access middleware and set up connection
                    middleware = context.get_middleware("auth")
                    if middleware and middleware.authorization:
                        auth_header = middleware.authorization
                        logger.info(f"Using authorization from middleware: {auth_header}")
                        if isinstance(auth_header, str):
                            if ':' in auth_header:
                                username, password = auth_header.split(':', 1)
                                user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
                            else:
                                user_pass_hash = auth_header 

                            db_file = os.path.join(dbpath, f"{user_pass_hash}.db")
                            logger.info(f'Using database file: {db_file}')
                            self.conn = duckdb.connect(db_file)

                except Exception as e:
                    logger.debug(f"Middleware access error: {e}")

                query = parse_ticket(ticket)
                
                # Rewrite query to use local schema instead of deltalake catalog
                if query.lower().startswith("select"):
                    # Extract schema and table from deltalake.schema.table pattern
                    parts = query.split()
                    for i, part in enumerate(parts):
                        if "deltalake." in part.lower():
                            # Remove the catalog prefix, keeping schema and table
                            parts[i] = part.split(".", 1)[1]
                    query = " ".join(parts)
                
                logger.info(f"Executing query: {query}")
                try:
                    result_table = self.conn.execute(query).fetch_arrow_table()
                    batches = result_table.to_batches(max_chunksize=1024)
                    if not batches:
                        logger.debug("No data in result")
                        schema = result_table.schema
                        return flight.RecordBatchStream(pa.Table.from_batches([], schema))
                    logger.debug(f"Returning {len(batches)} batches")
                    return flight.RecordBatchStream(pa.Table.from_batches(batches))
                except Exception as e:
                    logger.exception(f"Query execution error: {str(e)}")
                    raise

            def do_put(self, context, descriptor, reader, writer):
                """Handle 'PUT' requests"""
                table = reader.read_all()
                table_name = descriptor.path[0].decode('utf-8')
                self.conn.register("temp_table", table)
                self.conn.execute(
                    f"INSERT INTO {table_name} SELECT * FROM temp_table")

            def get_flight_info(self, context, descriptor):
                """Implement 'get_flight_info'"""
                try:
                    # Set up authenticated connection
                    middleware = context.get_middleware("auth")
                    if middleware and middleware.authorization:
                        auth_header = middleware.authorization
                        logger.info(f"Using authorization from middleware: {auth_header}")
                        if isinstance(auth_header, str):
                            if ':' in auth_header:
                                username, password = auth_header.split(':', 1)
                                user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
                            else:
                                user_pass_hash = auth_header 

                            db_file = os.path.join(dbpath, f"{user_pass_hash}.db")
                            logger.info(f'Using database file: {db_file}')
                            self.conn = duckdb.connect(db_file)

                    if descriptor.command is not None:
                        query = descriptor.command.decode("utf-8")
                        result_table = self.conn.execute(query).fetch_arrow_table()
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
                                    result_table = self.conn.execute(query).fetch_arrow_table()
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

            def list_flights(self, context, criteria):
                """List available flights with metadata"""
                logger.info("Listing available flights")
                
                try:
                    # Set up authenticated connection
                    middleware = context.get_middleware("auth")
                    if middleware and middleware.authorization:
                        auth_header = middleware.authorization
                        logger.info(f"Using authorization from middleware: {auth_header}")
                        if isinstance(auth_header, str):
                            if ':' in auth_header:
                                username, password = auth_header.split(':', 1)
                                user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
                            else:
                                user_pass_hash = auth_header 

                            db_file = os.path.join(dbpath, f"{user_pass_hash}.db")
                            logger.info(f'Using database file: {db_file}')
                            self.conn = duckdb.connect(db_file)

                    headers = middleware.headers if middleware else {}
                    catalog_filter = None
                    schema_filter = None
                    
                    # Extract filters from headers
                    if "airport-list-flights-filter-catalog" in headers:
                        catalog_filter = headers["airport-list-flights-filter-catalog"][0]
                    if "airport-list-flights-filter-schema" in headers:
                        schema_filter = headers["airport-list-flights-filter-schema"][0]
                    
                    logger.debug(f"Filtering flights - catalog: {catalog_filter}, schema: {schema_filter}")
                    
                    if catalog_filter and schema_filter:
                        # Query for tables in the specific catalog and schema
                        query = f"""
                            SELECT 
                                table_name,
                                table_schema as schema_name,
                                table_catalog as catalog_name,
                                table_type,
                                column_name,
                                data_type
                            FROM information_schema.tables 
                            JOIN information_schema.columns USING (table_catalog, table_schema, table_name)
                            WHERE table_catalog = '{catalog_filter}'
                            AND table_schema = '{schema_filter}'
                            ORDER BY table_name, ordinal_position
                        """
                        
                        try:
                            result = self.conn.execute(query).fetchall()
                            
                            # Group results by table
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
                                    'type': row[5]
                                })
                            
                            # Create flight info for each table
                            for table_name, table_info in tables.items():
                                # Create Arrow schema from columns
                                fields = []
                                for col in table_info['columns']:
                                    # Convert DuckDB type to Arrow type
                                    arrow_type = pa.string()  # Default to string
                                    if 'INT' in col['type'].upper():
                                        arrow_type = pa.int64()
                                    elif 'DOUBLE' in col['type'].upper() or 'FLOAT' in col['type'].upper():
                                        arrow_type = pa.float64()
                                    elif 'BOOLEAN' in col['type'].upper():
                                        arrow_type = pa.bool_()
                                    fields.append(pa.field(col['name'], arrow_type))
                                
                                schema = pa.schema(fields)
                                
                                # Create metadata for the table
                                schema_metadata = FlightSchemaMetadata(
                                    type="table",
                                    catalog=table_info['catalog_name'],
                                    schema=table_info['schema_name'],
                                    name=table_name,
                                    comment=None,
                                    input_schema=schema
                                )
                                
                                # Create flight info
                                flight_info = flight.FlightInfo(
                                    schema,
                                    flight.FlightDescriptor.for_path([table_name.encode()]),
                                    [flight.FlightEndpoint(
                                        ticket=flight.Ticket(
                                            f"SELECT * FROM {table_info['catalog_name']}.{table_info['schema_name']}.{table_name}".encode()
                                        ),
                                        locations=[self._location]
                                    )],
                                    -1,  # total_records
                                    -1,  # total_bytes
                                    schema_metadata.serialize()
                                )
                                
                                yield flight_info
                                
                        except Exception as e:
                            logger.exception(f"Error querying tables: {str(e)}")
                            raise flight.FlightUnavailableError(f"Failed to list tables: {str(e)}")
                            
                    else:
                        # Return default flights when no specific filters
                        for flight_info in self.flights:
                            schema_metadata = FlightSchemaMetadata(
                                type="table",
                                catalog="main",
                                schema="public",
                                name=flight_info["command"],
                                comment=None,
                                input_schema=flight_info["schema"]
                            )
                            
                            yield flight_info
                            
                except Exception as e:
                    logger.exception("Error in list_flights")
                    raise flight.FlightUnavailableError(f"Failed to list flights: {str(e)}")

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
                    return 'VARCHAR'  # Default to VARCHAR for unknown types

            def do_exchange(self, context, descriptor, reader, writer):
                """Handle data exchange (PUT/INSERT operations)"""
                logger.debug("do_exchange called")
                try:
                    # Get headers from middleware
                    middleware = context.get_middleware("auth")
                    headers = middleware.headers if middleware else {}
                    
                    # Set up authenticated connection
                    if middleware and middleware.authorization:
                        auth_header = middleware.authorization
                        logger.info(f"Using authorization from middleware: {auth_header}")
                        if isinstance(auth_header, str):
                            if ':' in auth_header:
                                username, password = auth_header.split(':', 1)
                                user_pass_hash = hashlib.sha256((username + password).encode()).hexdigest()
                            else:
                                user_pass_hash = auth_header 

                            db_file = os.path.join(dbpath, f"{user_pass_hash}.db")
                            logger.info(f'Using database file: {db_file}')
                            self.conn = duckdb.connect(db_file)
                    
                    # Get operation type from headers
                    operation = headers.get("airport-operation", [None])[0]
                    logger.debug(f"Exchange operation: {operation}")
                    
                    if operation == "insert":
                        # Get table path from headers
                        table_path = headers.get("airport-flight-path", [None])[0]
                        if not table_path:
                            raise flight.FlightUnavailableError("No table path provided for insert operation")
                        
                        logger.debug(f"Inserting into table: {table_path}")
                        
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
                                        self.conn.register(temp_name, temp_table)
                                        actual_schema = table_path.split('.')[0] if '.' in table_path else table_path
                                        query = f"INSERT INTO {actual_schema}.{table_path} SELECT * FROM {temp_name}"
                                        logger.debug(f"Executing insert query: {query}")
                                        self.conn.execute(query)
                                        
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

        server = DuckDBFlightServer()
        logger.info(
            f"Starting DuckDB Flight server on {flight_host}:{flight_port}")
        server.serve()

    # Start Flask server in a daemon thread
    flask_thread = threading.Thread(target=run_flask, daemon=True)
    flask_thread.start()

    # Run Flight server in main thread
    flight_thread = threading.Thread(target=run_flight_server, daemon=True)
    flight_thread.start()

    # Keep main thread alive until signal
    try:
        while running:
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received")
    finally:
        logger.info("Shutting down...")
