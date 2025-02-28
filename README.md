<img src="https://github.com/user-attachments/assets/deddab90-e409-4b1b-881f-d53f02597a1d" width=80 />

# :baby_chick: [QuackFlight](https://quackpy.fly.dev/?user=default#U0VMRUNUCiAgICB0b3duLAogICAgZGlzdHJpY3QsCiAgICBjb3VudCgpIEFTIGMsCkZST00gcmVhZF9wYXJxdWV0KCdodHRwczovL2RhdGFzZXRzLWRvY3VtZW50YXRpb24uczMuZXUtd2VzdC0zLmFtYXpvbmF3cy5jb20vaG91c2VfcGFycXVldC9ob3VzZV8wLnBhcnF1ZXQnKQpXSEVSRSByZWFkX3BhcnF1ZXQudG93biA9PSAnTE9ORE9OJwpHUk9VUCBCWQogICAgdG93biwKICAgIGRpc3RyaWN0Ck9SREVSIEJZIGMgREVTQwpMSU1JVCAxMA==)


_QuackFlight is a serverless OLAP API built on top of DuckDB exposing HTTP/S and Arrow Flight SQL interfaces_


> [!IMPORTANT]
> - Arrow Flight API for modern data clients _(DuckDB Airport)_
> - Easy HTTP API with multiple formats _(JSON,CSV,Parquet)_
> - Unlocked Concurrent inserts and querying on DuckDB
> - Persistent storage using w/ multiuser authentication
> - Native access to any DuckDB Extension & Format
> - Embedded SQL Query Interface for instant usage


<br>

<img src="https://github.com/user-attachments/assets/deddab90-e409-4b1b-881f-d53f02597a1d" width=80 />

### :seedling: Get Started
Run using [docker](https://github.com/quackscience/quackflight/pkgs/container/quackflight) or build from source
```bash
docker pull ghcr.io/quackscience/quackflight:latest
docker run -ti --rm -p 8123:8123 -p 8815:8815 ghcr.io/quackscience/quackflight:latest
```

### 👉 Usage

> See the [Examples](https://github.com/quackscience/quackflight/tree/main/examples) directory for quick starters

> [!NOTE]
> Quackpipe executes queries in `:memory:` unless _authentication_ details are provided for data persistence

<br>

#### 🕸️  HTTP API
Execute DuckDB queries using the HTTP POST/GET API _(compatible with the ClickHouse HTTP API)_
```bash
curl -X POST "http://user:persistence@localhost:8123" \
   -H "Content-Type: application/json" \
   -d 'SELECT version()'  
```

#### ✈️ FLIGHT API
Execute DuckDB queries using the _experimental_ Flight GRPC API and [Airport](https://github.com/Query-farm/duckdb-airport-extension)

> [!NOTE]
> Quackpipe executes queries in `:memory:` unless an `authorization` header is provided for data persistence

##### 🎫 Pass Airport Security
```sql
CREATE SECRET airport_flight (
·       type airport,
‣       auth_token 'user:password',
·       scope 'grpc://localhost:8815'
· );
```

##### 🎫 Take Airport Flights
```sql
D select flight_descriptor, endpoint from airport_list_flights('grpc://127.0.0.1:8815', null);
┌─────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│        flight_descriptor        │                                                    endpoint                                                    │
│ union(cmd blob, path varchar[]) │           struct(ticket blob, "location" varchar[], expiration_time timestamp, app_metadata blob)[]            │
├─────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ show_databases                  │ [{'ticket': SHOW DATABASES, 'location': [grpc://localhost:8815], 'expiration_time': NULL, 'app_metadata': }]   │
│ show_tables                     │ [{'ticket': SHOW TABLES, 'location': [grpc://localhost:8815], 'expiration_time': NULL, 'app_metadata': }]      │
│ show_version                    │ [{'ticket': SELECT version(), 'location': [grpc://localhost:8815], 'expiration_time': NULL, 'app_metadata': }] │
└─────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

D select * from airport_take_flight('grpc://localhost:8815/', ['show_version']);
┌─────────────┐
│ "version"() │
│   varchar   │
├─────────────┤
│ v1.1.3      │
└─────────────┘
```

##### 🎫 ATTACH Flights Tables
```sql
D --- Attach to Flight Server
D ATTACH 'deltalake' (TYPE AIRPORT, location 'grpc://localhost:8815/'); 

D --- Create Schema + Table
D CREATE SCHEMA deltalake.test1; 
D CREATE TABLE deltalake.test1.people (
     name VARCHAR,
     love_of_duckdb INT,
     tags VARCHAR[]
    );

D --- Insert into Flight Table
D INSERT INTO deltalake.test1.people values
  ('rusty', 5, ['airport', 'datasketches']);

D --- Select from Flight Table
D SELECT * FROM deltalake.test1.people;
┌─────────┬────────────────┬─────────────────────────┐
│  name   │ love_of_duckdb │          tags           │
│ varchar │     int32      │        varchar[]        │
├─────────┼────────────────┼─────────────────────────┤
│ rusty   │              5 │ [airport, datasketches] │
├─────────┴────────────────┴─────────────────────────┤
│ 1 row.                                   3 columns │
└────────────────────────────────────────────────────┘
```

> Flight Tables can be accessed via HTTP API using the schema name
```sql
USE test1; SELECT * FROM people;
```
![image](https://github.com/user-attachments/assets/82d9c7bf-cbf2-49d3-b4dc-a57a0ddaf46a)


##### 🎫 Take Custom Flights w/ Custom Headers + Ticket
```sql
D SELECT * FROM airport_take_flight('grpc://localhost:8815', 'SELECT 1', headers := MAP{'authorization':'user:password'} );
┌───────┐
│   1   │
│ int32 │
├───────┤
│   1   │
└───────┘
```

##### 🎫 Take Python Flights
```python
from pyarrow.flight import FlightClient, Ticket, FlightCallOptions 
import json
import pandas
import tabulate

sql="""SELECT version()"""
  
flight_ticket = Ticket(sql)

token = (b"authorization", bytes(f"user:persistence".encode('utf-8')))
options = FlightCallOptions(headers=[token])
client = FlightClient(f"grpc://localhost:8815")

reader = client.do_get(flight_ticket, options)
arrow_table = reader.read_all()
# Use pyarrow and pandas to view and analyze data
data_frame = arrow_table.to_pandas()
print(data_frame.to_markdown())
```
```sql
|    | "version"()   |
|---:|:--------------|
|  0 | v1.1.3        |
```

### 📺 SQL User-Interface
quackflight ships with the DuckDB SQL quack user-interface based on [ch-ui](https://github.com/caioricciuti/ch-ui)

<a href="https://quackpy.fly.dev">
  <img src="https://github.com/user-attachments/assets/902a6336-c4f4-4a4e-85d5-78dd62cb7602">
</a>

```mermaid

sequenceDiagram
    participant Client
    participant QuackFlight
    participant DuckDB


    Client ->> QuackFlight: ListFlights
    QuackFlight ->> Client: Return Flights Table
    Client ->> QuackFlight: GetFlightInfo
    QuackFlight ->> DuckDB: DuckDB Execute
    DuckDB ->> QuackFlight: Arrow Results Stream
    QuackFlight ->> Client: FlightInfo(ticket)
    Client ->> QuackFlight: do_get(ticket)
    QuackFlight ->> Client: Stream of Results

```

<br>


###### :black_joker: Disclaimers 

[^1]: DuckDB ® is a trademark of DuckDB Foundation. All rights reserved by their respective owners. [^1]
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement. [^2]
[^3]: Released under the MIT license. See LICENSE for details. All rights reserved by their respective owners. [^3]
[^4]: Flight implementation inspired by [Duck Takes Flight](https://www.definite.app/blog/duck-takes-flight) [^4]
