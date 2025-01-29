<a href="https://quackpy.fly.dev" target="_blank"><img src="https://github.com/user-attachments/assets/3223f0dd-78e2-4b61-8690-5502a2290421" width=220 /></a>

> _a pipe for quackheads_

# :baby_chick: [quackpy](https://quackpy.fly.dev/?user=default#U0VMRUNUCiAgICB0b3duLAogICAgZGlzdHJpY3QsCiAgICBjb3VudCgpIEFTIGMsCkZST00gcmVhZF9wYXJxdWV0KCdodHRwczovL2RhdGFzZXRzLWRvY3VtZW50YXRpb24uczMuZXUtd2VzdC0zLmFtYXpvbmF3cy5jb20vaG91c2VfcGFycXVldC9ob3VzZV8wLnBhcnF1ZXQnKQpXSEVSRSByZWFkX3BhcnF1ZXQudG93biA9PSAnTE9ORE9OJwpHUk9VUCBCWQogICAgdG93biwKICAgIGRpc3RyaWN0Ck9SREVSIEJZIGMgREVTQwpMSU1JVCAxMA==)pe


_QuackPy is a serverless OLAP API built on top of DuckDB exposing HTTP/S and Arrow Flight SQL interfaces_


> [!TIP]
> - Easy HTTP API with multiple formats _(JSON,CSV,Parquet)_
> - Powerful Arrow Flight API for modern data clients
> - Unlocked Concurrent inserts and querying on DuckDB
> - Persistent storage using w/ multiuser authentication
> - Native access to any DuckDB Extension & Format
> - Embedded SQL Query Interface for instant usage


<br>

### :seedling: Get Started
Run using [docker](https://github.com/quackscience/quackpy/pkgs/container/quackpy) or build from source
```bash
docker pull ghcr.io/quackscience/quackpy:latest
docker run -ti --rm -p 8123:8123 -p 8815:8815 ghcr.io/quackscience/quackpy:latest
```

### 👉 Usage

> See the [Examples](https://github.com/quackscience/quackpy/tree/main/examples) directory for quick starters

> [!NOTE]
> Quackpipe executes queries in `:memory:` unless _authentication_ details are provided for data persistence

#### 🕸️ HTTP API
Execute DuckDB queries using the HTTP POST/GET API _(compatible with the ClickHouse HTTP API)_
```bash
curl -X POST "http://user:persistence@localhost:8123" \
   -H "Content-Type: application/json" \
   -d 'SELECT version()'  
```

#### ✈️ FLIGHT API
Execute DuckDB queries using the _experimental_ Flight GRPC API

> [!NOTE]
> Quackpipe executes queries in `:memory:` unless an `authorization` header is provided for data persistence

##### Custom Flight Ticket
```sql
D SELECT * FROM airport_take_flight('grpc://localhost:8815', 'SELECT 1', headers := MAP{'authorization':'user:password'} );
┌───────┐
│   1   │
│ int32 │
├───────┤
│   1   │
└───────┘
```
##### Service Flight Ticket
```sql
D select flight_descriptor, endpoint from airport_list_flights('grpc://127.0.0.1:8815', null);
┌─────────────────────────────────┬────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│        flight_descriptor        │                                                    endpoint                                                    │
│ union(cmd blob, path varchar[]) │           struct(ticket blob, "location" varchar[], expiration_time timestamp, app_metadata blob)[]            │
├─────────────────────────────────┼────────────────────────────────────────────────────────────────────────────────────────────────────────────────┤
│ show_databases                  │ [{'ticket': SHOW DATABASES, 'location': [grpc://localhost:8815], 'expiration_time': NULL, 'app_metadata': }]   │
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

##### Python Flight Client
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
quackpy ships with the DuckDB SQL quack user-interface based on [ch-ui](https://github.com/caioricciuti/ch-ui)

<a href="https://quackpy.fly.dev">
  <img src="https://github.com/user-attachments/assets/902a6336-c4f4-4a4e-85d5-78dd62cb7602">
</a>

<br>


<img src="https://github.com/user-attachments/assets/43dcbdf8-bc19-4419-a4b8-13956a6c3a2f" width=700>






###### :black_joker: Disclaimers 

[^1]: DuckDB ® is a trademark of DuckDB Foundation. All rights reserved by their respective owners. [^1]
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement. [^2]
[^3]: Released under the MIT license. See LICENSE for details. All rights reserved by their respective owners. [^3]
[^4]: Flight implementation inspired by [Duck Takes Flight](https://www.definite.app/blog/duck-takes-flight) [^4]
