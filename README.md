<a href="https://quackpipe.fly.dev" target="_blank"><img src="https://github.com/user-attachments/assets/3223f0dd-78e2-4b61-8690-5502a2290421" width=220 /></a>

> _a pipe for quackheads_

# :baby_chick: [quackpy](https://quackpy.fly.dev/?user=default#U0VMRUNUCiAgICB0b3duLAogICAgZGlzdHJpY3QsCiAgICBjb3VudCgpIEFTIGMsCkZST00gcmVhZF9wYXJxdWV0KCdodHRwczovL2RhdGFzZXRzLWRvY3VtZW50YXRpb24uczMuZXUtd2VzdC0zLmFtYXpvbmF3cy5jb20vaG91c2VfcGFycXVldC9ob3VzZV8wLnBhcnF1ZXQnKQpXSEVSRSByZWFkX3BhcnF1ZXQudG93biA9PSAnTE9ORE9OJwpHUk9VUCBCWQogICAgdG93biwKICAgIGRpc3RyaWN0Ck9SREVSIEJZIGMgREVTQwpMSU1JVCAxMA==)pe


_QuackPy is a serverless OLAP API built on top of DuckDB exposing HTTP/S and Flight SQL client interfaces_

<br>

### :seedling: Get Started
Run using [docker](https://github.com/quackscience/quackpy/pkgs/container/quackpy) or build from source
```bash
docker pull ghcr.io/quackscience/quackpy:latest
docker run -ti --rm -p 8123:8123 -p 8815:8815 ghcr.io/quackscience/quackpy:latest
```

<br>

### 👉 Usage
Quackpipe APIs execute queries in `:memory:` unless _authentication_ details are provided for data persistence.

#### HTTP API
Execute DuckDB queries using the HTTP POST/GET API
```bash
curl -X POST http://user:persistence@localhost:8123 
   -H "Content-Type: application/json"
   -d 'SELECT version()'  
```

#### FLIGHT API
Execute DuckDB queries using the _experimental_ Flight GRPC API

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

### :seedling: User-Interface
quackpy ships with the quack user-interface to execute DuckDB SQL queries

![Quack-UI-Workspace-ezgif com-crop](https://github.com/user-attachments/assets/902a6336-c4f4-4a4e-85d5-78dd62cb7602)

<br>








###### :black_joker: Disclaimers 

[^1]: DuckDB ® is a trademark of DuckDB Foundation. All rights reserved by their respective owners. [^1]
[^2]: ClickHouse ® is a trademark of ClickHouse Inc. No direct affiliation or endorsement. [^2]
[^3]: Released under the MIT license. See LICENSE for details. All rights reserved by their respective owners. [^3]
