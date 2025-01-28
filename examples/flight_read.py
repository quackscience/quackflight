from pyarrow.flight import FlightClient, Ticket, FlightCallOptions 
import json
import pandas
import tabulate

# Downsampling query groups data into 2-hour bins
sql="""CREATE TABLE IF NOT EXISTS test AS SELECT version(), now(); SELECT * FROM test;"""
  
flight_ticket = Ticket(sql)

token = (b"authorization", bytes(f"user:persistence".encode('utf-8')))
options = FlightCallOptions(headers=[token])
client = FlightClient(f"grpc://localhost:8815")

reader = client.do_get(flight_ticket, options)
arrow_table = reader.read_all()
# Use pyarrow and pandas to view and analyze data
data_frame = arrow_table.to_pandas()
print(data_frame.to_markdown())
