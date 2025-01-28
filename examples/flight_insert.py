import pyarrow as pa
import pyarrow.flight as flight
import random
import time
from datetime import datetime
import logging
import json

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('flight_loader')

def connect_with_retry(max_attempts=5):
    """Connect to Flight server with retry logic"""
    for attempt in range(max_attempts):
        try:
            # Create authentication headers for persistence
            token = (b"authorization", bytes(f"user:persistence".encode('utf-8')))
            options = flight.FlightCallOptions(headers=[token])
            
            # Create client and test connection
            client = flight.FlightClient(f"grpc://localhost:8815")
            ticket = flight.Ticket("SELECT 1".encode())
            reader = client.do_get(ticket, options)
            list(reader)  # Consume the result
            
            logger.info("Successfully connected to Flight server with persistence")
            return client, options
        except Exception as e:
            if attempt < max_attempts - 1:
                logger.warning(f"Connection attempt {attempt + 1} failed: {e}, retrying in 1 second...")
                time.sleep(1)
            else:
                logger.error(f"Failed to connect after {max_attempts} attempts")
                raise

def generate_batch(batch_id):
    """Generate a batch of test data"""
    num_rows = 1_000  # Smaller batch size for more frequent updates
    data = {
        "batch_id": [batch_id] * num_rows,
        "timestamp": [datetime.now().isoformat()] * num_rows,
        "value": [random.uniform(0, 100) for _ in range(num_rows)],
        "category": [random.choice(['A', 'B', 'C', 'D']) for _ in range(num_rows)]
    }
    return data

def continuous_load(client, options):
    """Continuously load data to the Flight server"""
    batch_id = 0
    table_name = "concurrent_test"  # Use a constant table name
    
    # Create table using flight ticket
    create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            batch_id BIGINT,
            timestamp VARCHAR,
            value DOUBLE,
            category VARCHAR
        )
    """
    
    try:
        # Create table
        ticket = flight.Ticket(create_table_sql.encode())
        reader = client.do_get(ticket, options)
        list(reader)  # Consume the result
        logger.info(f"Table {table_name} created successfully")
        
        while True:
            try:
                # Generate data
                data = generate_batch(batch_id)
                
                # Insert data using simple INSERT VALUES
                values = []
                for i in range(len(data['batch_id'])):
                    values.append(f"({data['batch_id'][i]}, '{data['timestamp'][i]}', {data['value'][i]}, '{data['category'][i]}')")
                
                insert_sql = f"""
                    INSERT INTO {table_name} (batch_id, timestamp, value, category)
                    VALUES {','.join(values)}
                """
                
                # Execute insert
                ticket = flight.Ticket(insert_sql.encode())
                reader = client.do_get(ticket, options)
                list(reader)  # Consume the result
                
                logger.info(f"Uploaded batch {batch_id} with {len(data['batch_id'])} rows")
                batch_id += 1
                time.sleep(2)  # Write every 2 seconds
                
            except Exception as e:
                logger.error(f"Error uploading batch {batch_id}: {str(e)}")
                time.sleep(1)  # Wait a bit before retrying on error
                
    except Exception as e:
        logger.error(f"Error setting up table: {str(e)}")
        raise

if __name__ == "__main__":
    logger.info("Starting continuous data loader...")
    client, options = connect_with_retry()
    continuous_load(client, options)
