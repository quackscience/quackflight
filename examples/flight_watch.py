import pyarrow as pa
import pyarrow.flight as flight
import time
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('flight_monitor')

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

def monitor_table(client, options):
    """Monitor the concurrent_test table"""
    table_name = "concurrent_test"
    
    try:
        while True:
            try:
                # Get total count
                count_sql = f"SELECT COUNT(*) as total FROM {table_name}"
                ticket = flight.Ticket(count_sql.encode())
                reader = client.do_get(ticket, options)
                table = reader.read_all().to_pandas()
                total_count = table['total'][0]
                
                # Get latest sample
                sample_sql = f"""
                    SELECT * FROM {table_name} 
                    ORDER BY RANDOM() 
                    LIMIT 1
                """
                ticket = flight.Ticket(sample_sql.encode())
                reader = client.do_get(ticket, options)
                sample = reader.read_all().to_pandas()
                
                logger.info(f"Total rows: {total_count}")
                logger.info(f"Sample row:\n{sample.to_string()}")
                logger.info("-" * 50)
                
                time.sleep(5)  # Check every 5 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring table: {str(e)}")
                time.sleep(1)  # Wait a bit before retrying on error
                
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
    except Exception as e:
        logger.error(f"Fatal error in monitoring: {str(e)}")
        raise

if __name__ == "__main__":
    logger.info("Starting table monitor...")
    client, options = connect_with_retry()
    monitor_table(client, options)
