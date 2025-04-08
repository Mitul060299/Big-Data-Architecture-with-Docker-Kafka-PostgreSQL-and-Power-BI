from kafka import KafkaConsumer
import psycopg2
import json

# Kafka Consumer Configuration
KAFKA_TOPIC = 'flink_topic'
KAFKA_BROKER = 'localhost:9092'

# PostgreSQL Connection Configuration
DB_HOST = 'localhost'
DB_NAME = 'MSdb'
DB_USER = 'postgres'
DB_PASS = 'password'

def consume_and_store():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()
from kafka import KafkaConsumer
import psycopg2
import json

# Kafka Consumer Configuration
KAFKA_TOPIC = 'flink_topic'
KAFKA_BROKER = 'localhost:9092'

# PostgreSQL Connection Configuration
DB_HOST = 'localhost'
DB_NAME = 'MSdb'  # Use MSdb as the database name
DB_USER = 'postgres'
DB_PASS = 'password'

def consume_and_store():
    # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )
    cursor = conn.cursor()

    # Create a table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id SERIAL PRIMARY KEY,   -- Use SERIAL for auto-incrementing ID
        sender TEXT,
        location TEXT,
        message TEXT,
        time TIMESTAMP          -- Store the timestamp as a PostgreSQL TIMESTAMP
    )
    """)
    conn.commit()

    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Consuming from Kafka topic: {KAFKA_TOPIC}")

    for message in consumer:
        data = message.value
        print(f"Received: {data}")

        # Insert data into PostgreSQL
        cursor.execute("""
        INSERT INTO messages (sender, location, message, time)
        VALUES (%s, %s, %s, %s)
        """, (data['sender'], data['location'], data['message'], data['time']))
        conn.commit()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    consume_and_store()

    # Create a table if it doesn't exist
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS messages (
        id TEXT,
        sender TEXT,
        location TEXT,
        message TEXT
    )
    """)
    conn.commit()

    # Create Kafka consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    print(f"Consuming from Kafka topic: {KAFKA_TOPIC}")

    for message in consumer:
        data = message.value
        print(f"Received: {data}")

        # Insert data into PostgreSQL
        cursor.execute("""
        INSERT INTO messages (id, sender, location, message)
        VALUES (%s, %s, %s, %s)
        """, (data['id'],data['sender'], data['location'], data['message']))
        conn.commit()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    consume_and_store()
