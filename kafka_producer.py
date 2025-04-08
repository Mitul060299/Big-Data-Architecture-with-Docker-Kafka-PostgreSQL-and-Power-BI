import random
import json
from kafka import KafkaProducer
from faker import Faker
import time
from datetime import datetime

# Initialize Faker for generating fake names and messages
fake = Faker()

# Define locations
locations = ["America", "India", "Ireland", "Africa"]

# Define Kafka topic and server
topic = "flink_topic"
bootstrap_servers = ['localhost:9092']

# Kafka producer initialization
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serializes the message as JSON
)

# Initialize a sequential ID
message_id = 1

# Function to generate a random message
def generate_random_message():
    global message_id
    sender = fake.name()  # Generate random sender name
    location = random.choice(locations)  # Random location
    message = fake.sentence()  # Generate a random message
    timestamp = datetime.now().isoformat()  # Current timestamp in ISO 8601 format

    # Create the message payload
    message_payload = {
        'id': message_id,  # Sequential ID
        'time': timestamp,  # Current time
        'sender': sender,  # Sender name
        'location': location,  # Location
        'message': message  # Random message
    }
    message_id += 1  # Increment the ID for the next message
    return message_payload

# Function to send a message to Kafka
def send_message():
    message = generate_random_message()  # Generate a random message
    producer.send(topic, value=message)  # Send the message to Kafka topic
    print(f"Sent message: {message}")

# Run the producer to generate only 30 messages
if __name__ == "__main__":
    try:
        for _ in range(30):  # Limit to 30 messages
            send_message()  # Send a random message
            time.sleep(2)  # Wait for 2 seconds before sending another message
    except KeyboardInterrupt:
        print("Kafka Producer stopped.")
    finally:
        producer.close()  # Close the producer connection gracefully
