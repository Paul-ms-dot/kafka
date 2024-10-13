from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')  # Serialize data as JSON
)

topic = 'test-topic'
message = {'key': 'value'}
producer.send(topic, value=message)

print(f"Message sent to topic '{topic}': {message}")

producer.flush()
producer.close()
