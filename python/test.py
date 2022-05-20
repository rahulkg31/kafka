from consumer import Consumer
from producer import Producer

config = {
    "PRODUCER": {
        "BOOTSTRAP_SERVERS": [
            "localhost:9092"
        ],
        "TOPIC": "test",
        "REQUEST_TIMEOUT_MS": 60000,
        "IS_SSL": False,
        "SSL_CA": "",
        "SSL_CLIENT_CERT": "",
        "SSL_CLIENT_KEY": ""
    },
    "CONSUMER": {
        "BOOTSTRAP_SERVERS": [
            "localhost:9092"
        ],
        "TOPIC": "test",
        "GROUP_ID": "test",
        "AUTO_OFFSET_RESET": "earliest",
        "POLL_TIMEOUT_MS": 60000,
        "CONSUMER_TIMEOUT_MS": 60000,
        "MAX_POLL_INTERVAL_MS": 300000,
        "SESSION_TIMEOUT_MS": 30000,
        "MAX_POLL_RECORDS": 1000,
        "IS_SSL": False,
        "SSL_CA": "",
        "SSL_CLIENT_CERT": "",
        "SSL_CLIENT_KEY": ""
    }
}

producer = Producer(config)
consumer = Consumer(config)

# consumer
consumer.start_kafka_listener()

# producer
msg = {"name": "rahul"}
producer.publish(msg)