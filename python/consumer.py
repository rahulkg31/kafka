import time
import logging
from kafka import KafkaConsumer


class Consumer:
    """
    Kafka consumer
    """
    consumer = None
    topic = None
    auto_offset_reset = "earliest"
    poll_timeout_ms = 60000

    def __init__(self, config_data):
        if not config_data["CONSUMER"]["BOOTSTRAP_SERVERS"]:
            logging.exception("Error - bootstrap_servers can't be null")
            raise Exception("Error - bootstrap_servers can't be null")

        if not config_data["CONSUMER"]["TOPIC"] or config_data["CONSUMER"]["TOPIC"] == "":
            logging.exception("Error - consumer topic can't be null or empty")
            raise Exception("Error - consumer topic can't be null or empty")

        if not config_data["CONSUMER"]["GROUP_ID"] or config_data["CONSUMER"]["GROUP_ID"] == "":
            logging.exception("Error - group_id can't be null or empty")
            raise Exception("Error - group_id can't be null or empty")

        self.topic = config_data["CONSUMER"]["TOPIC"]
        self.auto_offset_reset = config_data["CONSUMER"]["AUTO_OFFSET_RESET"]
        self.poll_timeout_ms = config_data["CONSUMER"]["POLL_TIMEOUT_MS"]

        try:
            if config_data["CONSUMER"]["IS_SSL"]:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=config_data["CONSUMER"]["BOOTSTRAP_SERVERS"],
                    group_id=config_data["CONSUMER"]["GROUP_ID"],
                    security_protocol='SSL',
                    ssl_check_hostname=True,
                    ssl_cafile=config_data["CONSUMER"]["SSL_CA"],
                    ssl_certfile=config_data["CONSUMER"]["SSL_CLIENT_CERT"],
                    ssl_keyfile=config_data["CONSUMER"]["SSL_CLIENT_KEY"],
                    consumer_timeout_ms=config_data["CONSUMER"]["CONSUMER_TIMEOUT_MS"],
                    auto_offset_reset=config_data["CONSUMER"]["AUTO_OFFSET_RESET"],
                    enable_auto_commit=True,
                    max_poll_interval_ms=config_data["CONSUMER"]["MAX_POLL_INTERVAL_MS"],
                    session_timeout_ms=config_data["CONSUMER"]["SESSION_TIMEOUT_MS"],
                    max_poll_records=config_data["CONSUMER"]["MAX_POLL_RECORDS"]
                )
            else:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=config_data["CONSUMER"]["BOOTSTRAP_SERVERS"],
                    auto_offset_reset=self.auto_offset_reset,
                    group_id=config_data["CONSUMER"]["GROUP_ID"],
                )
            logging.info("kafka consumer created successfully")
        except Exception as e:
            logging.exception("Error creating kafka consumer " + str(e))

    def start_kafka_listener(self):
        """
        Start kafka consumer listener
        Returns
        -------
        """
        while True:
            try:
                data = self.consumer.poll(0)
                if data:
                    for topic_partition, messages in data.items():
                        for msg in messages:
                            msg = msg.value.decode('utf-8')
                            print("Message received: " + msg)
            except Exception as exc:
                logging.exception("Exception during polling - " + str(exc))

            time.sleep(0.05)

