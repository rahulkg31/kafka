import json
from kafka import KafkaProducer
import logging

class Producer:
    """
    Kafka producer
    """
    producer = None
    topic = None

    def __init__(self, config_data):
        if not config_data["PRODUCER"]["BOOTSTRAP_SERVERS"]:
            raise Exception("Error - bootstrap_servers can't be null")

        if not config_data["PRODUCER"]["TOPIC"] or config_data["PRODUCER"]["TOPIC"] == "":
            raise Exception("Error - producer topic can't be null or empty")

        self.topic = config_data["PRODUCER"]["TOPIC"]

        try:
            if config_data["PRODUCER"]["IS_SSL"]:
                self.producer = KafkaProducer(bootstrap_servers=config_data["PRODUCER"]["BOOTSTRAP_SERVERS"],
                                              value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                                              security_protocol='SSL',
                                              ssl_check_hostname=True,
                                              request_timeout_ms=config_data["PRODUCER"]["REQUEST_TIMEOUT_MS"],
                                              ssl_cafile=config_data["PRODUCER"]["SSL_CA"],
                                              ssl_certfile=config_data["PRODUCER"]["SSL_CLIENT_CERT"],
                                              ssl_keyfile=config_data["PRODUCER"]["SSL_CLIENT_KEY"]
                                              )
            else:
                self.producer = KafkaProducer(bootstrap_servers=config_data["PRODUCER"]["BOOTSTRAP_SERVERS"],
                                              value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                                              request_timeout_ms=config_data["PRODUCER"]["REQUEST_TIMEOUT_MS"])
        except Exception as e:
            logging.exception("Error creating kafka producer:"+str(e))

    def publish(self, message):
        """
        Publish message dict to a kafka topic
        Parameters
        ----------
        message - dict
        Returns
        -------
        """
        self.producer.send(self.topic, value=message).add_callback(self.success).add_errback(self.error)
        self.producer.flush()

    def success(self, metadata):
        """
        Success callback
        Parameters
        ----------
        metadata
        Returns
        -------
        """
        logging.info("Message published on topic :" + metadata.topic)
        print("Message published on topic:", metadata.topic)

    def error(self, exception):
        """
        Error callback
        Parameters
        ----------
        exception
        Returns
        -------
        """
        print("Error while publishing the message :", exception)