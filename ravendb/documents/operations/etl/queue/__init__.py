from enum import Enum

from ravendb.documents.operations.connection_strings import ConnectionString
import ravendb.serverwide.server_operation_executor
from ravendb.documents.operations.etl.queue.amazon_sqs_connection_settings import AmazonSqsConnectionSettings
from ravendb.documents.operations.etl.queue.azure_queue_storage_connection_settings import \
    AzureQueueStorageConnectionSettings
from ravendb.documents.operations.etl.queue.kafka_connection_settings import KafkaConnectionSettings
from ravendb.documents.operations.etl.queue.rabbit_mq_connection_settings import RabbitMqConnectionSettings


class QueueBrokerType(Enum):
    NONE = "None"
    KAFKA = "Kafka"
    RABBIT_MQ = "RabbitMq"
    AZURE_QUEUE_STORAGE = "AzureQueueStorage"
    AMAZON_SQS = "AmazonSqs"


class QueueConnectionString(ConnectionString):
    def __init__(
        self,
        name: str = None,
        broker_type: QueueBrokerType = None,
        kafka_settings: KafkaConnectionSettings = None,
        rabbit_mq_settings: RabbitMqConnectionSettings = None,
        azure_queue_storage_settings: AzureQueueStorageConnectionSettings = None,
        amazon_sqs_settings: AmazonSqsConnectionSettings = None,
    ):
        super().__init__()
        self.name = name
        self.broker_type = broker_type
        self.kafka_settings = kafka_settings
        self.rabbit_mq_settings = rabbit_mq_settings
        self.azure_queue_storage_settings = azure_queue_storage_settings
        self.amazon_sqs_settings = amazon_sqs_settings

    def get_type(self):
        return ravendb.serverwide.server_operation_executor.ConnectionStringType.QUEUE.value

    def to_json(self):
        return {
            "Name": self.name,
            "BrokerType": self.broker_type,
            "KafkaConnectionSettings": self.kafka_settings,
            "RabbitMqConnectionSettings": self.rabbit_mq_settings,
            "AzureQueueStorageConnectionSettings": self.azure_queue_storage_settings,
            "AmazonSqsConnectionSettings": self.amazon_sqs_settings,
            "Type": ravendb.serverwide.server_operation_executor.ConnectionStringType.QUEUE,
        }

    @classmethod
    def from_json(cls, json_dict: dict) -> "QueueConnectionString":
        return cls(
            name=json_dict["Name"],
            broker_type=QueueBrokerType(json_dict["BrokerType"]),
            kafka_settings=KafkaConnectionSettings.from_json(json_dict["KafkaConnectionSettings"]),
            rabbit_mq_settings=RabbitMqConnectionSettings.from_json(json_dict["RabbitMqConnectionSettings"]),
            azure_queue_storage_settings=AzureQueueStorageConnectionSettings.from_json(json_dict["AzureQueueStorageConnectionSettings"]),
            amazon_sqs_settings=AmazonSqsConnectionSettings.from_json(json_dict["AmazonSqsConnectionSettings"])
        )
