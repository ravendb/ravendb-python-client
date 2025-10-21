from ravendb import FtpSettings
from ravendb.documents.operations.ai.ai_connection_string import AiConnectionString, AiModelType
from ravendb.documents.operations.ai.azure_open_ai_settings import AzureOpenAiSettings
from ravendb.documents.operations.ai.embedded_settings import EmbeddedSettings
from ravendb.documents.operations.ai.google_settings import GoogleSettings, GoogleAiVersion
from ravendb.documents.operations.ai.hugging_face_settings import HuggingFaceSettings
from ravendb.documents.operations.ai.mistral_ai_settings import MistralAiSettings
from ravendb.documents.operations.ai.ollama_settings import OllamaSettings
from ravendb.documents.operations.ai.open_ai_settings import OpenAiSettings
from ravendb.documents.operations.backups.settings import (
    LocalSettings,
    S3Settings,
    AzureSettings,
    GlacierSettings,
    GetBackupConfigurationScript,
)
from ravendb.documents.operations.connection_string.get_connection_string_operation import GetConnectionStringsOperation
from ravendb.documents.operations.connection_string.put_connection_string_operation import (
    PutConnectionStringOperation,
)
from ravendb.documents.operations.connection_string.remove_connection_string_operation import (
    RemoveConnectionStringOperation,
)
from ravendb.documents.operations.etl.configuration import RavenConnectionString
from ravendb.documents.operations.etl.elastic_search.connection import (
    ElasticSearchConnectionString,
    Authentication,
    BasicAuthentication,
    ApiKeyAuthentication,
    CertificateAuthentication,
)
from ravendb.documents.operations.etl.olap.connection import OlapConnectionString
from ravendb.documents.operations.etl.queue.connection import QueueConnectionString, QueueBrokerType
from ravendb.documents.operations.etl.queue.amazon_sqs_connection_settings import (
    AmazonSqsConnectionSettings,
    AmazonSqsCredentials,
)
from ravendb.documents.operations.etl.queue.azure_queue_storage_connection_settings import (
    AzureQueueStorageConnectionSettings,
    EntraId,
    Passwordless,
)
from ravendb.documents.operations.etl.queue.kafka_connection_settings import KafkaConnectionSettings
from ravendb.documents.operations.etl.queue.rabbit_mq_connection_settings import RabbitMqConnectionSettings
from ravendb.documents.operations.etl.snowflake.connection import SnowflakeConnectionString
from ravendb.documents.operations.etl.sql import SqlConnectionString
from ravendb.serverwide.server_operation_executor import ConnectionStringType
from ravendb.tests.test_base import TestBase


class TestConnectionString(TestBase):
    def setUp(self):
        super().setUp()

    def test_full_lifecycle_for_all_connection_string_types(self):
        # Initialize all connection string types
        raven_connection_string = RavenConnectionString("raven1", self.store.database, self.store.urls)
        sql_connection_string = SqlConnectionString("sql1", "test", "MySql.Data.MySqlClient")
        olap_connection_string = OlapConnectionString("olap1", ftp_settings=FtpSettings(url=self.store.urls[0]))
        ai_connection_string = AiConnectionString(
            "ai1",
            "test-identifier",
            openai_settings=OpenAiSettings(api_key="test-key", endpoint="https://api.openai.com", model="gpt-4"),
            model_type=AiModelType.CHAT,
        )
        elastic_search_connection_string = ElasticSearchConnectionString("elastic1", nodes=["http://localhost:9200"])
        kafka_settings = KafkaConnectionSettings(bootstrap_servers="localhost:9092")
        queue_connection_string = QueueConnectionString("queue1", QueueBrokerType.KAFKA, kafka_settings=kafka_settings)
        snowflake_connection_string = SnowflakeConnectionString(
            "snowflake1", "account=myaccount;user=myuser;password=mypassword"
        )

        # Test full lifecycle for RavenConnectionString
        # 1. Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(raven_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # 2. Get and Assert
        raven_get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("raven1", ConnectionStringType.RAVEN)
        )
        self.assertIn("raven1", raven_get_result.raven_connection_strings)
        self.assertEqual(1, len(raven_get_result.raven_connection_strings))

        # 3. Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("raven1", ConnectionStringType.RAVEN)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # 4. Get and Assert None
        after_delete = self.store.maintenance.send(GetConnectionStringsOperation("raven1", ConnectionStringType.RAVEN))
        self.assertIsNone(after_delete.raven_connection_strings)

        # Test full lifecycle for SqlConnectionString
        # 1. Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(sql_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # 2. Get and Assert
        sql_get_result = self.store.maintenance.send(GetConnectionStringsOperation("sql1", ConnectionStringType.SQL))
        self.assertIn("sql1", sql_get_result.sql_connection_strings)
        self.assertEqual(1, len(sql_get_result.sql_connection_strings))

        # 3. Delete
        remove_result = self.store.maintenance.send(RemoveConnectionStringOperation("sql1", ConnectionStringType.SQL))
        self.assertGreater(remove_result.raft_command_index, 0)

        # 4. Get and Assert None
        after_delete = self.store.maintenance.send(GetConnectionStringsOperation("sql1", ConnectionStringType.SQL))
        self.assertIsNone(after_delete.sql_connection_strings)

        # Test full lifecycle for OlapConnectionString
        # 1. Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(olap_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # 2. Get and Assert
        olap_get_result = self.store.maintenance.send(GetConnectionStringsOperation("olap1", ConnectionStringType.OLAP))
        self.assertIn("olap1", olap_get_result.olap_connection_strings)
        self.assertEqual(1, len(olap_get_result.olap_connection_strings))

        # 3. Delete
        remove_result = self.store.maintenance.send(RemoveConnectionStringOperation("olap1", ConnectionStringType.OLAP))
        self.assertGreater(remove_result.raft_command_index, 0)

        # 4. Get and Assert None
        after_delete = self.store.maintenance.send(GetConnectionStringsOperation("olap1", ConnectionStringType.OLAP))
        self.assertIsNone(after_delete.olap_connection_strings)
        # Test full lifecycle for ElasticSearchConnectionString
        # 1. Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(elastic_search_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # 2. Get and Assert
        elastic_get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("elastic1", ConnectionStringType.ELASTIC_SEARCH)
        )
        self.assertIn("elastic1", elastic_get_result.elastic_search_connection_strings)
        self.assertEqual(1, len(elastic_get_result.elastic_search_connection_strings))

        # 3. Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("elastic1", ConnectionStringType.ELASTIC_SEARCH)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # 4. Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("elastic1", ConnectionStringType.ELASTIC_SEARCH)
        )
        self.assertIsNone(after_delete.elastic_search_connection_strings)

        # Test full lifecycle for QueueConnectionString
        # 1. Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(queue_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # 2. Get and Assert
        queue_get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("queue1", ConnectionStringType.QUEUE)
        )
        self.assertIn("queue1", queue_get_result.queue_connection_strings)
        self.assertEqual(1, len(queue_get_result.queue_connection_strings))

        # 3. Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("queue1", ConnectionStringType.QUEUE)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # 4. Get and Assert None
        after_delete = self.store.maintenance.send(GetConnectionStringsOperation("queue1", ConnectionStringType.QUEUE))
        self.assertIsNone(after_delete.queue_connection_strings)

        # Test full lifecycle for SnowflakeConnectionString
        # 1. Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(snowflake_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # 2. Get and Assert
        snowflake_get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("snowflake1", ConnectionStringType.SNOWFLAKE)
        )
        self.assertIn("snowflake1", snowflake_get_result.snowflake_connection_strings)
        self.assertEqual(1, len(snowflake_get_result.snowflake_connection_strings))

        # 3. Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("snowflake1", ConnectionStringType.SNOWFLAKE)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # 4. Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("snowflake1", ConnectionStringType.SNOWFLAKE)
        )
        self.assertIsNone(after_delete.snowflake_connection_strings)

        # Test full lifecycle for AiConnectionString
        # 1. Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(ai_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # 2. Get and Assert
        ai_get_result = self.store.maintenance.send(GetConnectionStringsOperation("ai1", ConnectionStringType.AI))
        self.assertIn("ai1", ai_get_result.ai_connection_strings)
        self.assertEqual(1, len(ai_get_result.ai_connection_strings))

        # 3. Delete
        remove_result = self.store.maintenance.send(RemoveConnectionStringOperation("ai1", ConnectionStringType.AI))
        self.assertGreater(remove_result.raft_command_index, 0)

        # 4. Get and Assert None
        after_delete = self.store.maintenance.send(GetConnectionStringsOperation("ai1", ConnectionStringType.AI))
        self.assertIsNone(after_delete.ai_connection_strings)

    def test_all_possible_fields_for_each_connection_string_type(self):
        # Test RavenConnectionString with all possible fields
        raven_connection_string = RavenConnectionString(
            name="raven_all_fields", database=self.store.database, topology_discovery_urls=self.store.urls
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(raven_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("raven_all_fields", ConnectionStringType.RAVEN)
        )
        self.assertIn("raven_all_fields", get_result.raven_connection_strings)
        retrieved = get_result.raven_connection_strings["raven_all_fields"]
        # Assert that all fields remain unchanged
        self.assertEqual(raven_connection_string.name, retrieved.name)
        self.assertEqual(raven_connection_string.database, retrieved.database)
        self.assertEqual(raven_connection_string.topology_discovery_urls, retrieved.topology_discovery_urls)

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("raven_all_fields", ConnectionStringType.RAVEN)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("raven_all_fields", ConnectionStringType.RAVEN)
        )
        self.assertIsNone(after_delete.raven_connection_strings)

        # Test SqlConnectionString with all possible fields
        sql_connection_string = SqlConnectionString(
            name="sql_all_fields",
            connection_string="Server=myServerAddress;Database=myDataBase;User Id=myUsername;Password=myPassword;",
            factory_name="System.Data.SqlClient",
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(sql_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("sql_all_fields", ConnectionStringType.SQL)
        )
        self.assertIn("sql_all_fields", get_result.sql_connection_strings)
        retrieved = get_result.sql_connection_strings["sql_all_fields"]
        # Assert that all fields remain unchanged
        self.assertEqual(sql_connection_string.name, retrieved.name)
        self.assertEqual(sql_connection_string.connection_string, retrieved.connection_string)
        self.assertEqual(sql_connection_string.factory_name, retrieved.factory_name)

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("sql_all_fields", ConnectionStringType.SQL)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("sql_all_fields", ConnectionStringType.SQL)
        )
        self.assertIsNone(after_delete.sql_connection_strings)

        # Test OlapConnectionString with all possible fields
        backup_script = GetBackupConfigurationScript(exec="test.exe", arguments="--test", timeout_in_ms=5000)
        local_settings = LocalSettings(
            disabled=False, get_backup_configuration_script=backup_script, folder_path="/path/to/folder"
        )
        s3_settings = S3Settings(
            disabled=False,
            get_backup_configuration_script=backup_script,
            aws_access_key="access_key",
            aws_secret_key="secret_key",
            aws_session_token="session_token",
            aws_region_name="us-east-1",
            remote_folder_name="remote_folder",
            bucket_name="bucket",
            custom_server_url="https://s3.custom.com",
            force_path_style=True,
        )
        azure_settings = AzureSettings(
            disabled=False,
            get_backup_configuration_script=backup_script,
            storage_container="container",
            remote_folder_name="remote_folder",
            account_name="account",
            account_key="key",
            sas_token="sas_token",
        )
        glacier_settings = GlacierSettings(
            disabled=False,
            get_backup_configuration_script=backup_script,
            aws_access_key="access_key",
            aws_secret_key="secret_key",
            aws_session_token="session_token",
            aws_region_name="us-east-1",
            remote_folder_name="remote_folder",
            vault_name="vault",
        )
        ftp_settings = FtpSettings(
            disabled=False,
            get_backup_configuration_script=backup_script,
            url="ftp://example.com",
            user_name="user",
            password="password",
            certificate_as_base64="base64cert",
        )

        olap_connection_string = OlapConnectionString(
            name="olap_all_fields",
            local_settings=local_settings,
            s3_settings=s3_settings,
            azure_settings=azure_settings,
            glacier_settings=glacier_settings,
            ftp_settings=ftp_settings,
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(olap_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("olap_all_fields", ConnectionStringType.OLAP)
        )
        self.assertIn("olap_all_fields", get_result.olap_connection_strings)
        retrieved = get_result.olap_connection_strings["olap_all_fields"]
        # Assert that all fields remain unchanged
        self.assertEqual(olap_connection_string.name, retrieved.name)

        # Verify local_settings
        self.assertEqual(olap_connection_string.local_settings.disabled, retrieved.local_settings.disabled)
        self.assertEqual(olap_connection_string.local_settings.folder_path, retrieved.local_settings.folder_path)
        self.assertEqual(
            olap_connection_string.local_settings.get_backup_configuration_script.exec,
            retrieved.local_settings.get_backup_configuration_script.exec,
        )
        self.assertEqual(
            olap_connection_string.local_settings.get_backup_configuration_script.arguments,
            retrieved.local_settings.get_backup_configuration_script.arguments,
        )
        self.assertEqual(
            olap_connection_string.local_settings.get_backup_configuration_script.timeout_in_ms,
            retrieved.local_settings.get_backup_configuration_script.timeout_in_ms,
        )

        # Verify s3_settings
        self.assertEqual(olap_connection_string.s3_settings.disabled, retrieved.s3_settings.disabled)
        self.assertEqual(olap_connection_string.s3_settings.aws_access_key, retrieved.s3_settings.aws_access_key)
        self.assertEqual(olap_connection_string.s3_settings.aws_secret_key, retrieved.s3_settings.aws_secret_key)
        self.assertEqual(olap_connection_string.s3_settings.aws_session_token, retrieved.s3_settings.aws_session_token)
        self.assertEqual(olap_connection_string.s3_settings.aws_region_name, retrieved.s3_settings.aws_region_name)
        self.assertEqual(
            olap_connection_string.s3_settings.remote_folder_name, retrieved.s3_settings.remote_folder_name
        )
        self.assertEqual(olap_connection_string.s3_settings.bucket_name, retrieved.s3_settings.bucket_name)
        self.assertEqual(olap_connection_string.s3_settings.custom_server_url, retrieved.s3_settings.custom_server_url)
        self.assertEqual(olap_connection_string.s3_settings.force_path_style, retrieved.s3_settings.force_path_style)
        self.assertEqual(
            olap_connection_string.s3_settings.get_backup_configuration_script.exec,
            retrieved.s3_settings.get_backup_configuration_script.exec,
        )
        self.assertEqual(
            olap_connection_string.s3_settings.get_backup_configuration_script.arguments,
            retrieved.s3_settings.get_backup_configuration_script.arguments,
        )
        self.assertEqual(
            olap_connection_string.s3_settings.get_backup_configuration_script.timeout_in_ms,
            retrieved.s3_settings.get_backup_configuration_script.timeout_in_ms,
        )

        # Verify azure_settings
        self.assertEqual(olap_connection_string.azure_settings.disabled, retrieved.azure_settings.disabled)
        self.assertEqual(
            olap_connection_string.azure_settings.storage_container, retrieved.azure_settings.storage_container
        )
        self.assertEqual(
            olap_connection_string.azure_settings.remote_folder_name, retrieved.azure_settings.remote_folder_name
        )
        self.assertEqual(olap_connection_string.azure_settings.account_name, retrieved.azure_settings.account_name)
        self.assertEqual(olap_connection_string.azure_settings.account_key, retrieved.azure_settings.account_key)
        self.assertEqual(olap_connection_string.azure_settings.sas_token, retrieved.azure_settings.sas_token)
        self.assertEqual(
            olap_connection_string.azure_settings.get_backup_configuration_script.exec,
            retrieved.azure_settings.get_backup_configuration_script.exec,
        )
        self.assertEqual(
            olap_connection_string.azure_settings.get_backup_configuration_script.arguments,
            retrieved.azure_settings.get_backup_configuration_script.arguments,
        )
        self.assertEqual(
            olap_connection_string.azure_settings.get_backup_configuration_script.timeout_in_ms,
            retrieved.azure_settings.get_backup_configuration_script.timeout_in_ms,
        )

        # Verify glacier_settings
        self.assertEqual(olap_connection_string.glacier_settings.disabled, retrieved.glacier_settings.disabled)
        self.assertEqual(
            olap_connection_string.glacier_settings.aws_access_key, retrieved.glacier_settings.aws_access_key
        )
        self.assertEqual(
            olap_connection_string.glacier_settings.aws_secret_key, retrieved.glacier_settings.aws_secret_key
        )
        self.assertEqual(
            olap_connection_string.glacier_settings.aws_session_token, retrieved.glacier_settings.aws_session_token
        )
        self.assertEqual(
            olap_connection_string.glacier_settings.aws_region_name, retrieved.glacier_settings.aws_region_name
        )
        self.assertEqual(
            olap_connection_string.glacier_settings.remote_folder_name, retrieved.glacier_settings.remote_folder_name
        )
        self.assertEqual(olap_connection_string.glacier_settings.vault_name, retrieved.glacier_settings.vault_name)
        self.assertEqual(
            olap_connection_string.glacier_settings.get_backup_configuration_script.exec,
            retrieved.glacier_settings.get_backup_configuration_script.exec,
        )
        self.assertEqual(
            olap_connection_string.glacier_settings.get_backup_configuration_script.arguments,
            retrieved.glacier_settings.get_backup_configuration_script.arguments,
        )
        self.assertEqual(
            olap_connection_string.glacier_settings.get_backup_configuration_script.timeout_in_ms,
            retrieved.glacier_settings.get_backup_configuration_script.timeout_in_ms,
        )

        # Verify ftp_settings
        self.assertEqual(olap_connection_string.ftp_settings.disabled, retrieved.ftp_settings.disabled)
        self.assertEqual(olap_connection_string.ftp_settings.url, retrieved.ftp_settings.url)
        self.assertEqual(olap_connection_string.ftp_settings.user_name, retrieved.ftp_settings.user_name)
        self.assertEqual(olap_connection_string.ftp_settings.password, retrieved.ftp_settings.password)
        self.assertEqual(
            olap_connection_string.ftp_settings.certificate_as_base64, retrieved.ftp_settings.certificate_as_base64
        )
        self.assertEqual(
            olap_connection_string.ftp_settings.get_backup_configuration_script.exec,
            retrieved.ftp_settings.get_backup_configuration_script.exec,
        )
        self.assertEqual(
            olap_connection_string.ftp_settings.get_backup_configuration_script.arguments,
            retrieved.ftp_settings.get_backup_configuration_script.arguments,
        )
        self.assertEqual(
            olap_connection_string.ftp_settings.get_backup_configuration_script.timeout_in_ms,
            retrieved.ftp_settings.get_backup_configuration_script.timeout_in_ms,
        )

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("olap_all_fields", ConnectionStringType.OLAP)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("olap_all_fields", ConnectionStringType.OLAP)
        )
        self.assertIsNone(after_delete.olap_connection_strings)

        # Test AiConnectionString with each possible setting type separately

        # 1. Test with OpenAI settings - CHAT model type (without temperature and embeddings_max_concurrent_batches)
        openai_chat_settings = OpenAiSettings(
            api_key="openai_key",
            endpoint="https://api.openai.com",
            model="gpt-4",
            organization_id="org_id",
            project_id="proj_id",
            dimensions=1536,
        )

        ai_openai_chat_connection_string = AiConnectionString(
            name="ai_openai_chat",
            identifier="test-ai-openai-chat",
            openai_settings=openai_chat_settings,
            model_type=AiModelType.CHAT,
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(ai_openai_chat_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("ai_openai_chat", ConnectionStringType.AI)
        )
        self.assertIn("ai_openai_chat", get_result.ai_connection_strings)
        retrieved = get_result.ai_connection_strings["ai_openai_chat"]
        self.assertEqual(ai_openai_chat_connection_string.name, retrieved.name)
        self.assertEqual(ai_openai_chat_connection_string.identifier, retrieved.identifier)
        self.assertIsNotNone(retrieved.openai_settings)
        self.assertEqual(openai_chat_settings.api_key, retrieved.openai_settings.api_key)
        self.assertEqual(openai_chat_settings.endpoint, retrieved.openai_settings.endpoint)
        self.assertEqual(openai_chat_settings.model, retrieved.openai_settings.model)
        self.assertEqual(openai_chat_settings.organization_id, retrieved.openai_settings.organization_id)
        self.assertEqual(openai_chat_settings.project_id, retrieved.openai_settings.project_id)
        self.assertEqual(openai_chat_settings.dimensions, retrieved.openai_settings.dimensions)

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("ai_openai_chat", ConnectionStringType.AI)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("ai_openai_chat", ConnectionStringType.AI)
        )
        self.assertIsNone(after_delete.ai_connection_strings)

        # 1.1 Test with OpenAI settings - TEXT_EMBEDDINGS model type (with temperature and embeddings_max_concurrent_batches)
        openai_embeddings_settings = OpenAiSettings(
            api_key="openai_key",
            endpoint="https://api.openai.com",
            model="text-embedding-ada-002",
            organization_id="org_id",
            project_id="proj_id",
            dimensions=1536,
            temperature=0.7,
        )
        openai_embeddings_settings.embeddings_max_concurrent_batches = 5

        ai_openai_embeddings_connection_string = AiConnectionString(
            name="ai_openai_embeddings",
            identifier="test-ai-openai-embeddings",
            openai_settings=openai_embeddings_settings,
            model_type=AiModelType.TEXT_EMBEDDINGS,
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(ai_openai_embeddings_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("ai_openai_embeddings", ConnectionStringType.AI)
        )
        self.assertIn("ai_openai_embeddings", get_result.ai_connection_strings)
        retrieved = get_result.ai_connection_strings["ai_openai_embeddings"]
        self.assertEqual(ai_openai_embeddings_connection_string.name, retrieved.name)
        self.assertEqual(ai_openai_embeddings_connection_string.identifier, retrieved.identifier)
        self.assertIsNotNone(retrieved.openai_settings)
        self.assertEqual(openai_embeddings_settings.api_key, retrieved.openai_settings.api_key)
        self.assertEqual(openai_embeddings_settings.endpoint, retrieved.openai_settings.endpoint)
        self.assertEqual(openai_embeddings_settings.model, retrieved.openai_settings.model)
        self.assertEqual(openai_embeddings_settings.organization_id, retrieved.openai_settings.organization_id)
        self.assertEqual(openai_embeddings_settings.project_id, retrieved.openai_settings.project_id)
        self.assertEqual(openai_embeddings_settings.dimensions, retrieved.openai_settings.dimensions)

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("ai_openai_embeddings", ConnectionStringType.AI)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("ai_openai_embeddings", ConnectionStringType.AI)
        )
        self.assertIsNone(after_delete.ai_connection_strings)

        # 2. Test with Azure OpenAI settings
        azure_openai_settings = AzureOpenAiSettings(
            api_key="azure_key",
            endpoint="https://azure-openai.com",
            model="gpt-4",
            deployment_name="deployment1",
            dimensions=1536,
            temperature=0.7,
        )
        azure_openai_settings.embeddings_max_concurrent_batches = 5

        ai_azure_openai_connection_string = AiConnectionString(
            name="ai_azure_openai",
            identifier="test-ai-azure-openai",
            azure_openai_settings=azure_openai_settings,
            model_type=AiModelType.CHAT,
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(ai_azure_openai_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("ai_azure_openai", ConnectionStringType.AI)
        )
        self.assertIn("ai_azure_openai", get_result.ai_connection_strings)
        retrieved = get_result.ai_connection_strings["ai_azure_openai"]
        self.assertEqual(ai_azure_openai_connection_string.name, retrieved.name)
        self.assertEqual(ai_azure_openai_connection_string.identifier, retrieved.identifier)
        self.assertIsNotNone(retrieved.azure_openai_settings)
        self.assertEqual(azure_openai_settings.api_key, retrieved.azure_openai_settings.api_key)
        self.assertEqual(azure_openai_settings.endpoint, retrieved.azure_openai_settings.endpoint)
        self.assertEqual(azure_openai_settings.model, retrieved.azure_openai_settings.model)
        self.assertEqual(azure_openai_settings.deployment_name, retrieved.azure_openai_settings.deployment_name)
        self.assertEqual(azure_openai_settings.dimensions, retrieved.azure_openai_settings.dimensions)

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("ai_azure_openai", ConnectionStringType.AI)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("ai_azure_openai", ConnectionStringType.AI)
        )
        self.assertIsNone(after_delete.ai_connection_strings)

        # 3. Test with Ollama settings
        ollama_settings = OllamaSettings(
            uri="http://localhost:11434",
            model="llama2",
            think=True,
            temperature=0.8,
            embeddings_max_concurrent_batches=5,
        )
        ollama_settings.think = True
        ollama_settings.temperature = 0.8
        ollama_settings.embeddings_max_concurrent_batches = 5

        ai_ollama_connection_string = AiConnectionString(
            name="ai_ollama", identifier="test-ai-ollama", ollama_settings=ollama_settings, model_type=AiModelType.CHAT
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(ai_ollama_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(GetConnectionStringsOperation("ai_ollama", ConnectionStringType.AI))
        self.assertIn("ai_ollama", get_result.ai_connection_strings)
        retrieved = get_result.ai_connection_strings["ai_ollama"]
        self.assertEqual(ai_ollama_connection_string.name, retrieved.name)
        self.assertEqual(ai_ollama_connection_string.identifier, retrieved.identifier)
        self.assertIsNotNone(retrieved.ollama_settings)
        self.assertEqual(ollama_settings.uri, retrieved.ollama_settings.uri)
        self.assertEqual(ollama_settings.model, retrieved.ollama_settings.model)
        self.assertEqual(ollama_settings.think, retrieved.ollama_settings.think)

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("ai_ollama", ConnectionStringType.AI)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(GetConnectionStringsOperation("ai_ollama", ConnectionStringType.AI))
        self.assertIsNone(after_delete.ai_connection_strings)

        # 4. Test with Embedded settings
        embedded_settings = EmbeddedSettings()
        embedded_settings.embeddings_max_concurrent_batches = 5

        ai_embedded_connection_string = AiConnectionString(
            name="ai_embedded",
            identifier="test-ai-embedded",
            embedded_settings=embedded_settings,
            model_type=AiModelType.CHAT,
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(ai_embedded_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(GetConnectionStringsOperation("ai_embedded", ConnectionStringType.AI))
        self.assertIn("ai_embedded", get_result.ai_connection_strings)
        retrieved = get_result.ai_connection_strings["ai_embedded"]
        self.assertEqual(ai_embedded_connection_string.name, retrieved.name)
        self.assertEqual(ai_embedded_connection_string.identifier, retrieved.identifier)
        self.assertIsNotNone(retrieved.embedded_settings)

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("ai_embedded", ConnectionStringType.AI)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("ai_embedded", ConnectionStringType.AI)
        )
        self.assertIsNone(after_delete.ai_connection_strings)

        # 5. Test with Google settings
        google_settings = GoogleSettings(
            model="gemini-pro", api_key="google_key", ai_version=GoogleAiVersion.V1, dimensions=768
        )
        google_settings.embeddings_max_concurrent_batches = 5

        ai_google_connection_string = AiConnectionString(
            name="ai_google", identifier="test-ai-google", google_settings=google_settings, model_type=AiModelType.CHAT
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(ai_google_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(GetConnectionStringsOperation("ai_google", ConnectionStringType.AI))
        self.assertIn("ai_google", get_result.ai_connection_strings)
        retrieved = get_result.ai_connection_strings["ai_google"]
        self.assertEqual(ai_google_connection_string.name, retrieved.name)
        self.assertEqual(ai_google_connection_string.identifier, retrieved.identifier)
        self.assertIsNotNone(retrieved.google_settings)
        self.assertEqual(google_settings.model, retrieved.google_settings.model)
        self.assertEqual(google_settings.api_key, retrieved.google_settings.api_key)
        self.assertEqual(google_settings.ai_version, retrieved.google_settings.ai_version)
        self.assertEqual(google_settings.dimensions, retrieved.google_settings.dimensions)

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("ai_google", ConnectionStringType.AI)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(GetConnectionStringsOperation("ai_google", ConnectionStringType.AI))
        self.assertIsNone(after_delete.ai_connection_strings)

        # 6. Test with HuggingFace settings
        huggingface_settings = HuggingFaceSettings(
            api_key="hf_key",
            model="mistral-7b",
            endpoint="https://api-inference.huggingface.co/models/mistralai/Mistral-7B-v0.1",
        )
        huggingface_settings.embeddings_max_concurrent_batches = 5

        ai_huggingface_connection_string = AiConnectionString(
            name="ai_huggingface",
            identifier="test-ai-huggingface",
            huggingface_settings=huggingface_settings,
            model_type=AiModelType.CHAT,
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(ai_huggingface_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("ai_huggingface", ConnectionStringType.AI)
        )
        self.assertIn("ai_huggingface", get_result.ai_connection_strings)
        retrieved = get_result.ai_connection_strings["ai_huggingface"]
        self.assertEqual(ai_huggingface_connection_string.name, retrieved.name)
        self.assertEqual(ai_huggingface_connection_string.identifier, retrieved.identifier)
        self.assertIsNotNone(retrieved.huggingface_settings)
        self.assertEqual(huggingface_settings.api_key, retrieved.huggingface_settings.api_key)
        self.assertEqual(huggingface_settings.model, retrieved.huggingface_settings.model)
        self.assertEqual(huggingface_settings.endpoint, retrieved.huggingface_settings.endpoint)

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("ai_huggingface", ConnectionStringType.AI)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("ai_huggingface", ConnectionStringType.AI)
        )
        self.assertIsNone(after_delete.ai_connection_strings)

        # 7. Test with MistralAI settings
        mistral_ai_settings = MistralAiSettings(
            api_key="mistral_key", model="mistral-medium", endpoint="https://api.mistral.ai"
        )
        mistral_ai_settings.embeddings_max_concurrent_batches = 5

        ai_mistral_connection_string = AiConnectionString(
            name="ai_mistral",
            identifier="test-ai-mistral",
            mistral_ai_settings=mistral_ai_settings,
            model_type=AiModelType.CHAT,
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(ai_mistral_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(GetConnectionStringsOperation("ai_mistral", ConnectionStringType.AI))
        self.assertIn("ai_mistral", get_result.ai_connection_strings)
        retrieved = get_result.ai_connection_strings["ai_mistral"]
        self.assertEqual(ai_mistral_connection_string.name, retrieved.name)
        self.assertEqual(ai_mistral_connection_string.identifier, retrieved.identifier)
        self.assertIsNotNone(retrieved.mistral_ai_settings)
        self.assertEqual(mistral_ai_settings.api_key, retrieved.mistral_ai_settings.api_key)
        self.assertEqual(mistral_ai_settings.model, retrieved.mistral_ai_settings.model)
        self.assertEqual(mistral_ai_settings.endpoint, retrieved.mistral_ai_settings.endpoint)

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("ai_mistral", ConnectionStringType.AI)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(GetConnectionStringsOperation("ai_mistral", ConnectionStringType.AI))
        self.assertIsNone(after_delete.ai_connection_strings)

        # Test ElasticSearchConnectionString with all possible fields
        api_key_auth = ApiKeyAuthentication(
            api_key_id="api_key_id", api_key="api_key", encoded_api_key="encoded_api_key"
        )

        basic_auth = BasicAuthentication(username="elastic", password="password")

        cert_auth = CertificateAuthentication(certificates_base64=["base64cert1", "base64cert2"])

        auth = Authentication(api_key=api_key_auth, basic=basic_auth, certificate=cert_auth)

        elastic_search_connection_string = ElasticSearchConnectionString(
            name="elastic_all_fields", nodes=["http://localhost:9200", "http://localhost:9201"], authentication=auth
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(elastic_search_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("elastic_all_fields", ConnectionStringType.ELASTIC_SEARCH)
        )
        self.assertIn("elastic_all_fields", get_result.elastic_search_connection_strings)
        retrieved = get_result.elastic_search_connection_strings["elastic_all_fields"]
        # Assert that all fields remain unchanged
        self.assertEqual(elastic_search_connection_string.name, retrieved.name)
        self.assertEqual(elastic_search_connection_string.nodes, retrieved.nodes)

        # Verify authentication details
        # API Key Authentication
        self.assertEqual(
            elastic_search_connection_string.authentication.api_key.api_key_id,
            retrieved.authentication.api_key.api_key_id,
        )
        self.assertEqual(
            elastic_search_connection_string.authentication.api_key.api_key, retrieved.authentication.api_key.api_key
        )
        self.assertEqual(
            elastic_search_connection_string.authentication.api_key.encoded_api_key,
            retrieved.authentication.api_key.encoded_api_key,
        )

        # Basic Authentication
        self.assertEqual(
            elastic_search_connection_string.authentication.basic.username, retrieved.authentication.basic.username
        )
        self.assertEqual(
            elastic_search_connection_string.authentication.basic.password, retrieved.authentication.basic.password
        )

        # Certificate Authentication
        self.assertEqual(
            elastic_search_connection_string.authentication.certificate.certificates_base64,
            retrieved.authentication.certificate.certificates_base64,
        )

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("elastic_all_fields", ConnectionStringType.ELASTIC_SEARCH)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("elastic_all_fields", ConnectionStringType.ELASTIC_SEARCH)
        )
        self.assertIsNone(after_delete.elastic_search_connection_strings)

        # Test QueueConnectionString with all possible fields
        kafka_settings = KafkaConnectionSettings(
            bootstrap_servers="localhost:9092",
            connection_options={"acks": "all", "batch.size": "16384"},
            use_raven_certificate=True,
        )

        rabbit_mq_settings = RabbitMqConnectionSettings(connection_string="amqp://guest:guest@localhost:5672/")

        entra_id = EntraId(
            storage_account_name="storage_account",
            tenant_id="tenant_id",
            client_id="client_id",
            client_secret="client_secret",
        )

        passwordless = Passwordless(storage_account_name="storage_account")

        azure_queue_storage_settings = AzureQueueStorageConnectionSettings(
            entra_id=entra_id,
            connection_string="DefaultEndpointsProtocol=https;AccountName=account;AccountKey=key;EndpointSuffix=core.windows.net",
            passwordless=passwordless,
        )

        amazon_sqs_credentials = AmazonSqsCredentials(
            access_key="access_key", secret_key="secret_key", region_name="us-east-1"
        )

        amazon_sqs_settings = AmazonSqsConnectionSettings(
            basic=amazon_sqs_credentials, passwordless=True, use_emulator=True
        )

        queue_connection_string = QueueConnectionString(
            name="queue_all_fields",
            broker_type=QueueBrokerType.KAFKA,
            kafka_settings=kafka_settings,
            rabbit_mq_settings=rabbit_mq_settings,
            azure_queue_storage_settings=azure_queue_storage_settings,
            amazon_sqs_settings=amazon_sqs_settings,
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(queue_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("queue_all_fields", ConnectionStringType.QUEUE)
        )
        self.assertIn("queue_all_fields", get_result.queue_connection_strings)
        retrieved = get_result.queue_connection_strings["queue_all_fields"]
        # Assert that all fields remain unchanged
        self.assertEqual(queue_connection_string.name, retrieved.name)
        self.assertEqual(queue_connection_string.broker_type, retrieved.broker_type)

        # Verify Kafka settings
        self.assertEqual(
            queue_connection_string.kafka_settings.bootstrap_servers, retrieved.kafka_settings.bootstrap_servers
        )
        self.assertEqual(
            queue_connection_string.kafka_settings.connection_options, retrieved.kafka_settings.connection_options
        )
        self.assertEqual(
            queue_connection_string.kafka_settings.use_raven_certificate, retrieved.kafka_settings.use_raven_certificate
        )

        # Verify RabbitMQ settings
        self.assertEqual(
            queue_connection_string.rabbit_mq_settings.connection_string, retrieved.rabbit_mq_settings.connection_string
        )

        # Verify Azure Queue Storage settings
        # EntraId
        self.assertEqual(
            queue_connection_string.azure_queue_storage_settings.entra_id.storage_account_name,
            retrieved.azure_queue_storage_settings.entra_id.storage_account_name,
        )
        self.assertEqual(
            queue_connection_string.azure_queue_storage_settings.entra_id.tenant_id,
            retrieved.azure_queue_storage_settings.entra_id.tenant_id,
        )
        self.assertEqual(
            queue_connection_string.azure_queue_storage_settings.entra_id.client_id,
            retrieved.azure_queue_storage_settings.entra_id.client_id,
        )
        self.assertEqual(
            queue_connection_string.azure_queue_storage_settings.entra_id.client_secret,
            retrieved.azure_queue_storage_settings.entra_id.client_secret,
        )

        # Connection string
        self.assertEqual(
            queue_connection_string.azure_queue_storage_settings.connection_string,
            retrieved.azure_queue_storage_settings.connection_string,
        )

        # Passwordless
        self.assertEqual(
            queue_connection_string.azure_queue_storage_settings.passwordless.storage_account_name,
            retrieved.azure_queue_storage_settings.passwordless.storage_account_name,
        )

        # Verify Amazon SQS settings
        # Basic credentials
        self.assertEqual(
            queue_connection_string.amazon_sqs_settings.basic.access_key, retrieved.amazon_sqs_settings.basic.access_key
        )
        self.assertEqual(
            queue_connection_string.amazon_sqs_settings.basic.secret_key, retrieved.amazon_sqs_settings.basic.secret_key
        )
        self.assertEqual(
            queue_connection_string.amazon_sqs_settings.basic.region_name,
            retrieved.amazon_sqs_settings.basic.region_name,
        )

        # Other settings
        self.assertEqual(
            queue_connection_string.amazon_sqs_settings.passwordless, retrieved.amazon_sqs_settings.passwordless
        )
        self.assertEqual(
            queue_connection_string.amazon_sqs_settings.use_emulator, retrieved.amazon_sqs_settings.use_emulator
        )

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("queue_all_fields", ConnectionStringType.QUEUE)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("queue_all_fields", ConnectionStringType.QUEUE)
        )
        self.assertIsNone(after_delete.queue_connection_strings)

        # Test SnowflakeConnectionString with all possible fields
        snowflake_connection_string = SnowflakeConnectionString(
            name="snowflake_all_fields",
            connection_string="account=myaccount;user=myuser;password=mypassword;warehouse=mywarehouse;database=mydatabase;schema=myschema",
        )

        # Create
        put_result = self.store.maintenance.send(PutConnectionStringOperation(snowflake_connection_string))
        self.assertGreater(put_result.raft_command_index, 0)

        # Get and Assert
        get_result = self.store.maintenance.send(
            GetConnectionStringsOperation("snowflake_all_fields", ConnectionStringType.SNOWFLAKE)
        )
        self.assertIn("snowflake_all_fields", get_result.snowflake_connection_strings)
        retrieved = get_result.snowflake_connection_strings["snowflake_all_fields"]
        # Assert that all fields remain unchanged
        self.assertEqual(snowflake_connection_string.name, retrieved.name)
        self.assertEqual(snowflake_connection_string.connection_string, retrieved.connection_string)

        # Delete
        remove_result = self.store.maintenance.send(
            RemoveConnectionStringOperation("snowflake_all_fields", ConnectionStringType.SNOWFLAKE)
        )
        self.assertGreater(remove_result.raft_command_index, 0)

        # Get and Assert None
        after_delete = self.store.maintenance.send(
            GetConnectionStringsOperation("snowflake_all_fields", ConnectionStringType.SNOWFLAKE)
        )
        self.assertIsNone(after_delete.snowflake_connection_strings)
