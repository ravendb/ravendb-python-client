import enum


class EtlType(enum.Enum):
    RAVEN = "Raven"
    SQL = "Sql"
    OLAP = "Olap"
    ELASTIC_SEARCH = "ElasticSearch"
    QUEUE = "Queue"
    SNOWFLAKE = "Snowflake"
    EMBEDDINGS_GENERATION = "EmbeddingsGeneration"
    GEN_AI = "GenAi"
