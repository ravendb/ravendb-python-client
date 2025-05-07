from ravendb.tests.dotnet_migrated_tests.test_ravenDB_22076 import Dto
from ravendb.tests.test_base import TestBase


class TestVectorSearch(TestBase):
    def test_should_generate_rql_with_text_field_using_named_ai_task(self):
        with self.store.open_session() as session:
            q = session.query(object_type=Dto).vector_search_text_using_task("EmbeddingField", "fishing", "my-ai-task")
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.text(EmbeddingField, ai.task('my-ai-task')), $p0)",
                q._to_string(),
            )

            q_exact = session.query(object_type=Dto).vector_search_text_using_task(
                "EmbeddingField", "fishing", "my-ai-task", is_exact=True
            )
            self.assertEqual(
                "from 'Dtoes' where exact(vector.search(embedding.text(EmbeddingField, ai.task('my-ai-task')), $p0))",
                q_exact._to_string(),
            )

            q2 = session.query(object_type=Dto).vector_search_text_i1_using_task(
                "EmbeddingField", "fishing", "my-ai-task"
            )
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.text_i1(EmbeddingField, ai.task('my-ai-task')), $p0)",
                q2._to_string(),
            )

            q2_exact = session.query(object_type=Dto).vector_search_text_i1_using_task(
                "EmbeddingField", "fishing", "my-ai-task", is_exact=True
            )
            self.assertEqual(
                "from 'Dtoes' where exact(vector.search(embedding.text_i1(EmbeddingField, ai.task('my-ai-task')), $p0))",
                q2_exact._to_string(),
            )

            q3 = session.query(object_type=Dto).vector_search_text_i8_using_task(
                "EmbeddingField", "fishing", "my-ai-task"
            )
            self.assertEqual(
                "from 'Dtoes' where vector.search(embedding.text_i8(EmbeddingField, ai.task('my-ai-task')), $p0)",
                q3._to_string(),
            )

            q3_exact = session.query(object_type=Dto).vector_search_text_i8_using_task(
                "EmbeddingField", "fishing", "my-ai-task", is_exact=True
            )
            self.assertEqual(
                "from 'Dtoes' where exact(vector.search(embedding.text_i8(EmbeddingField, ai.task('my-ai-task')), $p0))",
                q3_exact._to_string(),
            )
