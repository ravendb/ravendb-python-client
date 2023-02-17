from typing import List

from ravendb import Lazy, ConditionalLoadResult, QueryData
from ravendb.infrastructure.orders import Company
from ravendb.tests.test_base import TestBase


class Result:
    def __init__(self, Id: str = None, change_vector: str = None):
        self.Id = Id
        self.change_vector = change_vector


class TestRavenDB16301(TestBase):
    def setUp(self):
        super(TestRavenDB16301, self).setUp()

    def test_can_use_conditional_load_lazily(self):
        # Store 100 companies using bulk insert - it won't store identity property on the server
        with self.store.bulk_insert() as bulk_insert:
            for i in range(100):
                bulk_insert.store(Company())

        conditional_loads: List[Lazy[ConditionalLoadResult[Company]]] = []

        with self.store.open_session() as query_modify_session:
            id_and_cv_results = list(
                query_modify_session.advanced.document_query(object_type=Company)
                .wait_for_non_stale_results()
                .select_fields_query_data(
                    Result,
                    QueryData.custom_function("o", "{ Id : id(o), change_vector : getMetadata(o)['@change-vector'] }"),
                )
            )

            all_ids = [single_id.Id for single_id in id_and_cv_results]
            query_modify_session.load(all_ids, Company)

            # load 50 first companies and update them
            res_ids = all_ids[0:50]

            res = query_modify_session.load(res_ids, Company)

            c = 0
            for key in res:
                c += 1
                res[key].phone = c

            query_modify_session.save_changes()

        with self.store.open_session() as session:
            # load last 10 (track them in the session)
            session.load(all_ids[-10:])
            number_of_requests_per_session = session.advanced.number_of_requests

            for res in id_and_cv_results:
                conditional_loads.append(session.advanced.lazily.conditional_load(res.Id, res.change_vector, Company))

            session.advanced.eagerly.execute_all_pending_lazy_operations()

            self.assertEqual(number_of_requests_per_session + 1, session.advanced.number_of_requests)

            for i in range(100):
                conditional_load = conditional_loads[i]

                self.assertFalse(conditional_load.is_value_created)
                conditional_load_result = conditional_load.value

                if i < 50:
                    # load from server
                    self.assertEqual(id_and_cv_results[i].Id, conditional_load_result.entity.Id)
                elif i < 90:
                    # not modified
                    self.assertEqual(id_and_cv_results[i].change_vector, conditional_load_result.change_vector)
                    self.assertIsNone(conditional_load_result.entity)

                else:
                    # tracked in session
                    self.assertEqual(id_and_cv_results[i].Id, conditional_load_result.entity.Id)
                    self.assertIsNotNone(conditional_load_result.entity)
                    self.assertEqual(id_and_cv_results[i].change_vector, conditional_load_result.change_vector)

                # not exist on server
                lazy = session.advanced.lazily.conditional_load(
                    "Companies/322-A", id_and_cv_results[0].change_vector, Company
                )
                load = lazy.value
                self.assertIsNone(load.entity)
                self.assertIsNone(load.change_vector)
