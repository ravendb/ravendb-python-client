"""Unit tests for the CDC Sink configuration classes: CdcSinkConfiguration,
its nested config classes, and the CdcSinkTaskState / CdcSinkTableLoadState
serialization shape.
"""

import unittest

from ravendb.documents.operations.cdc_sink.cdc_sink_configuration import (
    CdcColumnMapping,
    CdcColumnType,
    CdcSinkConfiguration,
    CdcSinkEmbeddedTableConfig,
    CdcSinkLinkedTableConfig,
    CdcSinkOnDeleteConfig,
    CdcSinkPostgresSettings,
    CdcSinkRelationType,
    CdcSinkTableConfig,
)
from ravendb.documents.operations.cdc_sink.cdc_sink_task_state import (
    CdcSinkTableLoadState,
    CdcSinkTablesDict,
    CdcSinkTaskState,
)


class TestCdcSinkConfiguration(unittest.TestCase):
    def test_to_json_key_order_and_values(self):
        config = CdcSinkConfiguration(
            task_id=42,
            disabled=True,
            name="cdc-1",
            mentor_node="A",
            pin_to_mentor_node=True,
            connection_string_name="sql-cs",
            tables=[CdcSinkTableConfig(collection_name="Orders", source_table_name="orders")],
            postgres=CdcSinkPostgresSettings(publication_name="pub", slot_name="slot"),
            skip_initial_load=True,
        )
        out = config.to_json()
        self.assertEqual(
            [
                "Name",
                "TaskId",
                "Disabled",
                "ConnectionStringName",
                "MentorNode",
                "PinToMentorNode",
                "Tables",
                "Postgres",
                "SkipInitialLoad",
            ],
            list(out.keys()),
        )
        self.assertEqual("cdc-1", out["Name"])
        self.assertEqual(42, out["TaskId"])
        self.assertTrue(out["Disabled"])
        self.assertEqual("sql-cs", out["ConnectionStringName"])
        self.assertEqual("A", out["MentorNode"])
        self.assertTrue(out["PinToMentorNode"])
        self.assertTrue(out["SkipInitialLoad"])
        self.assertEqual({"PublicationName": "pub", "SlotName": "slot"}, out["Postgres"])

    def test_postgres_is_null_when_unset(self):
        out = CdcSinkConfiguration(name="c", connection_string_name="sql").to_json()
        self.assertIsNone(out["Postgres"])

    def test_test_mode_is_never_serialized(self):
        out = CdcSinkConfiguration(name="c", connection_string_name="sql").to_json()
        self.assertNotIn("TestMode", out)

    def test_from_json_round_trip_with_null_postgres(self):
        raw = {
            "Name": "c",
            "TaskId": 0,
            "Disabled": False,
            "ConnectionStringName": "sql",
            "MentorNode": None,
            "PinToMentorNode": False,
            "Tables": [],
            "Postgres": None,
            "SkipInitialLoad": False,
        }
        config = CdcSinkConfiguration.from_json(raw)
        self.assertEqual("c", config.name)
        self.assertEqual("sql", config.connection_string_name)
        self.assertIsNone(config.postgres)
        self.assertEqual([], config.tables)
        self.assertEqual(raw, config.to_json())

    def test_from_json_missing_keys_use_defaults(self):
        config = CdcSinkConfiguration.from_json({"ConnectionStringName": "sql"})
        self.assertFalse(config.disabled)
        self.assertEqual(0, config.task_id)
        self.assertIsNone(config.name)
        self.assertFalse(config.pin_to_mentor_node)
        self.assertEqual([], config.tables)
        self.assertIsNone(config.postgres)
        self.assertFalse(config.skip_initial_load)

    def test_get_default_task_name(self):
        config = CdcSinkConfiguration(connection_string_name="my-sql")
        self.assertEqual("CDC Sink to my-sql", config.get_default_task_name())

    def test_empty_tables_serialize_as_array_not_omitted(self):
        out = CdcSinkConfiguration(name="c", connection_string_name="sql").to_json()
        self.assertEqual([], out["Tables"])

    def test_task_id_round_trips_beyond_2_31(self):
        config = CdcSinkConfiguration(task_id=5000000000, name="c", connection_string_name="sql")
        self.assertEqual(5000000000, CdcSinkConfiguration.from_json(config.to_json()).task_id)


class TestCdcSinkTableConfig(unittest.TestCase):
    def test_to_json_keys(self):
        table = CdcSinkTableConfig(
            collection_name="Orders",
            source_table_schema="dbo",
            source_table_name="orders",
            columns=[CdcColumnMapping(column="id", name="Id")],
            primary_key_columns=["id"],
            patch="this.Total = $row.total;",
            on_delete=CdcSinkOnDeleteConfig(patch="this.Archived = true;", ignore_deletes=True),
            disabled=False,
        )
        out = table.to_json()
        self.assertEqual(
            [
                "CollectionName",
                "SourceTableSchema",
                "SourceTableName",
                "Columns",
                "PrimaryKeyColumns",
                "Patch",
                "OnDelete",
                "Disabled",
                "EmbeddedTables",
                "LinkedTables",
            ],
            list(out.keys()),
        )
        self.assertEqual("Orders", out["CollectionName"])
        self.assertEqual(["id"], out["PrimaryKeyColumns"])
        self.assertEqual({"Patch": "this.Archived = true;", "IgnoreDeletes": True}, out["OnDelete"])
        self.assertEqual([], out["EmbeddedTables"])
        self.assertEqual([], out["LinkedTables"])

    def test_empty_lists_are_arrays_not_omitted(self):
        out = CdcSinkTableConfig(collection_name="Orders").to_json()
        self.assertEqual([], out["Columns"])
        self.assertEqual([], out["PrimaryKeyColumns"])
        self.assertIsNone(out["OnDelete"])

    def test_from_json_round_trip(self):
        table = CdcSinkTableConfig(
            collection_name="Orders",
            source_table_schema="dbo",
            source_table_name="orders",
            columns=[CdcColumnMapping(column="id", name="Id")],
            primary_key_columns=["id"],
            patch="this.Total = $row.total;",
            on_delete=CdcSinkOnDeleteConfig(ignore_deletes=True),
            disabled=True,
        )
        parsed = CdcSinkTableConfig.from_json(table.to_json())
        self.assertEqual("Orders", parsed.collection_name)
        self.assertEqual("dbo", parsed.source_table_schema)
        self.assertTrue(parsed.disabled)
        self.assertTrue(parsed.on_delete.ignore_deletes)
        self.assertEqual("this.Total = $row.total;", parsed.patch)


class TestCdcColumnMapping(unittest.TestCase):
    def test_type_omitted_when_default(self):
        out = CdcColumnMapping(column="id", name="Id").to_json()
        self.assertEqual(["Column", "Name"], list(out.keys()))

    def test_type_written_as_enum_name_when_not_default(self):
        self.assertEqual(
            "Json", CdcColumnMapping(column="data", name="Data", type=CdcColumnType.JSON).to_json()["Type"]
        )
        self.assertEqual(
            "Attachment", CdcColumnMapping(column="file", name="File", type=CdcColumnType.ATTACHMENT).to_json()["Type"]
        )

    def test_enum_wire_values(self):
        self.assertEqual("Default", CdcColumnType.DEFAULT.value)
        self.assertEqual("Json", CdcColumnType.JSON.value)
        self.assertEqual("Attachment", CdcColumnType.ATTACHMENT.value)

    def test_from_json_default_type_when_absent(self):
        mapping = CdcColumnMapping.from_json({"Column": "id", "Name": "Id"})
        self.assertEqual(CdcColumnType.DEFAULT, mapping.type)
        mapping = CdcColumnMapping.from_json({"Column": "data", "Name": "Data", "Type": "Json"})
        self.assertEqual(CdcColumnType.JSON, mapping.type)


class TestCdcSinkEmbeddedTableConfig(unittest.TestCase):
    def test_type_always_written_as_enum_name(self):
        for relation_type in CdcSinkRelationType:
            config = CdcSinkEmbeddedTableConfig(source_table_name="lines", property_name="Lines", type=relation_type)
            self.assertEqual(relation_type.value, config.to_json()["Type"])

    def test_to_json_keys(self):
        config = CdcSinkEmbeddedTableConfig(
            source_table_schema="dbo",
            source_table_name="order_lines",
            property_name="Lines",
            columns=[CdcColumnMapping(column="id", name="Id")],
            primary_key_columns=["id"],
            join_columns=["order_id"],
            type=CdcSinkRelationType.ARRAY,
            patch="this.Total = $row.total;",
            on_delete=CdcSinkOnDeleteConfig(ignore_deletes=True),
            case_sensitive_keys=True,
            embedded_tables=[CdcSinkEmbeddedTableConfig(source_table_name="child", property_name="Child")],
            linked_tables=[
                CdcSinkLinkedTableConfig(
                    source_table_name="customers",
                    property_name="Customer",
                    join_columns=["customer_id"],
                    linked_collection_name="Customers",
                )
            ],
        )
        out = config.to_json()
        self.assertEqual(
            [
                "SourceTableSchema",
                "SourceTableName",
                "PropertyName",
                "Columns",
                "PrimaryKeyColumns",
                "JoinColumns",
                "Type",
                "Patch",
                "OnDelete",
                "CaseSensitiveKeys",
                "EmbeddedTables",
                "LinkedTables",
            ],
            list(out.keys()),
        )
        self.assertEqual("Array", out["Type"])
        self.assertTrue(out["CaseSensitiveKeys"])
        self.assertEqual(1, len(out["EmbeddedTables"]))
        self.assertEqual(1, len(out["LinkedTables"]))

    def test_round_trip(self):
        config = CdcSinkEmbeddedTableConfig(
            source_table_name="order_lines",
            property_name="Lines",
            type=CdcSinkRelationType.MAP,
            case_sensitive_keys=True,
            embedded_tables=[CdcSinkEmbeddedTableConfig(source_table_name="child", property_name="Child")],
            linked_tables=[
                CdcSinkLinkedTableConfig(
                    source_table_name="customers",
                    property_name="Customer",
                    join_columns=["customer_id"],
                    linked_collection_name="Customers",
                )
            ],
        )
        parsed = CdcSinkEmbeddedTableConfig.from_json(config.to_json())
        self.assertEqual(CdcSinkRelationType.MAP, parsed.type)
        self.assertTrue(parsed.case_sensitive_keys)
        self.assertEqual("child", parsed.embedded_tables[0].source_table_name)
        self.assertEqual("Customers", parsed.linked_tables[0].linked_collection_name)


class TestCdcSinkOnDeleteConfig(unittest.TestCase):
    def test_to_json_keys(self):
        config = CdcSinkOnDeleteConfig(patch="this.Archived = true;", ignore_deletes=True)
        self.assertEqual({"Patch": "this.Archived = true;", "IgnoreDeletes": True}, config.to_json())

    def test_round_trip(self):
        parsed = CdcSinkOnDeleteConfig.from_json({"Patch": "p", "IgnoreDeletes": False})
        self.assertEqual("p", parsed.patch)
        self.assertFalse(parsed.ignore_deletes)


class TestCdcSinkPostgresSettings(unittest.TestCase):
    def test_to_json_keys(self):
        self.assertEqual(
            {"PublicationName": "pub", "SlotName": "slot"},
            CdcSinkPostgresSettings(publication_name="pub", slot_name="slot").to_json(),
        )

    def test_round_trip(self):
        parsed = CdcSinkPostgresSettings.from_json({"PublicationName": "pub", "SlotName": "slot"})
        self.assertEqual("pub", parsed.publication_name)
        self.assertEqual("slot", parsed.slot_name)


class TestCdcSinkTaskState(unittest.TestCase):
    def test_collection_name_constant(self):
        self.assertEqual("@cdc-states", CdcSinkTaskState.collection_name)

    def test_get_document_id_preserves_casing(self):
        self.assertEqual("@cdc-states/MyCdc", CdcSinkTaskState.get_document_id("MyCdc"))

    def test_to_json_shape(self):
        state = CdcSinkTaskState(last_lsn="0/1", configuration_name="cdc-1")
        state.tables["orders"] = CdcSinkTableLoadState(
            initial_load_completed=True, last_key_values=["1"], key_columns=["id"]
        )
        out = state.to_json()
        self.assertEqual(["ConfigurationName", "LastLsn", "Tables"], list(out.keys()))
        self.assertIsInstance(out["Tables"], dict)
        self.assertIsInstance(out["Tables"]["orders"], dict)

    def test_to_json_preserves_table_key_casing(self):
        state = CdcSinkTaskState(configuration_name="cdc-1")
        state.tables["Orders"] = CdcSinkTableLoadState()
        out = state.to_json()
        self.assertIn("Orders", out["Tables"])

    def test_tables_dict_lookup_is_case_insensitive(self):
        state = CdcSinkTaskState(configuration_name="cdc-1")
        state.tables["orders"] = CdcSinkTableLoadState(initial_load_completed=True)
        self.assertIn("ORDERS", state.tables)
        self.assertTrue(state.tables["ORDERS"].initial_load_completed)

    def test_tables_dict_reinsert_with_different_casing_updates_single_entry(self):
        state = CdcSinkTaskState(configuration_name="cdc-1")
        state.tables["orders"] = CdcSinkTableLoadState(initial_load_completed=False)
        state.tables["ORDERS"] = CdcSinkTableLoadState(initial_load_completed=True)
        self.assertEqual(1, len(state.tables))
        self.assertTrue(state.tables["orders"].initial_load_completed)
        out = state.to_json()
        self.assertEqual(1, len(out["Tables"]))
        self.assertTrue(out["Tables"]["ORDERS"]["InitialLoadCompleted"])

    def test_tables_dict_removal_through_any_casing_cleans_up(self):
        state = CdcSinkTaskState(configuration_name="cdc-1")
        state.tables["orders"] = CdcSinkTableLoadState(initial_load_completed=True)
        state.tables["products"] = CdcSinkTableLoadState()
        del state.tables["ORDERS"]
        self.assertEqual(1, len(state.tables))
        self.assertNotIn("orders", state.tables)
        out = state.to_json()
        self.assertEqual(["products"], list(out["Tables"].keys()))
        self.assertTrue(out["Tables"]["products"]["InitialLoadCompleted"] is False)

    def test_from_json_missing_tables_yields_empty_dict(self):
        state = CdcSinkTaskState.from_json({"ConfigurationName": "cdc-1", "LastLsn": "0/1"})
        self.assertEqual({}, state.tables)
        self.assertEqual("0/1", state.last_lsn)

    def test_from_json_builds_case_insensitive_tables_at_parse_time(self):
        raw = {
            "ConfigurationName": "cdc-1",
            "LastLsn": "0/1",
            "Tables": {"orders": {"InitialLoadCompleted": True, "LastKeyValues": ["1"], "KeyColumns": ["id"]}},
        }
        state = CdcSinkTaskState.from_json(raw)
        self.assertTrue(state.tables["ORDERS"].initial_load_completed)
        self.assertEqual(["1"], state.tables["Orders"].last_key_values)
        self.assertEqual(["id"], state.tables["orders"].key_columns)

    def test_table_load_state_null_lists_on_wire(self):
        out = CdcSinkTableLoadState().to_json()
        self.assertEqual(["InitialLoadCompleted", "LastKeyValues", "KeyColumns"], list(out.keys()))
        self.assertIsNone(out["LastKeyValues"])
        self.assertIsNone(out["KeyColumns"])

    def test_table_load_state_from_json_nulls_stay_none(self):
        load = CdcSinkTableLoadState.from_json({"InitialLoadCompleted": False})
        self.assertIsNone(load.last_key_values)
        self.assertIsNone(load.key_columns)
        load = CdcSinkTableLoadState.from_json(
            {"InitialLoadCompleted": True, "LastKeyValues": None, "KeyColumns": None}
        )
        self.assertIsNone(load.last_key_values)
        self.assertTrue(load.initial_load_completed)

    def test_tables_dict_type(self):
        self.assertIsInstance(CdcSinkTablesDict(), dict)


class TestNestedConfigurationRoundTrip(unittest.TestCase):
    def test_fully_populated_configuration_survives_round_trip(self):
        config = CdcSinkConfiguration(
            task_id=7,
            name="cdc-nested",
            connection_string_name="sql-cs",
            skip_initial_load=True,
            tables=[
                CdcSinkTableConfig(
                    collection_name="Orders",
                    source_table_schema="dbo",
                    source_table_name="orders",
                    columns=[
                        CdcColumnMapping(column="id", name="Id"),
                        CdcColumnMapping(column="data", name="Data", type=CdcColumnType.JSON),
                    ],
                    primary_key_columns=["id"],
                    patch="this.Total = $row.total;",
                    on_delete=CdcSinkOnDeleteConfig(patch="this.Archived = true;", ignore_deletes=True),
                    disabled=False,
                    embedded_tables=[
                        CdcSinkEmbeddedTableConfig(
                            source_table_name="order_lines",
                            property_name="Lines",
                            columns=[CdcColumnMapping(column="id", name="Id")],
                            primary_key_columns=["id"],
                            join_columns=["order_id"],
                            type=CdcSinkRelationType.ARRAY,
                            case_sensitive_keys=True,
                            embedded_tables=[
                                CdcSinkEmbeddedTableConfig(source_table_name="line_items", property_name="Items")
                            ],
                        )
                    ],
                    linked_tables=[
                        CdcSinkLinkedTableConfig(
                            source_table_name="customers",
                            property_name="Customer",
                            join_columns=["customer_id"],
                            linked_collection_name="Customers",
                        )
                    ],
                ),
                CdcSinkTableConfig(collection_name="Products", source_table_name="products"),
            ],
            postgres=CdcSinkPostgresSettings(publication_name="rvn_cdc_p_1", slot_name="rvn_cdc_s_1"),
        )
        parsed = CdcSinkConfiguration.from_json(config.to_json())
        self.assertEqual(config.to_json(), parsed.to_json())
        self.assertEqual("Orders", parsed.tables[0].collection_name)
        self.assertEqual(CdcColumnType.JSON, parsed.tables[0].columns[1].type)
        self.assertEqual("Array", parsed.tables[0].embedded_tables[0].type.value)
        self.assertEqual("Customers", parsed.tables[0].linked_tables[0].linked_collection_name)
        self.assertEqual("rvn_cdc_p_1", parsed.postgres.publication_name)
        # table order is preserved
        self.assertEqual(["Orders", "Products"], [t.collection_name for t in parsed.tables])


if __name__ == "__main__":
    unittest.main()
