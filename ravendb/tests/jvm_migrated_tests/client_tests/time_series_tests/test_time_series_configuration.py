import json
import unittest

from ravendb import GetDatabaseRecordOperation
from ravendb.documents.operations.time_series import (
    ConfigureTimeSeriesOperation,
    TimeSeriesConfiguration,
    TimeSeriesCollectionConfiguration,
    TimeSeriesPolicy,
    RawTimeSeriesPolicy,
    ConfigureTimeSeriesPolicyOperation,
    ConfigureRawTimeSeriesPolicyOperation,
    ConfigureTimeSeriesValueNamesOperation,
)
from ravendb.primitives.time_series import TimeValue, TimeValueUnit
from ravendb.tests.test_base import TestBase


class TestTimeSeriesConfiguration(TestBase):
    def setUp(self):
        super().setUp()

    def test_serialization(self):
        self.assertEqual("{'Value': 7200, 'Unit': 'Second'}", str(TimeValue.of_hours(2).to_json()))

    def test_deserialization(self):
        time_value = TimeValue.from_json(json.loads('{"Value":7200,"Unit":"Second"}'))
        self.assertEqual(TimeValueUnit.SECOND, time_value.unit)
        self.assertEqual(7200, time_value.value)

        time_value = TimeValue.from_json(json.loads('{"Value":2,"Unit":"Month"}'))
        self.assertEqual(TimeValueUnit.MONTH, time_value.unit)
        self.assertEqual(2, time_value.value)

        time_value = TimeValue.from_json(json.loads('{"Value":0,"Unit":"None"}'))
        self.assertEqual(TimeValueUnit.NONE, time_value.unit)
        self.assertEqual(0, time_value.value)

    @unittest.skip("Disable on pull request")
    def test_can_configure_time_series(self):
        config = TimeSeriesConfiguration()
        self.store.maintenance.send(ConfigureTimeSeriesOperation(config))

        config.collections = {}
        self.store.maintenance.send(ConfigureTimeSeriesOperation(config))

        config.collections["Users"] = TimeSeriesCollectionConfiguration()
        self.store.maintenance.send(ConfigureTimeSeriesOperation(config))

        users = config.collections.get("Users")
        users.policies = [
            TimeSeriesPolicy("ByHourFor12Hours", TimeValue.of_hours(1), TimeValue.of_hours(48)),
            TimeSeriesPolicy("ByMinuteFor3Hours", TimeValue.of_minutes(1), TimeValue.of_minutes(180)),
            TimeSeriesPolicy("BySecondFor1Minute", TimeValue.of_seconds(1), TimeValue.of_seconds(60)),
            TimeSeriesPolicy("ByMonthFor1Year", TimeValue.of_months(1), TimeValue.of_years(1)),
            TimeSeriesPolicy("ByYearFor3Years", TimeValue.of_years(1), TimeValue.of_years(3)),
            TimeSeriesPolicy("ByDayFor1Month", TimeValue.of_days(1), TimeValue.of_months(1)),
        ]

        self.store.maintenance.send(ConfigureTimeSeriesOperation(config))

        users.raw_policy = RawTimeSeriesPolicy(TimeValue.of_hours(96))
        self.store.maintenance.send(ConfigureTimeSeriesOperation(config))

        updated: TimeSeriesConfiguration = self.store.maintenance.server.send(
            GetDatabaseRecordOperation(self.store.database)
        ).time_series

        collection = updated.collections.get("Users")
        policies = collection.policies
        self.assertEqual(6, len(policies))

        self.assertEqual(TimeValue.of_seconds(60), policies[0].retention_time)
        self.assertEqual(TimeValue.of_seconds(1), policies[0].aggregation_time)

        self.assertEqual(TimeValue.of_minutes(180), policies[1].retention_time)
        self.assertEqual(TimeValue.of_minutes(1), policies[1].aggregation_time)

        self.assertEqual(TimeValue.of_hours(48), policies[2].retention_time)
        self.assertEqual(TimeValue.of_hours(1), policies[2].aggregation_time)

        self.assertEqual(TimeValue.of_months(1), policies[3].retention_time)
        self.assertEqual(TimeValue.of_days(1), policies[3].aggregation_time)

        self.assertEqual(TimeValue.of_years(1), policies[4].retention_time)
        self.assertEqual(TimeValue.of_months(1), policies[4].aggregation_time)

        self.assertEqual(TimeValue.of_years(3), policies[5].retention_time)
        self.assertEqual(TimeValue.of_years(1), policies[5].aggregation_time)

    def test_can_configure_time_series_2(self):
        collection_name = "Users"

        p1 = TimeSeriesPolicy("BySecondFor1Minute", TimeValue.of_seconds(1), TimeValue.of_seconds(60))
        p2 = TimeSeriesPolicy("ByMinuteFor3Hours", TimeValue.of_minutes(1), TimeValue.of_minutes(180))
        p3 = TimeSeriesPolicy("ByHourFor12Hours", TimeValue.of_hours(1), TimeValue.of_hours(48))
        p4 = TimeSeriesPolicy("ByDayFor1Month", TimeValue.of_days(1), TimeValue.of_months(1))
        p5 = TimeSeriesPolicy("ByMonthFor1Year", TimeValue.of_months(1), TimeValue.of_years(1))
        p6 = TimeSeriesPolicy("ByYearFor3Years", TimeValue.of_years(1), TimeValue.of_years(3))

        policies = [p1, p2, p3, p4, p5, p6]

        for policy in policies:
            self.store.maintenance.send(ConfigureTimeSeriesPolicyOperation(collection_name, policy))

        self.store.maintenance.send(
            ConfigureRawTimeSeriesPolicyOperation(collection_name, RawTimeSeriesPolicy(TimeValue.of_hours(96)))
        )

        parameters = ConfigureTimeSeriesValueNamesOperation.Parameters(None, None, None, None)
        parameters.collection = collection_name
        parameters.time_series = "HeartRate"
        parameters.value_names = ["HeartRate"]
        parameters.update = True

        name_config = ConfigureTimeSeriesValueNamesOperation(parameters)
        self.store.maintenance.send(name_config)

        updated: TimeSeriesConfiguration = self.store.maintenance.server.send(
            GetDatabaseRecordOperation(self.store.database)
        ).time_series

        collection = updated.collections.get(collection_name)
        policies = collection.policies

        self.assertEqual(6, len(policies))

        self.assertEqual(TimeValue.of_seconds(60), policies[0].retention_time)
        self.assertEqual(TimeValue.of_seconds(1), policies[0].aggregation_time)

        self.assertEqual(TimeValue.of_minutes(180), policies[1].retention_time)
        self.assertEqual(TimeValue.of_minutes(1), policies[1].aggregation_time)

        self.assertEqual(TimeValue.of_hours(48), policies[2].retention_time)
        self.assertEqual(TimeValue.of_hours(1), policies[2].aggregation_time)

        self.assertEqual(TimeValue.of_months(1), policies[3].retention_time)
        self.assertEqual(TimeValue.of_days(1), policies[3].aggregation_time)

        self.assertEqual(TimeValue.of_years(1), policies[4].retention_time)
        self.assertEqual(TimeValue.of_months(1), policies[4].aggregation_time)

        self.assertEqual(TimeValue.of_years(3), policies[5].retention_time)
        self.assertEqual(TimeValue.of_years(1), policies[5].aggregation_time)

    def test_not_valid_configure_should_throw(self):
        config = TimeSeriesConfiguration()
        collections_config = {}
        config.collections = collections_config

        time_series_collection_configuration = TimeSeriesCollectionConfiguration()
        collections_config["Users"] = time_series_collection_configuration

        time_series_collection_configuration.raw_policy = RawTimeSeriesPolicy(TimeValue.of_months(1))
        time_series_collection_configuration.policies = [
            TimeSeriesPolicy("By30DaysFor5Years", TimeValue.of_days(30), TimeValue.of_years(5))
        ]

        self.assertRaisesWithMessage(
            self.store.maintenance.send,
            Exception,
            "Unable to compare 1 month with 30 days, since a month might have different number of days.",
            ConfigureTimeSeriesOperation(config),
        )

        config2 = TimeSeriesConfiguration()
        collections_config = {}
        config2.collections = collections_config

        time_series_collection_configuration = TimeSeriesCollectionConfiguration()
        collections_config["Users"] = time_series_collection_configuration

        time_series_collection_configuration.raw_policy = RawTimeSeriesPolicy(TimeValue.of_months(12))
        time_series_collection_configuration.policies = [
            TimeSeriesPolicy("By365DaysFor5Years", TimeValue.of_seconds(365 * 24 * 3600), TimeValue.of_years(5))
        ]

        self.assertRaisesWithMessage(
            self.store.maintenance.send,
            Exception,
            "Unable to compare 1 year with 365 days, since a month might have different number of days.",
            ConfigureTimeSeriesOperation(config2),
        )

        config3 = TimeSeriesConfiguration()
        collections_config = {}
        config3.collections = collections_config

        time_series_collection_configuration = TimeSeriesCollectionConfiguration()
        collections_config["Users"] = time_series_collection_configuration

        time_series_collection_configuration.raw_policy = RawTimeSeriesPolicy(TimeValue.of_months(1))
        time_series_collection_configuration.policies = [
            TimeSeriesPolicy("By27DaysFor1Year", TimeValue.of_days(27), TimeValue.of_years(1)),
            TimeSeriesPolicy("By364daysFor5Years", TimeValue.of_days(364), TimeValue.of_years(5)),
        ]

        self.assertRaisesWithMessage(
            self.store.maintenance.send,
            Exception,
            "The aggregation time of the policy 'By364daysFor5Years' (364 days) "
            "must be divided by the aggregation time of 'By27DaysFor1Year' (27 days) without a remainder.",
            ConfigureTimeSeriesOperation(config3),
        )
