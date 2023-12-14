import json
import unittest

from ravendb import GetDatabaseRecordOperation
from ravendb.documents.operations.time_series import (
    ConfigureTimeSeriesOperation,
    TimeSeriesConfiguration,
    TimeSeriesCollectionConfiguration,
    TimeSeriesPolicy,
    RawTimeSeriesPolicy,
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