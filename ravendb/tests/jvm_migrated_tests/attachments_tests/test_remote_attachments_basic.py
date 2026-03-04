import unittest

from ravendb.documents.operations.attachments import (
    ConfigureRemoteAttachmentsOperation,
    GetRemoteAttachmentsConfigurationOperation,
    RemoteAttachmentsAzureSettings,
    RemoteAttachmentsConfiguration,
    RemoteAttachmentsDestinationConfiguration,
    RemoteAttachmentsS3Settings,
)
from ravendb.tests.test_base import TestBase


class TestRemoteAttachmentsBasic(TestBase):
    def setUp(self):
        super().setUp()

    # ── CRUD tests ────────────────────────────────────────────────────────────

    def test_can_put_and_get_remote_attachments_configuration_with_case_insensitive_identifier(self):
        self.store.maintenance.send(
            ConfigureRemoteAttachmentsOperation(
                RemoteAttachmentsConfiguration(
                    destinations={
                        "S3-uSeRs": RemoteAttachmentsDestinationConfiguration(
                            s3_settings=RemoteAttachmentsS3Settings(bucket_name="testS3Bucket-Users"),
                            disabled=False,
                        )
                    },
                    max_items_to_process=1,
                )
            )
        )

        config = self.store.maintenance.send(GetRemoteAttachmentsConfigurationOperation())
        self.assertEqual(1, len(config.destinations))
        dest_key, dest_val = next(iter(config.destinations.items()))
        self.assertEqual("S3-uSeRs", dest_key)
        self.assertEqual("testS3Bucket-Users", dest_val.s3_settings.bucket_name)
        self.assertFalse(dest_val.disabled)
        self.assertIsNone(config.check_frequency_in_sec)

    def test_can_put_and_get_remote_attachments_configuration_with_default_remote_frequency_in_sec(self):
        self.store.maintenance.send(
            ConfigureRemoteAttachmentsOperation(
                RemoteAttachmentsConfiguration(
                    destinations={
                        "S3-Users": RemoteAttachmentsDestinationConfiguration(
                            s3_settings=RemoteAttachmentsS3Settings(bucket_name="testS3Bucket-Users"),
                            disabled=False,
                        )
                    },
                    max_items_to_process=1,
                )
            )
        )

        config = self.store.maintenance.send(GetRemoteAttachmentsConfigurationOperation())
        self.assertEqual(1, len(config.destinations))
        dest_key, dest_val = next(iter(config.destinations.items()))
        self.assertEqual("S3-Users", dest_key)
        self.assertEqual("testS3Bucket-Users", dest_val.s3_settings.bucket_name)
        self.assertFalse(dest_val.disabled)
        self.assertIsNone(config.check_frequency_in_sec)

    def test_can_put_and_get_remote_attachments_configuration(self):
        c1 = RemoteAttachmentsConfiguration(
            destinations={
                "S3-Users": RemoteAttachmentsDestinationConfiguration(
                    s3_settings=RemoteAttachmentsS3Settings(bucket_name="testS3Bucket-Users"),
                    disabled=False,
                )
            },
            check_frequency_in_sec=1000,
        )
        self.store.maintenance.send(ConfigureRemoteAttachmentsOperation(c1))

        config = self.store.maintenance.send(GetRemoteAttachmentsConfigurationOperation())
        dest_key, dest_val = next(iter(config.destinations.items()))
        self.assertEqual(1, len(config.destinations))
        self.assertEqual("S3-Users", dest_key)
        self.assertEqual("testS3Bucket-Users", dest_val.s3_settings.bucket_name)
        self.assertFalse(dest_val.disabled)
        self.assertEqual(1000, config.check_frequency_in_sec)

        c2 = RemoteAttachmentsConfiguration(
            destinations={
                "S3-Orders": RemoteAttachmentsDestinationConfiguration(
                    s3_settings=RemoteAttachmentsS3Settings(bucket_name="testS3Bucket-Orders"),
                    disabled=True,
                )
            },
            check_frequency_in_sec=10000,
            disabled=True,
        )
        self.store.maintenance.send(ConfigureRemoteAttachmentsOperation(c2))

        config2 = self.store.maintenance.send(GetRemoteAttachmentsConfigurationOperation())
        dest_key2, dest_val2 = next(iter(config2.destinations.items()))
        self.assertEqual(1, len(config2.destinations))
        self.assertTrue(config2.disabled)
        self.assertEqual("S3-Orders", dest_key2)
        self.assertEqual("testS3Bucket-Orders", dest_val2.s3_settings.bucket_name)
        self.assertTrue(dest_val2.disabled)
        self.assertEqual(10000, config2.check_frequency_in_sec)

    def test_can_put_and_update_remote_attachments_configuration(self):
        c1 = RemoteAttachmentsConfiguration(
            destinations={
                "S3-Users": RemoteAttachmentsDestinationConfiguration(
                    s3_settings=RemoteAttachmentsS3Settings(bucket_name="testS3Bucket-Users"),
                    disabled=False,
                )
            },
            check_frequency_in_sec=1000,
        )
        self.store.maintenance.send(ConfigureRemoteAttachmentsOperation(c1))

        config = self.store.maintenance.send(GetRemoteAttachmentsConfigurationOperation())
        dest_key, dest_val = next(iter(config.destinations.items()))
        self.assertEqual("S3-Users", dest_key)
        self.assertEqual("testS3Bucket-Users", dest_val.s3_settings.bucket_name)
        self.assertFalse(dest_val.disabled)
        self.assertEqual(1000, config.check_frequency_in_sec)

        config.destinations["S3-Orders"] = RemoteAttachmentsDestinationConfiguration(
            s3_settings=RemoteAttachmentsS3Settings(bucket_name="testS3Bucket-Orders"),
            disabled=True,
        )
        config.check_frequency_in_sec = 10000
        self.store.maintenance.send(ConfigureRemoteAttachmentsOperation(config))

        config2 = self.store.maintenance.send(GetRemoteAttachmentsConfigurationOperation())
        self.assertEqual(2, len(config2.destinations))
        self.assertEqual(10000, config2.check_frequency_in_sec)

        dest_orders = config2.destinations.get("S3-Orders")
        self.assertIsNotNone(dest_orders)
        self.assertEqual("testS3Bucket-Orders", dest_orders.s3_settings.bucket_name)
        self.assertTrue(dest_orders.disabled)

        config3 = self.store.maintenance.send(GetRemoteAttachmentsConfigurationOperation())
        dest_users = config3.destinations.get("S3-Users")
        self.assertIsNotNone(dest_users)
        self.assertEqual("testS3Bucket-Users", dest_users.s3_settings.bucket_name)
        self.assertFalse(dest_users.disabled)

    # ── Validation tests (client-side assert_configuration) ───────────────────

    def _make_op(self, config: RemoteAttachmentsConfiguration):
        """Helper: call ConfigureRemoteAttachmentsOperation constructor (triggers assert_configuration)."""
        ConfigureRemoteAttachmentsOperation(config)

    def test_assert_configuration_rejects_both_uploaders(self):
        self.assertRaisesWithMessageContaining(
            self._make_op,
            ValueError,
            "Only one uploader for RemoteAttachmentsConfiguration can be configured.",
            RemoteAttachmentsConfiguration(
                destinations={
                    "test": RemoteAttachmentsDestinationConfiguration(
                        s3_settings=RemoteAttachmentsS3Settings(bucket_name="testS3Bucket"),
                        azure_settings=RemoteAttachmentsAzureSettings(
                            account_name="testAzureAccount", storage_container="testAzureContainer"
                        ),
                    )
                },
                check_frequency_in_sec=1000,
            ),
        )

    def test_assert_configuration_rejects_zero_check_frequency(self):
        self.assertRaisesWithMessageContaining(
            self._make_op,
            ValueError,
            "Remote attachments check frequency must be greater than 0.",
            RemoteAttachmentsConfiguration(
                destinations={
                    "test": RemoteAttachmentsDestinationConfiguration(
                        s3_settings=RemoteAttachmentsS3Settings(bucket_name="testS3Bucket"),
                    )
                },
                check_frequency_in_sec=0,
            ),
        )

    def test_assert_configuration_rejects_zero_max_items(self):
        self.assertRaisesWithMessageContaining(
            self._make_op,
            ValueError,
            "Max items to process must be greater than 0.",
            RemoteAttachmentsConfiguration(
                destinations={
                    "test": RemoteAttachmentsDestinationConfiguration(
                        s3_settings=RemoteAttachmentsS3Settings(bucket_name="testS3Bucket"),
                    )
                },
                check_frequency_in_sec=1,
                max_items_to_process=0,
            ),
        )

    def test_assert_configuration_rejects_no_uploader(self):
        self.assertRaisesWithMessageContaining(
            self._make_op,
            ValueError,
            "Exactly one uploader for RemoteAttachmentsConfiguration must be configured.",
            RemoteAttachmentsConfiguration(
                destinations={"test": RemoteAttachmentsDestinationConfiguration(disabled=False)},
                check_frequency_in_sec=1,
                max_items_to_process=1,
            ),
        )

    def test_assert_configuration_rejects_null_destination(self):
        self.assertRaisesWithMessageContaining(
            self._make_op,
            ValueError,
            "Destination configuration for key S3-Users is null",
            RemoteAttachmentsConfiguration(
                destinations={"S3-Users": None},
                check_frequency_in_sec=1000,
            ),
        )

    def test_assert_configuration_rejects_duplicate_keys(self):
        # Python dicts enforce unique keys natively, so we test via two separate
        # destinations with keys that differ only in case.
        self.assertRaisesWithMessageContaining(
            self._make_op,
            ValueError,
            "Destination key 'TEST' is duplicate. Duplicate keys are not allowed in remote attachments configuration",
            RemoteAttachmentsConfiguration(
                destinations={
                    "test": RemoteAttachmentsDestinationConfiguration(
                        s3_settings=RemoteAttachmentsS3Settings(bucket_name="testS3Bucket"),
                    ),
                    "TEST": RemoteAttachmentsDestinationConfiguration(
                        azure_settings=RemoteAttachmentsAzureSettings(
                            account_name="testAzureAccount", storage_container="testAzureContainer"
                        ),
                    ),
                },
                check_frequency_in_sec=1,
            ),
        )


if __name__ == "__main__":
    unittest.main()
