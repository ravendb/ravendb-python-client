import unittest

from ravendb.documents.indexes.definitions import IndexDefinition, AdditionalAssembly
from ravendb.documents.operations.indexes import PutIndexesOperation
from ravendb.tests.test_base import TestBase


class TestRavenDB15753(TestBase):
    def setUp(self):
        super().setUp()

    def test_additional_assemblies_runtime(self):
        index_definition = IndexDefinition()
        index_definition.name = "XmlIndex"
        index_definition.maps = ["from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"]

        assemblies = set()
        index_definition.additional_assemblies = assemblies

        assemblies.add(AdditionalAssembly.from_runtime("System.Xml"))
        assemblies.add(AdditionalAssembly.from_runtime("System.Xml.ReaderWriter"))
        assemblies.add(AdditionalAssembly.from_runtime("System.Private.Xml"))

        self.store.maintenance.send(PutIndexesOperation(index_definition))

    def test_additional_assemblies_runtime_invalid_name(self):
        def act():
            index_definition = IndexDefinition()
            index_definition.name = "XmlIndex"
            index_definition.maps = ["from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"]
            index_definition.additional_assemblies = {
                AdditionalAssembly.from_runtime("Some.Assembly.That.Does.Not.Exist")
            }

            self.store.maintenance.send(PutIndexesOperation(index_definition))

        self.assertRaisesWithMessage(act, Exception, "Cannot load assembly 'Some.Assembly.That.Does.Not.Exist'.")

    @unittest.skip("https://issues.hibernatingrhinos.com/issue/RDBC-555")
    def test_additional_assemblies_nuGet(self):
        index_definition = IndexDefinition()
        index_definition.name = "XmlIndex"
        index_definition.maps = ["from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"]
        assemblies = set()
        index_definition.additional_assemblies = assemblies
        assemblies.add(AdditionalAssembly.from_runtime("System.Private.Xml"))
        assemblies.add(AdditionalAssembly.from_NuGet("System.Xml.ReaderWriter", "4.3.1"))

        self.store.maintenance.send(PutIndexesOperation(index_definition))

    @unittest.skip("https://issues.hibernatingrhinos.com/issue/RDBC-555")
    def test_additional_assemblies_NuGet_invalid_name(self):
        def act():
            index_definition = IndexDefinition()
            index_definition.name = "XmlIndex"
            index_definition.maps = ["from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"]
            index_definition.additional_assemblies = {
                AdditionalAssembly.from_NuGet("Some.Assembly.That.Does.Not.Exist", "4.3.1")
            }

            self.store.maintenance.send(PutIndexesOperation(index_definition))

        self.assertRaisesWithMessage(
            act,
            Exception,
            "Cannot load NuGet package 'Some.Assembly.That.Does.Not.Exist' version '4.3.1' "
            "from 'https://api.nuget.org/v3/index.json'.",
        )

    @unittest.skip("https://issues.hibernatingrhinos.com/issue/RDBC-555")
    def test_additional_assemblies_NuGet_invalid_source(self):
        def act():
            index_definition = IndexDefinition()
            index_definition.name = "XmlIndex"
            index_definition.maps = ["from c in docs.Companies select new { Name = typeof(System.Xml.XmlNode).Name }"]
            index_definition.additional_assemblies = {
                AdditionalAssembly.from_NuGet(
                    "System.Xml.ReaderWriter", "4.3.1", "http://some.url.that.does.not.exist.com"
                )
            }

            self.store.maintenance.send(PutIndexesOperation(index_definition))

        self.assertRaisesWithMessage(
            act,
            Exception,
            "Cannot load NuGet package 'System.Xml.ReaderWriter' version '4.3.1' "
            "from 'http://some.url.that.does.not.exist.com'.",
        )
