import unittest
import importlib
import inspect
import pkgutil
import ravendb

from ravendb.tests.test_base import TestBase


class TestPlatformContracts(TestBase):
    def setUp(self):
        super(TestPlatformContracts, self).setUp()

    def tearDown(self):
        super(TestPlatformContracts, self).tearDown()
        TestBase.delete_all_topology_files()

    def test_all_commands_expose_callable_is_read_request(self):
        from ravendb.http.raven_command import RavenCommand

        offenders = []
        for mod in self._import_all_ravendb_modules():
            for cls in self._iter_subclasses_in_module(mod, RavenCommand):
                attr = getattr(cls, "is_read_request", None)
                if isinstance(attr, property):
                    offenders.append(f"{cls.__module__}.{cls.__qualname__}")

        if offenders:
            self.fail("is_read_request must be a method on: " + ", ".join(offenders))

    # private helpers
    def _import_all_ravendb_modules(self):
        for _, modname, _ in pkgutil.walk_packages(ravendb.__path__, ravendb.__name__ + "."):
            if modname.startswith("ravendb.tests."):
                continue
            try:
                yield importlib.import_module(modname)
            except Exception:
                continue

    def _iter_subclasses_in_module(self, mod, base_cls):
        for _, obj in inspect.getmembers(mod):
            if inspect.isclass(obj) and issubclass(obj, base_cls) and obj is not base_cls:
                yield obj
            if inspect.isclass(obj):
                for _, inner in inspect.getmembers(obj, inspect.isclass):
                    if issubclass(inner, base_cls) and inner is not base_cls:
                        yield inner


if __name__ == "__main__":
    unittest.main
