# -*- coding: utf-8 -*- #
from d_commands import database_commands, commands_data
from data.indexes import IndexDefinition, IndexQuery
from data.operations import BulkOperationOption
from data.patches import ScriptedPatchRequest
from store.document_store import DocumentStore
from data.document_convention import DocumentConvention
from tools.utils import Utils

put1 = commands_data.PutCommandData("products/999", document={"Name": "tests", "Category": "testing"},
                                    metadata={"Raven-Python-Type": "Products"})
put2 = commands_data.PutCommandData("products/1000", document={"Name": "testsDelete", "Category": "testing"},
                                    metadata={"Raven-Python-Type": "Products"})
put3 = commands_data.PutCommandData("products/1001", document={"Name": "testsDelete", "Category": "testing"},
                                    metadata={"Raven-Python-Type": "Products"})
put4 = commands_data.PutCommandData("products/1002", document={"Name": "testsDelete", "Category": "testing"},
                                    metadata={"Raven-Python-Type": "Products"})
put5 = commands_data.PutCommandData("products/1003", document={"Name": "testsDelete", "Category": "testing"},
                                    metadata={"Raven-Python-Type": "Products"})
put6 = commands_data.PutCommandData("products/1004", document={"Name": "testsDelete", "Category": "testing"},
                                    metadata={"Raven-Python-Type": "Products"})
delete = commands_data.DeleteCommandData("products/1000")

option = BulkOperationOption(allow_stale=False)
patch = ScriptedPatchRequest("this.FirstName = 'Idan';")
patch2 = commands_data.ScriptedPatchCommandData("employees/1", patch)

maps = set()
maps.add("""from region in docs.Regions
                    select new
                    {
                        region.Name,region.Territories
                    }""")
index = IndexDefinition(maps)


class Foo:
    def __init__(self, name):
        self.name = name
        self.supplier = "s"


class FooBar:
    def __init__(self, name):
        self.name = name
        self.supplier = "bar"


class Product:
    def __init__(self, name):
        self.name = name
        self.supplier = "product"


def g(name):
    return name + 's'


if __name__ == "__main__":
    print("Start Testing")
    store = DocumentStore("http://localhost.fiddler:8080", "IdanDB")
    store.initialize()
    foo = Foo("testing_store")
    foo_bar = FooBar("testing foo bar")
    product = Product("product")
    with store.open_session() as session:
        session.store(foo)
        session.store(foo_bar)
        session.store(product)
        session.store(Foo("testing_store with extra foo"))
        session.save_changes()

    # store.database_commands.put("o/100", Utils.capitalize_dict_keys(g),
    #                             {"Raven-Entity-Name": "Foos", "Raven-Python_type": str(c.__class__)})
