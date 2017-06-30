from pyravendb.store.document_store import DocumentStore


class Dog(object):
    def __init__(self, Name, Id=None):
        self.Id = Id
        self.Name = Name


class Child(object):
    def __init__(self, Name, Id=None):
        self.Id = Id
        self.Name = Name


class Node(object):
    def __init__(self, changed):
        self.changed = changed


if __name__ == "__main__":
    with DocumentStore("http://localhost.fiddler:8080", "NorthWind") as store:
        store.initialize()

        with store.open_session() as session:
            dog = Dog("Faz")
            child = Child("Ilay")
            session.store(dog)
            session.store(child)
            session.save_changes()

        with store.open_session() as session:
            child = session.load("children/1")
            dog = session.load("dogs/1")

            print("Child:{0}".format(child.Name))
            print("Dog:{0}".format(dog.Name))
