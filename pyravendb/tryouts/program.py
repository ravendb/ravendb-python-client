from pyravendb.store.document_store import DocumentStore


class User:
    def __init__(self, name):
        self.name


if __name__ == "__main__":
    with DocumentStore(urls=["http://localhost:8080"], database="NorthWind") as store:
        store.initialize()
        with store.open_session() as session:
            session.store(User("Idan"))
            session.save_changes()
