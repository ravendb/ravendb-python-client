from pyravendb.store.document_store import DocumentStore
from pyravendb.subscriptions.data import SubscriptionCreationOptions, SubscriptionWorkerOptions
from pyravendb.subscriptions.data import SubscriptionOpeningStrategy
from pyravendb.custom_exceptions.exceptions import SubscriptionClosedException
from functools import partial


# For testing C# compatibility
class Location:
    def __init__(self, Latitude, Longitude):
        self.Latitude = Latitude
        self.Longitude = Longitude


class Address:
    def __init__(self, City, Country, Line1, Line2, Location, PostalCode, Region):
        self.City = City
        self.Country = Country
        self.Line1 = Line1
        self.Line2 = Line2
        self.Location = Location
        self.PostalCode = PostalCode
        self.Region = Region


class Contact:
    def __init__(self, Name, Title):
        self.Name = Name
        self.Title = Title


class Company:
    def __init__(self, Address, Contact, ExternalId, Fax, Name, Phone):
        self.Address = Address
        self.Contact = Contact
        self.ExternalId = ExternalId
        self.Fax = Fax
        self.Name = Name
        self.Phone = Phone


class User:
    def __init__(self, name, age=0, dog=None):
        self.name = name
        self.dog = dog
        self.age = age


class Dog:
    def __init__(self, name, brand):
        self.name = name
        self.brand = brand

    def __str__(self):
        return "The dog name is " + self.name + " and his brand is " + self.brand


if __name__ == "__main__":
    with DocumentStore(urls=["http://localhost.fiddler:8080"], database="demo") as store:
        store.initialize()
        companies = []
        subscription_creation_options = SubscriptionCreationOptions("From Orders")
        subscription_name = store.subscriptions.create(subscription_creation_options)

        worker_options = SubscriptionWorkerOptions(subscription_name,
                                                   strategy=SubscriptionOpeningStrategy.take_over,
                                                   close_when_no_docs_left=True)
        with store.subscriptions.get_subscription_worker(worker_options) as subscription_worker:
            try:
                subscription_worker.run(
                    partial(lambda batch, x=companies: x.extend([item.raw_result for item in batch.items]))).join()
            except SubscriptionClosedException:
                # That's expected
                pass

    print(len(companies))
