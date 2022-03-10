from ravendb.tests.test_base import TestBase


class OuterClass:
    def __init__(self, inner_class_matrix=None, inner_classes=None, a=None, inner_class=None, middle_class=None):
        self.inner_class_matrix = inner_class_matrix
        self.inner_classes = inner_classes
        self.a = a
        self.inner_class = inner_class
        self.middle_class = middle_class


class InnerClass:
    def __init__(self, a):
        self.a = a


class MiddleClass:
    def __init__(self, inner_class_a):
        self.a = inner_class_a


class TestRavenDB10641(TestBase):
    def setUp(self):
        super(TestRavenDB10641, self).setUp()

    def test_what_changed_when_property_in_array_changed_should_return_with_relevant_path(self):
        with self.store.open_session() as session:
            ic = InnerClass("innerValue")
            key = "docs/1"
            doc = OuterClass(a="outerValue", inner_class=ic, inner_classes=[ic])
            session.store(doc, key)
            session.save_changes()
            doc.inner_classes[0].a = "newInnerValue"
            changes = session.advanced.what_changed()
            changed_paths = list(map(lambda change: change["field_path"], changes["docs/1"]))
            self.assertSequenceContainsElements(changed_paths, "inner_classes[0]")

    def test_what_changed_when_property_of_inner_property_changed_to_null_should_return_property_name_plus_path(self):
        with self.store.open_session() as session:
            ic = InnerClass("innerValue")
            key = "docs/1"
            doc = OuterClass(a="outerValue", inner_class=ic, inner_classes=[ic])
            session.store(doc, key)
            session.save_changes()

            doc.inner_class.a = None
            changes = session.advanced.what_changed()
            changed_paths = list(map(lambda change: change["field_path"], changes["docs/1"]))
            self.assertSequenceContainsElements(changed_paths, "inner_class")

    def test_what_changed_when_inner_property_changed_should_return_the_property_name_plus_path(self):
        with self.store.open_session() as session:
            ic = InnerClass("innerValue")
            key = "docs/1"
            doc = OuterClass(a="outerValue", inner_class=ic, inner_classes=[ic])
            session.store(doc, key)
            session.save_changes()

            doc.inner_class.a = "newInnerValue"
            changes = session.advanced.what_changed()
            changed_paths = list(map(lambda change: change["field_path"], changes["docs/1"]))
            self.assertSequenceContainsElements(changed_paths, "inner_class")

    def test_what_changed_when_outer_property_changed_field_path_should_be_empty(self):
        with self.store.open_session() as session:
            ic = InnerClass("innerValue")
            key = "docs/1"
            doc = OuterClass(a="outerValue", inner_class=ic, inner_classes=[ic])
            session.store(doc, key)
            session.save_changes()

            doc.a = "newOuterValue"
            changes = session.advanced.what_changed()
            changed_paths = list(map(lambda change: change["field_path"], changes["docs/1"]))
            self.assertSequenceContainsElements(changed_paths, "")

    def test_what_changed_when_all_named_a_properties_changed_should_return_different_paths(self):
        with self.store.open_session() as session:
            ic = InnerClass("innerValue")
            ic2 = InnerClass("oldValue")
            mc = MiddleClass(ic2)
            ic3 = InnerClass("oldValue")
            key = "docs/1"
            doc = OuterClass(
                a="outerValue", inner_class=ic, middle_class=mc, inner_classes=[ic2], inner_class_matrix=[[ic3]]
            )
            session.store(doc, key)
            session.save_changes()

            doc.a = "newOuterValue"
            doc.inner_class.a = "newInnerValue"
            doc.middle_class.a = InnerClass("")
            doc.inner_classes[0].a = "newValue"
            doc.inner_class_matrix[0][0].a = "newValue"
            changes = session.advanced.what_changed()
            changed_paths = list(map(lambda change: change["field_path"], changes["docs/1"]))
            self.assertSequenceContainsElements(
                changed_paths, "", "inner_class", "inner_classes[0]", "inner_class_matrix[0][0]"
            )

    def test_what_changed_when_inner_property_changed_from_null_should_return_the_property_name_plus_path(self):
        with self.store.open_session() as session:
            ic = InnerClass(None)
            key = "docs/1"
            doc = OuterClass(a="outerValue", inner_class=ic, inner_classes=[ic])
            session.store(doc, key)
            session.save_changes()

            doc.inner_class.a = "newInnerValue"

            changes = session.advanced.what_changed()
            changed_paths = list(map(lambda change: change["field_path"], changes["docs/1"]))
            self.assertSequenceContainsElements(changed_paths, "inner_class")

    def test_what_changed_when_in_matrix_changed_should_return_with_relevant_path(self):
        with self.store.open_session() as session:
            ic = InnerClass("oldValue")
            key = "docs/1"
            doc = OuterClass([[ic]])
            session.store(doc, key)
            session.save_changes()

            doc.inner_class_matrix[0][0].a = "newValue"

            changes = session.advanced.what_changed()
            changes_paths = list(map(lambda change: change["field_path"], changes["docs/1"]))
            self.assertSequenceContainsElements(changes_paths, "inner_class_matrix[0][0]")

    def test_what_changed_when_array_property_in_array_changed_from_null_should_return_relevant_path(self):
        with self.store.open_session() as session:
            key = "docs/1"
            doc = OuterClass([[]])
            session.store(doc, key)
            session.save_changes()

            doc.inner_class_matrix[0] = [InnerClass(None)]

            changes = session.advanced.what_changed()
            changes_paths = list(map(lambda change: change["field_path"], changes[key]))
            self.assertSequenceContainsElements(changes_paths, "inner_class_matrix[0]")
