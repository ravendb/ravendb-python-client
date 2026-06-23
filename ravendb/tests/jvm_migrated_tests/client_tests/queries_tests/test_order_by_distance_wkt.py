import unittest

from ravendb.documents.session.tokens.query_tokens.definitions import OrderByToken


def _rql(token: OrderByToken) -> str:
    writer = []
    token.write_to(writer)
    return "".join(writer)


class TestOrderByDistanceWktRql(unittest.TestCase):
    """The WKT ascending distance factory must nest spatial.wkt(...) inside spatial.distance(field, ...),
    matching C# (OrderByToken.cs) and its three sibling factories. The bug emitted a stray ')' right
    after the field name: 'spatial.distance(loc), spatial.wkt(...)'."""

    def test_distance_ascending_wkt_nests_wkt_inside_distance(self):
        rql = _rql(OrderByToken.create_distance_ascending_wkt("loc", "p0", None))
        self.assertIn("spatial.distance(loc, spatial.wkt($p0))", rql)
        self.assertNotIn("spatial.distance(loc),", rql)

    def test_distance_ascending_wkt_matches_descending_structure(self):
        asc = _rql(OrderByToken.create_distance_ascending_wkt("loc", "p0", None))
        desc = _rql(OrderByToken.create_distance_descending_wkt("loc", "p0", None))
        # Identical apart from the trailing descending marker.
        self.assertEqual(asc, desc.replace(" desc", ""))

    def test_distance_ascending_wkt_with_round_factor(self):
        rql = _rql(OrderByToken.create_distance_ascending_wkt("loc", "p0", "p1"))
        self.assertIn("spatial.distance(loc, spatial.wkt($p0), $p1)", rql)


if __name__ == "__main__":
    unittest.main()
