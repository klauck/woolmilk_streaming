import time
import unittest
from tests.test_utils import TestUtil


class TestNexmarkQueries(unittest.TestCase):
    def setUp(self):
        self.util = TestUtil("test_nexmark_queries", 1000)
        self.util.load_table("bid")
        self.util.load_table("auction")
        self.util.load_table("person")
        self.util.load_table("category")

    def test_q1(self):
        self.util.sql(
            "SELECT auction, price * 0.85, bidder, date_time FROM bid",
            showOnly=True
        )

    def test_q2(self):
        self.util.sql(
            "SELECT auction, price "
            "FROM Bid "
            "WHERE auction = 1007 OR auction = 1020 "
            "OR auction = 2001 OR auction = 2019 OR auction = 2087",
            showOnly=True
        )


    def test_q3(self):
        self.util.sql(
            "SELECT P.name, P.city, P.state, A.id "
            "FROM Auction A, Person P "
            "WHERE A.seller = P.id "
            "AND (P.state = 'or' OR P.state = 'id' OR P.state = 'ca') "
            "AND A.category = 10",
            showOnly=True
        )

    def test_q4(self):
        current_time = time.time() * 1000 + 100000

        self.util.sql(
            "SELECT AVG(Q.final) "
            "FROM Category C, "
            "    (SELECT MAX(B.price) AS final, A.category "
            "    FROM Auction A, Bid B "
            f"    WHERE A.id=B.auction AND B.date_time < A.expires "
            f"    AND A.expires < {current_time} "
            "    GROUP BY A.id, A.category) Q "
            "WHERE Q.category = C.id "
            "GROUP BY C.id",
            showOnly=True
        )


if __name__ == "__main__":
    unittest.main()
