import os
import subprocess
import time
import unittest

import datafusion

import woolmilk.source_node


class TestNexmarkQueries(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.ctx = datafusion.SessionContext()

        bid = woolmilk.source_node.generate_table(1000, "bid")
        cls.ctx.register_record_batches("bid", [bid.to_batches()])

        auction = woolmilk.source_node.generate_table(1000, "auction")
        cls.ctx.register_record_batches("auction", [auction.to_batches()])

        person = woolmilk.source_node.generate_table(1000, "person")
        cls.ctx.register_record_batches("person", [person.to_batches()])

        result_df = cls.ctx.sql("SELECT DISTINCT(category) AS id FROM auction").collect()
        cls.ctx.register_record_batches("category", [result_df])

    def test_q1(self):
        result_df = TestNexmarkQueries.ctx.sql(
            "SELECT auction, price * 0.85, bidder, date_time FROM bid"
        )
        result_df.show()

    def test_q2(self):
        result_df = TestNexmarkQueries.ctx.sql(
            "SELECT auction, price "
            "FROM Bid "
            "WHERE auction = 1007 OR auction = 1020 "
            "OR auction = 2001 OR auction = 2019 OR auction = 2087"
        )
        result_df.show()

    def test_q3(self):
        result_df = TestNexmarkQueries.ctx.sql(
            "SELECT P.name, P.city, P.state, A.id "
            "FROM Auction A, Person P "
            "WHERE A.seller = P.id "
            "AND (P.state = 'or' OR P.state = 'id' OR P.state = 'ca') "
            "AND A.category = 10"
        )
        result_df.show()

    def test_q4(self):
        current_time = time.time() * 1000 + 100000

        # result_df = TestNexmarkQueries.ctx.sql("SELECT MAX(B.price) AS final, A.category "
        # "    FROM Auction A, Bid B "
        # f"    WHERE A.id=B.auction AND B.date_time < A.expires AND A.expires < {current_time} "
        # "    GROUP BY A.id, A.category")

        result_df = TestNexmarkQueries.ctx.sql(
            "SELECT AVG(Q.final) "
            "FROM Category C, "
            "    (SELECT MAX(B.price) AS final, A.category "
            "    FROM Auction A, Bid B "
            f"    WHERE A.id=B.auction AND B.date_time < A.expires AND A.expires < {current_time} "
            "    GROUP BY A.id, A.category) Q "
            "WHERE Q.category = C.id "
            "GROUP BY C.id"
        )
        result_df.show()


if __name__ == "__main__":
    unittest.main()
