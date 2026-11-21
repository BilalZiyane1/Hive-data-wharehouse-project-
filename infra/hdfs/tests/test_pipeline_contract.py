import ast
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class PipelineContractTest(unittest.TestCase):
    def test_python_sources_parse(self):
        sources = list((ROOT / "app").glob("*.py"))
        self.assertGreaterEqual(len(sources), 10)
        for source in sources:
            ast.parse(source.read_text())

    def test_hive_external_tables_are_declared(self):
        sql = (ROOT / "hive" / "warehouse_tables.sql").read_text()
        for table in (
            "bronze_transactions",
            "silver_transactions",
            "silver_quarantine",
            "gold_daily_transaction_metrics",
            "gold_enriched_transactions",
        ):
            self.assertIn(table, sql)

    def test_hive_demo_queries_exist_and_avoid_yarn(self):
        sql = (ROOT / "hive" / "demo_queries.sql").read_text()
        self.assertIn("SET mapreduce.framework.name=local", sql)
        self.assertIn("SELECT COUNT(*) AS gold_row_count", sql)
        self.assertIn("GROUP BY currency", sql)

    def test_lightweight_profile_limits_nodes(self):
        compose = (ROOT / "docker-compose.yml").read_text()
        self.assertIn('SPARK_WORKER_MEMORY: 1g', compose)
        self.assertIn('HDFS_DATANODE_OPTS: "-Xmx160m"', compose)
        self.assertIn('profiles: ["full"]', compose)


if __name__ == "__main__":
    unittest.main()
