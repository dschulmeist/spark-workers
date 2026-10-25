import tempfile
import unittest

from spark_workers.manager import Manager


class ListingTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.manager = Manager(self.temporary.name, {"backends": {}}, None)
        self.addCleanup(self.manager.db.close)
        for index in range(15):
            self.manager.save(
                {
                    "id": str(index),
                    "request_id": str(index),
                    "backend": "test",
                    "status": "failed" if index % 2 else "completed",
                    "repository": "/tmp/project" if index < 10 else "/tmp/project-other",
                    "created_at": index * 10,
                    "goal": "x" * 1000,
                    "summary": "full evidence must not appear",
                    "error": "e" * 500,
                }
            )

    def test_combined_filters_include_time_boundaries(self):
        result = self.manager.list_tasks(
            status="failed", repository="/tmp/project", created_after=30, created_before=70
        )
        self.assertEqual([task["id"] for task in result["tasks"]], ["7", "5", "3"])
        self.assertEqual(result["total"], 3)
        self.assertIsNone(result["next_offset"])

    def test_pagination_finds_tasks_beyond_recent_status_list(self):
        task_ids = []
        offset = 0
        while offset is not None:
            result = self.manager.list_tasks(limit=4, offset=offset)
            self.assertEqual(result["total"], 15)
            self.assertLessEqual(len(result["tasks"]), 4)
            task_ids.extend(task["id"] for task in result["tasks"])
            offset = result["next_offset"]
        self.assertEqual(task_ids, [str(index) for index in reversed(range(15))])

    def test_list_is_compact_and_filters_are_parameterized(self):
        task = self.manager.list_tasks(limit=1)["tasks"][0]
        self.assertLessEqual(len(task["goal"]), 180)
        self.assertLessEqual(len(task["error"]), 240)
        self.assertNotIn("summary", task)
        result = self.manager.list_tasks(repository="/tmp/project' OR 1=1 --")
        self.assertEqual(result, {"tasks": [], "total": 0, "next_offset": None})

    def test_invalid_time_range_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "created_after"):
            self.manager.list_tasks(created_after=100, created_before=10)
