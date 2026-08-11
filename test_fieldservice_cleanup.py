import unittest
from datetime import date
from unittest.mock import patch

from fieldservice_cleanup import run_sweep


class CleanupDecisionTest(unittest.TestCase):
    def test_only_closed_and_followed_up_old_photos_are_published(self):
        records = [{"photo_id": "photo-old-ready", "dispatch_status": "closed", "technician_follow_up": "complete", "photo_taken_on": "2026-06-01"}, {"photo_id": "photo-old-pending", "dispatch_status": "closed", "technician_follow_up": "pending", "photo_taken_on": "2026-06-01"}]
        with patch("fieldservice_cleanup.publish_cleanup") as publish:
            result = run_sweep(records, today=date(2026, 7, 10))
        self.assertEqual(result["deleted_photo_ids"], ["photo-old-ready"])
        self.assertTrue(result["published"])
        publish.assert_called_once_with(["photo-old-ready"])


if __name__ == "__main__":
    unittest.main()
