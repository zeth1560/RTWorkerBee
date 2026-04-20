"""Tests for Pickle Planner booking match timestamp normalization (stdlib only)."""

from __future__ import annotations

import unittest

from pickle_planner import (
    BookingMatchResult,
    normalize_optional_timestamp,
    parse_booking_match_response,
)


class NormalizeOptionalTimestampTests(unittest.TestCase):
    def test_none_and_blank(self) -> None:
        self.assertIsNone(normalize_optional_timestamp(None))
        self.assertIsNone(normalize_optional_timestamp(""))
        self.assertIsNone(normalize_optional_timestamp("   "))
        self.assertIsNone(normalize_optional_timestamp("null"))

    def test_z_suffix_utc(self) -> None:
        out = normalize_optional_timestamp("2026-04-20T14:00:00Z")
        self.assertIsNotNone(out)
        assert out is not None
        self.assertTrue(out.endswith("+00:00"), msg=out)
        self.assertIn("2026-04-20T14:00:00", out)

    def test_numeric_epoch(self) -> None:
        out = normalize_optional_timestamp(1713621600.0)
        self.assertIsNotNone(out)


class ParseBookingMatchPayloadTests(unittest.TestCase):
    def test_minimal_json_shape(self) -> None:
        r = parse_booking_match_response(
            {
                "booking_id": "550e8400-e29b-41d4-a716-446655440000",
                "start_time": "2026-04-20T14:00:00Z",
                "end_time": "2026-04-20T16:00:00Z",
            }
        )
        self.assertIsInstance(r, BookingMatchResult)
        self.assertEqual(r.booking_id, "550e8400-e29b-41d4-a716-446655440000")
        self.assertIsNotNone(r.start_time)
        self.assertIsNotNone(r.end_time)

    def test_missing_times(self) -> None:
        r = parse_booking_match_response({"booking_id": "abc"})
        self.assertEqual(r.booking_id, "abc")
        self.assertIsNone(r.start_time)
        self.assertIsNone(r.end_time)

    def test_non_dict_body(self) -> None:
        r = parse_booking_match_response([])
        self.assertIsNone(r.booking_id)


if __name__ == "__main__":
    unittest.main()
