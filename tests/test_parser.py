import gzip
import io
import tempfile
import unittest
import zipfile
from pathlib import Path

from spark_telemetry_analyzer.errors import ParseError, UnsupportedFormatError
from spark_telemetry_analyzer.models import ParseStats
from spark_telemetry_analyzer.parser import discover_logs, iter_events, load_application

from .helpers import log_bytes


class ParserTest(unittest.TestCase):
    def test_plain_gzip_zip_and_stream_agree(self):
        raw = log_bytes("skew", 0.3)
        expected = load_application(raw).stages.keys()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            z.writestr("eventLogs/events_1_app", raw)
        for source in (gzip.compress(raw), buf.getvalue(), io.BytesIO(raw)):
            self.assertEqual(load_application(source).stages.keys(), expected)

    def test_files_and_rolling_directories(self):
        raw = log_bytes("skew", 0.3).decode().splitlines()
        with tempfile.TemporaryDirectory() as tmp:
            d = Path(tmp) / "eventlog_v2_app1"
            d.mkdir()
            third = len(raw) // 3
            # written out of order on purpose: events_10 must sort after events_2
            (d / "events_10_app1").write_text("\n".join(raw[2 * third:]) + "\n")
            (d / "events_1_app1").write_text("\n".join(raw[:third]) + "\n")
            (d / "events_2_app1").write_text("\n".join(raw[third:2 * third]) + "\n")
            (d / "appstatus_app1").write_text("")
            app = load_application(d)
            self.assertEqual(app.parse_stats.files, 3)
            self.assertTrue(app.has_app_end)
            self.assertEqual(discover_logs([tmp]), [d])

    def test_truncated_last_line_is_tolerated(self):
        lines = log_bytes("skew", 0.3).decode().splitlines()
        data = ("\n".join(lines[:-1]) + "\n" + lines[-1][:30]).encode()
        app = load_application(data)
        self.assertEqual(app.parse_stats.bad_lines, 1)
        self.assertFalse(app.has_app_end)

    def test_irrelevant_events_skipped_without_decoding(self):
        stats = ParseStats()
        data = b'{"Event":"SparkListenerBlockManagerAdded","x":1}\n{"Event":"SparkListenerApplicationEnd","Timestamp":5}\n\n'
        events = list(iter_events(data, stats))
        self.assertEqual(len(events), 1)
        self.assertEqual((stats.events_skipped, stats.lines), (1, 2))

    def test_rejects_non_event_logs(self):
        for payload in (b"hello world\n", b"", b'{"foo": 1}\n', b"[1,2,3]\n"):
            with self.assertRaises(ParseError):
                load_application(payload)
        with self.assertRaises(ParseError):
            load_application(b"not json\n" * 200)

    def test_unsupported_codecs_give_actionable_errors(self):
        with self.assertRaises(UnsupportedFormatError) as ctx:
            load_application(b"\x82SNAPPY\x00" + b"0" * 32)
        self.assertIn("compress", str(ctx.exception))
        try:
            import zstandard  # noqa: F401
        except ImportError:
            with self.assertRaises(UnsupportedFormatError) as ctx:
                load_application(b"\x28\xb5\x2f\xfd" + b"0" * 32)
            self.assertIn("zstd", str(ctx.exception))

    def test_missing_path_and_empty_directory(self):
        with self.assertRaises(ParseError):
            load_application(Path("/definitely/not/here"))
        with self.assertRaises(ParseError):
            discover_logs(["/definitely/not/here"])
        with tempfile.TemporaryDirectory() as tmp:
            with self.assertRaises(ParseError):
                load_application(Path(tmp))

    def test_discover_skips_hidden_and_checksums(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name in ("app1", ".app1.crc", "app2.inprogress", ".hidden"):
                (Path(tmp) / name).write_text("x")
            names = sorted(p.name for p in discover_logs([tmp]))
            self.assertEqual(names, ["app1", "app2.inprogress"])
            names = sorted(p.name for p in discover_logs([tmp], include_inprogress=False))
            self.assertEqual(names, ["app1"])


if __name__ == "__main__":
    unittest.main()
