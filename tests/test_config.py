import os
import tempfile
import unittest
from pathlib import Path

from spark_telemetry_analyzer.config import Settings, load_settings, settings_from_mapping
from spark_telemetry_analyzer.errors import ConfigError

ROOT = Path(__file__).resolve().parent.parent


class ConfigTest(unittest.TestCase):
    def write(self, text):
        tmp = tempfile.NamedTemporaryFile("w", suffix=".toml", delete=False)
        self.addCleanup(os.unlink, tmp.name)
        tmp.write(text)
        tmp.close()
        return tmp.name

    def test_defaults(self):
        self.assertEqual(load_settings(None), Settings())

    def test_overrides(self):
        path = self.write('[thresholds]\ngc_ratio_warn = 0.2\nsmall_task_min_count = 50\n[analysis]\ntop_n = 3\ndisabled_rules = ["data_skew"]\n')
        s = load_settings(path)
        self.assertEqual(s.thresholds.gc_ratio_warn, 0.2)
        self.assertEqual(s.thresholds.small_task_min_count, 50)
        self.assertEqual(s.top_n, 3)
        self.assertEqual(s.disabled_rules, frozenset({"DATA_SKEW"}))

    def test_env_var(self):
        path = self.write("[analysis]\ntop_n = 7\n")
        os.environ["STA_CONFIG"] = path
        self.addCleanup(os.environ.pop, "STA_CONFIG", None)
        self.assertEqual(load_settings().top_n, 7)

    def test_validation_errors(self):
        bad = [
            "[thresholds]\nnope = 1\n",
            '[thresholds]\ngc_ratio_warn = "high"\n',
            "[thresholds]\ngc_ratio_warn = -1\n",
            "[analysis]\nnope = 1\n",
            "[analysis]\nreservoir_size = 5\n",
            '[analysis]\ndisabled_rules = "X"\n',
            "[mystery]\na = 1\n",
            "[analysis\n",
        ]
        for text in bad:
            with self.subTest(text=text), self.assertRaises(ConfigError):
                load_settings(self.write(text))
        with self.assertRaises(ConfigError):
            load_settings("/no/such/file.toml")

    def test_shipped_example_config_is_valid(self):
        s = load_settings(ROOT / "config" / "sta.example.toml")
        self.assertIsInstance(s, Settings)
        self.assertEqual(settings_from_mapping({}), Settings())


if __name__ == "__main__":
    unittest.main()
