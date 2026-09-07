import subprocess
import sys
import time
import unittest

from e2e import startup_diagnostics


class StartupDiagnosticsTest(unittest.TestCase):
    def test_stops_live_process_before_reading_stderr(self):
        process = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        started = time.monotonic()
        try:
            self.assertEqual(startup_diagnostics(process), "")
            self.assertIsNotNone(process.poll())
            self.assertLess(time.monotonic() - started, 6)
        finally:
            if process.poll() is None:
                process.kill()
                process.wait(timeout=3)
            process.stderr.close()


if __name__ == "__main__":
    unittest.main()
