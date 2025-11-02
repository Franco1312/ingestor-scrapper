"""
Tests for health runner module.
"""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ingestor_scrapper.health import runner


class TestHealthRunner:
    """Tests for run_health_check function."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    @patch("ingestor_scrapper.health.runner.config.load_config")
    @patch("ingestor_scrapper.health.runner.checks.fetch")
    @patch("ingestor_scrapper.health.runner.checks.checksum_sha256")
    @patch("ingestor_scrapper.health.runner.store.compare_with_history")
    @patch("ingestor_scrapper.health.runner.store.update_metrics")
    @patch("ingestor_scrapper.health.runner.notify.notify")
    def test_run_health_check_success_info(
        self,
        mock_notify,
        mock_update,
        mock_compare,
        mock_checksum,
        mock_fetch,
        mock_config,
        temp_dir,
    ):
        """Test successful health check returns INFO."""
        # Setup mocks
        mock_config.return_value = {
            "test_site": {
                "url": "https://example.com",
                "type": "html",
                "selectors": [],
                "min_bytes": 0,
                "expected_columns": [],
                "min_rows": 0,
                "content_type": None,
                "checksum_window": 10,
                "notify": {},
            }
        }
        mock_fetch.return_value = (b"<html></html>", {}, 200, "https://example.com")
        mock_checksum.return_value = "abc123"
        mock_compare.return_value = {
            "changed": False,
            "size_change_pct": 0.0,
            "size_dropped_50pct": False,
            "anomaly": False,
        }
        mock_notify.return_value = 0
        
        # Run check
        exit_code = runner.run_health_check(
            "test_site", config_path=str(temp_dir / "config.json")
        )
        
        # Verify
        assert exit_code == 0
        mock_fetch.assert_called_once()
        mock_checksum.assert_called_once()
        mock_update.assert_called_once()
        mock_notify.assert_called_once()

    @patch("ingestor_scrapper.health.runner.config.load_config")
    def test_run_health_check_site_not_found(self, mock_config, temp_dir):
        """Test health check for non-existent site returns FAIL."""
        mock_config.return_value = {}
        
        exit_code = runner.run_health_check("nonexistent")
        
        assert exit_code == 3

    @patch("ingestor_scrapper.health.runner.config.load_config")
    @patch("ingestor_scrapper.health.runner.checks.fetch")
    def test_run_health_check_fetch_fails(
        self, mock_fetch, mock_config, temp_dir
    ):
        """Test health check when fetch fails."""
        mock_config.return_value = {
            "test_site": {
                "url": "https://example.com",
                "type": "html",
                "selectors": [],
                "min_bytes": 0,
                "notify": {},
            }
        }
        mock_fetch.side_effect = Exception("Network error")
        
        exit_code = runner.run_health_check("test_site")
        
        assert exit_code == 3

    @patch("ingestor_scrapper.health.runner.config.load_config")
    @patch("ingestor_scrapper.health.runner.checks.fetch")
    @patch("ingestor_scrapper.health.runner.checks.checksum_sha256")
    @patch("ingestor_scrapper.health.runner.store.compare_with_history")
    @patch("ingestor_scrapper.health.runner.store.update_metrics")
    @patch("ingestor_scrapper.health.runner.notify._print_stdout")
    def test_run_health_check_dry_run(
        self,
        mock_print,
        mock_update,
        mock_compare,
        mock_checksum,
        mock_fetch,
        mock_config,
        temp_dir,
    ):
        """Test dry-run mode doesn't send notifications."""
        mock_config.return_value = {
            "test_site": {
                "url": "https://example.com",
                "type": "html",
                "selectors": [],
                "min_bytes": 0,
                "notify": {},
            }
        }
        mock_fetch.return_value = (b"<html></html>", {}, 200, "https://example.com")
        mock_checksum.return_value = "abc123"
        mock_compare.return_value = {
            "changed": False,
            "size_change_pct": 0.0,
            "size_dropped_50pct": False,
            "anomaly": False,
        }
        
        exit_code = runner.run_health_check("test_site", dry_run=True)
        
        # In dry-run, print is called instead of notify
        mock_print.assert_called_once()
        assert exit_code == 0


class TestRunChecksForType:
    """Tests for _run_checks_for_type function."""

    def test_run_checks_html(self):
        """Test checks for HTML type."""
        site_config = {
            "url": "https://example.com",
            "type": "html",
            "selectors": ["div.test"],
            "min_bytes": 0,
            "content_type": "text/html",
        }
        
        summary = runner._run_checks_for_type(
            site_config,
            b"<html><body><div class='test'></div></body></html>",
            {"Content-Type": "text/html"},
            200,
            "https://example.com",
        )
        
        assert "checks" in summary
        assert summary["checks"]["status"] is True
        assert summary["checks"]["min_bytes"] is True

    def test_run_checks_csv(self):
        """Test checks for CSV type."""
        site_config = {
            "url": "https://example.com",
            "type": "csv",
            "expected_columns": ["name", "age"],
            "min_rows": 1,
            "min_bytes": 0,
        }
        
        summary = runner._run_checks_for_type(
            site_config,
            b"name,age\nJohn,30\nJane,25",
            {},
            200,
            "https://example.com",
        )
        
        assert "checks" in summary
        assert summary["checks"]["status"] is True

    def test_run_checks_excel(self):
        """Test checks for Excel type."""
        site_config = {
            "url": "https://example.com",
            "type": "excel",
            "expected_columns": ["Sheet1"],
            "min_rows": 10,
            "min_bytes": 0,
        }
        
        summary = runner._run_checks_for_type(
            site_config,
            b"dummy excel content",
            {},
            200,
            "https://example.com",
        )
        
        assert "checks" in summary
        assert summary["checks"]["status"] is True


class TestDetermineLevel:
    """Tests for _determine_level function."""

    def test_determine_level_fail_status(self):
        """Test FAIL level for bad status."""
        summary = {"checks": {"status": False}}
        historical = {}
        
        level = runner._determine_level(summary, historical)
        
        assert level == "FAIL"

    def test_determine_level_fail_min_bytes(self):
        """Test FAIL level for insufficient bytes."""
        summary = {"checks": {"status": True, "min_bytes": False}}
        historical = {}
        
        level = runner._determine_level(summary, historical)
        
        assert level == "FAIL"

    def test_determine_level_warn_anomaly(self):
        """Test WARN level for anomaly."""
        summary = {"checks": {"status": True, "min_bytes": True}}
        historical = {"anomaly": True}
        
        level = runner._determine_level(summary, historical)
        
        assert level == "WARN"

    def test_determine_level_info_all_pass(self):
        """Test INFO level when all checks pass."""
        summary = {"checks": {"status": True, "min_bytes": True}}
        historical = {}
        
        level = runner._determine_level(summary, historical)
        
        assert level == "INFO"

