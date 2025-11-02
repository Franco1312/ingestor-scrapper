"""
Tests for health store module.
"""

import json
import tempfile
from pathlib import Path

import pytest

from ingestor_scrapper.health import store


class TestHealthStore:
    """Tests for health store functions."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        with tempfile.TemporaryDirectory() as tmpdir:
            yield Path(tmpdir)

    def test_load_metrics_not_exist(self, temp_dir):
        """Test loading non-existent metrics returns empty dict."""
        metrics = store.load_metrics(metrics_dir=temp_dir)
        assert metrics == {}

    def test_save_and_load_metrics(self, temp_dir):
        """Test saving and loading metrics."""
        test_metrics = {
            "site1": {
                "checksum": "abc123",
                "last_size": 1000,
                "history_checksums": ["abc123"],
            }
        }
        store.save_metrics(test_metrics, metrics_dir=temp_dir)
        
        loaded = store.load_metrics(metrics_dir=temp_dir)
        assert loaded == test_metrics

    def test_update_metrics_new_site(self, temp_dir):
        """Test updating metrics for new site."""
        metrics_dir = temp_dir
        metrics = store.update_metrics(
            "site1",
            "abc123",
            1000,
            metrics_dir=metrics_dir,
        )
        
        assert metrics["checksum"] == "abc123"
        assert metrics["last_size"] == 1000
        assert "abc123" in metrics["history_checksums"]
        
        # Verify saved
        loaded = store.load_metrics(metrics_dir=metrics_dir)
        assert "site1" in loaded

    def test_update_metrics_existing_site(self, temp_dir):
        """Test updating metrics for existing site."""
        metrics_dir = temp_dir
        
        # Initial update
        store.update_metrics("site1", "abc123", 1000, metrics_dir=metrics_dir)
        
        # Update again with new values
        metrics = store.update_metrics(
            "site2",
            "def456",
            2000,
            metrics_dir=metrics_dir,
        )
        
        assert metrics["checksum"] == "def456"
        assert metrics["last_size"] == 2000

    def test_update_metrics_checksum_window(self, temp_dir):
        """Test checksum history is limited to window."""
        metrics_dir = temp_dir
        
        # Add many checksums
        for i in range(20):
            store.update_metrics(
                f"site1",
                f"checksum{i}",
                1000,
                checksum_window=10,
                metrics_dir=metrics_dir,
            )
        
        loaded = store.load_metrics(metrics_dir=metrics_dir)
        history = loaded["site1"]["history_checksums"]
        
        # Should only have last 10
        assert len(history) == 10
        assert history[-1] == "checksum19"

    def test_update_metrics_same_checksum(self, temp_dir):
        """Test same checksum isn't added twice to history."""
        metrics_dir = temp_dir
        
        # Update twice with same checksum
        store.update_metrics("site1", "abc123", 1000, metrics_dir=metrics_dir)
        store.update_metrics("site1", "abc123", 1000, metrics_dir=metrics_dir)
        
        loaded = store.load_metrics(metrics_dir=metrics_dir)
        history = loaded["site1"]["history_checksums"]
        
        # Should only have one entry
        assert len(history) == 1
        assert history[0] == "abc123"

    def test_get_site_metrics_existing(self, temp_dir):
        """Test getting metrics for existing site."""
        metrics_dir = temp_dir
        store.update_metrics("site1", "abc123", 1000, metrics_dir=metrics_dir)
        
        metrics = store.get_site_metrics("site1", metrics_dir=metrics_dir)
        assert metrics["checksum"] == "abc123"
        assert metrics["last_size"] == 1000

    def test_get_site_metrics_not_exist(self, temp_dir):
        """Test getting metrics for non-existent site."""
        metrics = store.get_site_metrics("nonexistent", metrics_dir=temp_dir)
        assert metrics == {}

    def test_compare_with_history_no_history(self, temp_dir):
        """Test comparison with no history."""
        result = store.compare_with_history(1000, "abc123", "site1", metrics_dir=temp_dir)
        assert result["changed"] is False
        assert result["size_change_pct"] == 0.0
        assert result["size_dropped_50pct"] is False
        assert result["anomaly"] is False

    def test_compare_with_history_changed(self, temp_dir):
        """Test comparison with changed checksum."""
        metrics_dir = temp_dir
        store.update_metrics("site1", "old123", 1000, metrics_dir=metrics_dir)
        
        result = store.compare_with_history(1000, "new456", "site1", metrics_dir=metrics_dir)
        assert result["changed"] is True

    def test_compare_with_history_size_drop(self, temp_dir):
        """Test comparison with size drop."""
        metrics_dir = temp_dir
        store.update_metrics("site1", "abc123", 1000, metrics_dir=metrics_dir)
        
        result = store.compare_with_history(400, "abc123", "site1", metrics_dir=metrics_dir)
        assert result["size_dropped_50pct"] is True

    def test_compare_with_history_anomaly(self, temp_dir):
        """Test anomaly detection."""
        metrics_dir = temp_dir
        store.update_metrics("site1", "old123", 1000, metrics_dir=metrics_dir)
        
        result = store.compare_with_history(400, "new456", "site1", metrics_dir=metrics_dir)
        assert result["anomaly"] is True
        assert result["changed"] is True
        assert result["size_dropped_50pct"] is True

