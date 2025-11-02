"""
Tests for health checks module.
"""

import pytest
from ingestor_scrapper.health import checks


class TestChecksStatus:
    """Tests for check_status function."""

    def test_check_status_200(self):
        """Test successful status code 200."""
        assert checks.check_status(200) is True

    def test_check_status_204(self):
        """Test successful status code 204."""
        assert checks.check_status(204) is True

    def test_check_status_299(self):
        """Test successful status code 299."""
        assert checks.check_status(299) is True

    def test_check_status_404(self):
        """Test failed status code 404."""
        assert checks.check_status(404) is False

    def test_check_status_500(self):
        """Test failed status code 500."""
        assert checks.check_status(500) is False


class TestChecksMinBytes:
    """Tests for check_min_bytes function."""

    def test_check_min_bytes_sufficient(self):
        """Test content meets minimum bytes requirement."""
        content = b"x" * 1000
        assert checks.check_min_bytes(content, 500) is True

    def test_check_min_bytes_exact(self):
        """Test content exactly meets minimum bytes requirement."""
        content = b"x" * 500
        assert checks.check_min_bytes(content, 500) is True

    def test_check_min_bytes_insufficient(self):
        """Test content doesn't meet minimum bytes requirement."""
        content = b"x" * 100
        assert checks.check_min_bytes(content, 500) is False

    def test_check_min_bytes_zero(self):
        """Test zero minimum bytes always passes."""
        content = b"x"
        assert checks.check_min_bytes(content, 0) is True


class TestChecksContentType:
    """Tests for check_content_type function."""

    def test_check_content_type_matches(self):
        """Test Content-Type matches expected value."""
        headers = {"Content-Type": "application/json; charset=utf-8"}
        assert checks.check_content_type(headers, "application/json") is True

    def test_check_content_type_contains(self):
        """Test Content-Type contains expected value."""
        headers = {"Content-Type": "application/vnd.ms-excel"}
        assert checks.check_content_type(headers, "excel") is True

    def test_check_content_type_case_insensitive(self):
        """Test Content-Type matching is case-insensitive."""
        headers = {"Content-Type": "TEXT/HTML"}
        assert checks.check_content_type(headers, "text/html") is True

    def test_check_content_type_missing(self):
        """Test missing Content-Type header returns False."""
        headers = {}
        assert checks.check_content_type(headers, "application/json") is False

    def test_check_content_type_no_match(self):
        """Test Content-Type doesn't match expected value."""
        headers = {"Content-Type": "text/html"}
        assert checks.check_content_type(headers, "application/json") is False


class TestChecksHtmlContains:
    """Tests for check_html_contains function."""

    def test_check_html_contains_simple_string(self):
        """Test HTML contains simple string selector."""
        content = b'<html><body><div class="test">Hello</div></body></html>'
        selectors = ["div.test"]
        result = checks.check_html_contains(content, selectors)
        assert result["div.test"] is True

    def test_check_html_contains_not_found(self):
        """Test HTML doesn't contain selector."""
        content = b"<html><body><p>Test</p></body></html>"
        selectors = ["missing"]
        result = checks.check_html_contains(content, selectors)
        assert result["missing"] is False

    def test_check_html_contains_empty(self):
        """Test empty selectors returns empty dict."""
        content = b"<html></html>"
        result = checks.check_html_contains(content, [])
        assert result == {}

    def test_check_html_contains_partial_match(self):
        """Test HTML contains partial match."""
        content = b"<html><body><title>My Page</title></body></html>"
        selectors = ["title"]
        result = checks.check_html_contains(content, selectors)
        assert result["title"] is True


class TestChecksCsvSchema:
    """Tests for check_csv_schema function."""

    def test_check_csv_schema_valid_columns(self):
        """Test CSV schema with valid columns."""
        content = b"name,age,city\nJohn,30,NY\nJane,25,LA"
        expected = ["name", "age", "city"]
        result = checks.check_csv_schema(content, expected)
        assert result["valid"] is True
        assert result["missing_columns"] == []
        assert result["row_count"] == 2

    def test_check_csv_schema_missing_column(self):
        """Test CSV schema with missing column."""
        content = b"name,age\nJohn,30"
        expected = ["name", "age", "city"]
        result = checks.check_csv_schema(content, expected)
        assert result["valid"] is False
        assert "city" in result["missing_columns"]

    def test_check_csv_schema_min_rows_sufficient(self):
        """Test CSV meets minimum rows requirement."""
        content = b"name,age\nJohn,30\nJane,25\nBob,40"
        result = checks.check_csv_schema(content, [], min_rows=2)
        assert result["valid"] is True
        assert result["row_count_valid"] is True

    def test_check_csv_schema_min_rows_insufficient(self):
        """Test CSV doesn't meet minimum rows requirement."""
        content = b"name,age\nJohn,30"
        result = checks.check_csv_schema(content, [], min_rows=5)
        assert result["valid"] is False
        assert result["row_count_valid"] is False

    def test_check_csv_schema_no_expectations(self):
        """Test CSV check with no expectations passes."""
        content = b"name,age\nJohn,30"
        result = checks.check_csv_schema(content, [])
        assert result["valid"] is True


class TestChecksumSha256:
    """Tests for checksum_sha256 function."""

    def test_checksum_sha256_deterministic(self):
        """Test checksum is deterministic."""
        content = b"test content"
        checksum1 = checks.checksum_sha256(content)
        checksum2 = checks.checksum_sha256(content)
        assert checksum1 == checksum2

    def test_checksum_sha256_different_content(self):
        """Test different content produces different checksum."""
        content1 = b"test content 1"
        content2 = b"test content 2"
        checksum1 = checks.checksum_sha256(content1)
        checksum2 = checks.checksum_sha256(content2)
        assert checksum1 != checksum2

    def test_checksum_sha256_format(self):
        """Test checksum is 64 character hex string."""
        content = b"test"
        checksum = checks.checksum_sha256(content)
        assert len(checksum) == 64
        assert all(c in "0123456789abcdef" for c in checksum)

