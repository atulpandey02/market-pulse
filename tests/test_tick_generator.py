import pytest
from ingestion.producer import TickGenerator, SYMBOLS


class TestTickGenerator:
    """Unit tests for TickGenerator.
    No Kafka connection needed — pure business logic testing.
    """

    def setup_method(self):
        """Runs before each test method."""
        self.generator = TickGenerator(symbols=SYMBOLS)

    def test_generate_returns_dict(self):
        """Tick should be a dictionary."""
        tick = self.generator.generate("AAPL")
        assert isinstance(tick, dict)

    def test_generate_has_required_fields(self):
        """Tick must have all required fields."""
        tick = self.generator.generate("AAPL")
        required_fields = ["symbol", "price", "volume", "timestamp", "event_id"]
        for field in required_fields:
            assert field in tick, f"Missing field: {field}"

    def test_generate_correct_symbol(self):
        """Symbol in tick must match what was requested."""
        tick = self.generator.generate("AAPL")
        assert tick["symbol"] == "AAPL"

    def test_generate_price_in_valid_range(self):
        """Price must be between 100 and 500."""
        tick = self.generator.generate("AAPL")
        assert 100 <= tick["price"] <= 500

    def test_generate_volume_in_valid_range(self):
        """Volume must be between 100 and 10000."""
        tick = self.generator.generate("AAPL")
        assert 100 <= tick["volume"] <= 10000

    def test_event_id_is_unique(self):
        """Every tick must have a unique event_id."""
        tick1 = self.generator.generate("AAPL")
        tick2 = self.generator.generate("AAPL")
        assert tick1["event_id"] != tick2["event_id"]

    def test_event_id_contains_symbol(self):
        """event_id must be prefixed with symbol for traceability."""
        tick = self.generator.generate("AAPL")
        assert tick["event_id"].startswith("AAPL")

    def test_random_symbol_returns_valid_symbol(self):
        """random_symbol must return a symbol from the list."""
        symbol= self.generator.random_symbol()
        assert symbol in SYMBOLS

    def test_custom_symbols_list(self):
        """Generator should work with any symbols list."""
        custom_generator = TickGenerator(symbols=["BTC", "ETH"])
        symbol = custom_generator.random_symbol()
        assert symbol in ["BTC", "ETH"]
