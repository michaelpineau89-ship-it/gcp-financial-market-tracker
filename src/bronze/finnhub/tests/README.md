# Finnhub Service Tests

Unit and integration tests for the Finnhub data ingestion service.

## Test Structure

- `test_main.py` - Main test suite covering:
  - `TestMakeApiCall` - Tests for API retry wrapper
  - `TestRunFinnhubIngestionImpl` - Tests for core ingestion logic
  - `TestRunFinnhubIngestion` - Tests for Cloud Functions entry point
  - `TestApiKeyValidation` - Tests for environment configuration
  - `TestTargetTickersConfiguration` - Tests for ticker configuration
  - `TestDataFrameHandling` - Tests for DataFrame operations

## Running Tests

### Setup
```bash
# Install test dependencies
pip install -r requirements-test.txt
```

### Run all tests
```bash
pytest
```

### Run with coverage report
```bash
pytest --cov=. --cov-report=html
```

### Run specific test class
```bash
pytest tests/test_main.py::TestMakeApiCall -v
```

### Run specific test
```bash
pytest tests/test_main.py::TestMakeApiCall::test_make_api_call_success -v
```

## Test Coverage

The test suite covers:

1. **API Wrapper (`make_api_call`)**
   - ✅ Successful API calls
   - ✅ Retry on rate limits (429)
   - ✅ Keyword arguments

2. **Ingestion Logic (`run_finnhub_ingestion_impl`)**
   - ✅ Successful data ingestion
   - ✅ No data scenarios
   - ✅ BigQuery failures
   - ✅ Partial data ingestion

3. **Cloud Functions Handler (`run_finnhub_ingestion`)**
   - ✅ Successful execution
   - ✅ Pub/Sub message parsing
   - ✅ Error handling

4. **Configuration**
   - ✅ API_KEY validation
   - ✅ TARGET_TICKERS configuration

5. **Data Handling**
   - ✅ Empty DataFrame handling
   - ✅ DataFrame concatenation
   - ✅ Timestamp columns

## Mocking Strategy

The tests use `unittest.mock` to mock external dependencies:

- `finnhub.Client` - Mocked for all Finnhub API calls
- `pandas_gbq.to_gbq` - Mocked for BigQuery operations
- Cloud events - Mocked for Pub/Sub testing

This ensures tests are fast and don't require actual API keys or BigQuery access.

## CI/CD Integration

Tests are run automatically in the GitHub Actions workflow before deployment. See `.github/workflows/deploy-finnhub-cf.yml` for the configuration.
