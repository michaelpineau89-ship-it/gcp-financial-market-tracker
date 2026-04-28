# FMP Service Tests

Unit and integration tests for the FMP (Financial Modeling Prep) ingestion service.

## Test Structure

- `test_main.py` - Main test suite covering:
  - `TestFetchFmpData` - Tests for API data fetching
  - `TestRunFmpIngestionImpl` - Tests for core ingestion logic
  - `TestRunFmpIngestion` - Tests for Cloud Functions entry point
  - `TestApiKeyValidation` - Tests for environment configuration
  - `TestTargetTickers` - Tests for ticker configuration

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
pytest tests/test_main.py::TestFetchFmpData -v
```

### Run specific test
```bash
pytest tests/test_main.py::TestFetchFmpData::test_fetch_fmp_data_success -v
```

## Test Coverage

The test suite covers:

1. **API Fetching (`fetch_fmp_data`)**
   - ✅ Successful API calls
   - ✅ Empty list responses
   - ✅ HTTP errors (401, 500)
   - ✅ Request timeouts
   - ✅ Optional arguments

2. **Ingestion Logic (`run_fmp_ingestion_impl`)**
   - ✅ Successful data ingestion
   - ✅ No data scenarios
   - ✅ BigQuery failures
   - ✅ Partial data ingestion

3. **Cloud Functions Handler (`run_fmp_ingestion`)**
   - ✅ Successful execution
   - ✅ Pub/Sub message parsing
   - ✅ Error handling
   - ✅ Empty events

4. **Configuration**
   - ✅ API_KEY validation
   - ✅ TARGET_TICKERS configuration

## Mocking Strategy

The tests use `unittest.mock` to mock external dependencies:

- `requests.get` - Mocked for all HTTP calls to FMP API
- `pandas_gbq.to_gbq` - Mocked for BigQuery operations
- Cloud events - Mocked for Pub/Sub testing

This ensures tests are fast and don't require actual API keys or BigQuery access.

## CI/CD Integration

Tests are run automatically in the GitHub Actions workflow before deployment. See `.github/workflows/deploy-fmp-cf.yml` for the configuration.
