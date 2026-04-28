# Alpha Vantage Service Tests

Unit and integration tests for the Alpha Vantage quote ingestion service.

## Test Structure

- `test_main.py` - Main test suite covering:
  - `TestGetData` - Tests for quote data fetching
  - `TestRunAlphaVantageIngestionImpl` - Tests for core ingestion logic
  - `TestRunAlphaVantageIngestion` - Tests for Cloud Functions entry point
  - `TestDataCleaning` - Tests for Alpha Vantage key cleaning
  - `TestApiKeyValidation` - Tests for environment configuration
  - `TestTargetTickersConfiguration` - Tests for ticker configuration

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
pytest tests/test_main.py::TestGetData -v
```

### Run specific test
```bash
pytest tests/test_main.py::TestGetData::test_get_data_success -v
```

## Test Coverage

The test suite covers:

1. **API Data Fetching (`get_data`)**
   - ✅ Successful API calls with quote data
   - ✅ Empty quote responses
   - ✅ HTTP errors
   - ✅ Request exceptions and timeouts
   - ✅ Timeout configuration

2. **Ingestion Logic (`run_alphavantage_ingestion_impl`)**
   - ✅ Successful data ingestion
   - ✅ No data scenarios
   - ✅ BigQuery failures
   - ✅ Partial data ingestion

3. **Cloud Functions Handler (`run_alphavantage_ingestion`)**
   - ✅ Successful execution
   - ✅ Pub/Sub message parsing
   - ✅ Error handling

4. **Data Cleaning**
   - ✅ Alpha Vantage key transformation
   - ✅ Timestamp column addition
   - ✅ DataFrame handling

5. **Configuration**
   - ✅ API_KEY validation
   - ✅ TARGET_TICKERS configuration

## Mocking Strategy

The tests use `unittest.mock` to mock external dependencies:

- `requests.get` - Mocked for all API calls to Alpha Vantage
- `pd.DataFrame.to_gbq` - Mocked for BigQuery operations
- `time.sleep` - Mocked to avoid rate-limit delays in tests
- Cloud events - Mocked for Pub/Sub testing

This ensures tests are fast and don't require actual API keys or BigQuery access.

## CI/CD Integration

Tests are run automatically in the GitHub Actions workflow before deployment. See `.github/workflows/deploy-alphavantage-cf.yml` for the configuration.
