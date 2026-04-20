# SEC EDGAR Service Tests

Comprehensive unit test suite for the SEC EDGAR ingestion Cloud Function.

## Test Structure

### Test Classes

1. **TestGetTickerCikMapping** (5 tests)
   - Tests ticker-to-CIK mapping retrieval from SEC API
   - Validates CIK zero-padding to 10 digits
   - Tests partial matches and empty results
   - Validates error handling for API failures

2. **TestGetLatest13fUrl** (4 tests)
   - Tests retrieval of latest 13F filing URLs
   - Handles cases with no 13F filings
   - Tests empty filing data
   - Validates API error handling

3. **TestParseSecXml** (3 tests)
   - Tests SEC XML parsing with namespace cleanup
   - Validates namespace removal
   - Tests handling of empty XML documents

4. **TestRunEdgarIngestionImpl** (3 tests)
   - Tests end-to-end ingestion logic
   - Tests handling of missing holdings
   - Tests resilience with partial errors

5. **TestRunEdgarIngestion** (2 tests)
   - Tests Cloud Functions event handler
   - Tests error handling in event handler

6. **TestTargetTickersConfiguration** (2 tests)
   - Validates TARGET_TICKERS configuration
   - Validates ticker symbol format

7. **TestWhaleCiksConfiguration** (2 tests)
   - Validates WHALE_CIKS configuration
   - Validates CIK mapping format

8. **TestEdgarHeaders** (3 tests)
   - Validates SEC headers configuration
   - Ensures User-Agent is set
   - Ensures Accept-Encoding is set

9. **TestProjectConfiguration** (1 test)
   - Validates PROJECT_ID configuration

10. **TestBigQueryIntegration** (2 tests)
    - Tests BigQuery submissions loading
    - Tests BigQuery holdings loading

## Running Tests

### Run all tests:
```bash
cd src/edgar
export API_KEY="test_key"
python -m pytest tests/test_main.py -v
```

### Run with coverage report:
```bash
cd src/edgar
export API_KEY="test_key"
python -m pytest tests/ -v --cov=. --cov-report=html
```

### Run specific test class:
```bash
python -m pytest tests/test_main.py::TestGetTickerCikMapping -v
```

### Run specific test:
```bash
python -m pytest tests/test_main.py::TestGetTickerCikMapping::test_get_ticker_cik_mapping_success -v
```

## Mocking Strategy

The test suite mocks all external dependencies:

- **requests.get**: Mocked for all SEC API calls
  - Ticker-to-CIK mapping API
  - SEC submissions API
  - 13F filing downloads

- **pandas_gbq.to_gbq**: Mocked for BigQuery operations
  - Submissions table loads
  - Holdings table loads

- **pd.DataFrame.to_gbq**: Mocked to prevent actual BigQuery authentication

- **CloudEvent**: Used to create test event payloads

## Coverage Requirements

- **Minimum coverage**: 70%
- **Coverage report**: Generated in `htmlcov/index.html`

## Test Data

Tests use realistic but minimal data:
- Real CUSIP codes (037833100 = Apple)
- Real SEC CIK format (10-digit zero-padded)
- Real accession number format
- Real filing dates from recent submissions

## Dependencies

See `requirements-test.txt`:
- pytest
- pytest-cov
- pytest-mock
- mock

## Notes

- All tests are isolated and can run in any order
- No external API calls are made during testing
- No BigQuery authentication is required
- Tests run in < 1 second total
