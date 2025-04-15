# Stock Daily Basic Incremental Update Example

This script demonstrates how to perform incremental updates for the `stock_daily_basic` task using various modes.

## Usage

Run the script from the project root directory with one of the following modes:

**1. Automatic Mode (Default):**
   Updates data starting from the next trading day after the latest date found in the database.
   ```bash
   python examples/stock_daily_basic_incremental_update.py --auto
   ```
   (If no arguments are provided, this mode is used by default).

**2. Update Last N Trading Days:**
   Updates data for the specified number of most recent trading days.
   ```bash
   python examples/stock_daily_basic_incremental_update.py --days 5
   ```

**3. Update Specific Date Range:**
   Updates data for a specific date range.
   ```bash
   python examples/stock_daily_basic_incremental_update.py --start-date 20230101 --end-date 20230131
   ```

**4. Full Update Mode (Use with caution):**
   Forces an update starting from the earliest date (e.g., 19910101).
   ```bash
   python examples/stock_daily_basic_incremental_update.py --full-update
   ```

**Optional Arguments:**

*   `--end-date YYYYMMDD`: Specify the end date for the update (defaults to today).
*   `--show-progress` / `--no-show-progress`: Control whether the progress bar is displayed (defaults to True). 