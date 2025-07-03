# Strategy Research Project: [YOUR STRATEGY NAME]

**ID:** `my_research_project` | **Version:** `0.1.0`

---

## 1. Overview

This project template provides a complete, out-of-the-box environment for backtesting trading strategies using the `backtrader` library. It is designed to quickly test a strategy idea against historical data and evaluate its performance.

The example strategy implemented is a simple Moving Average Crossover.

## 2. Project Structure

-   **/data**: Contains the historical market data (OHLCV format) required for the backtest.
-   **/notebooks**: Use `01_strategy_performance_analysis.ipynb` to perform in-depth analysis and visualization of the backtest results.
-   **/src**: For defining more complex strategies or custom `backtrader` components (e.g., analyzers, indicators) in separate Python files.
-   **config.yml**: **Crucial for this template.** Configure the backtest period, initial capital, commission, slippage, and strategy-specific parameters.
-   **main.py**: The main backtest runner. It loads the configuration, sets up the `backtrader` engine, runs the strategy, and prints a summary of performance metrics.

## 3. How to Run

### a. Implement Your Strategy
1.  Open `main.py`.
2.  Locate the `MovingAverageCrossStrategy` class (or whichever strategy class is defined).
3.  **Replace the example logic within this class with your own trading strategy.** You can modify the `__init__` method to define your indicators and the `next` method to define your entry/exit logic.

### b. Configure Your Backtest
1.  Open `config.yml`.
2.  Update the `project_id` and `description`.
3.  Under `backtest_settings`, define the `start_date`, `end_date`, `initial_cash`, and other broker settings.
4.  Under `parameters`, adjust the parameters for your strategy. These are passed directly to your strategy class.

### c. Execute the Backtest

Run the following command from this project's root directory:

```bash
python main.py --config config.yml
```

## 4. How to Interpret the Results

The script will print a summary of the backtest results to the console, including:

-   **Starting/Ending Portfolio Value**: The absolute change in your portfolio's value.
-   **Sharpe Ratio**: A measure of risk-adjusted return. Higher is generally better.
-   **Annualized Return**: The geometric average amount of money earned by an investment each year over a given time period.
-   **Max Drawdown**: The maximum observed loss from a peak to a trough of a portfolio, before a new peak is attained. This is a key measure of risk.

For more detailed analysis, use the provided notebook.

## 5. Next Steps

-   Implement a more sophisticated strategy in `main.py`.
-   Use the `notebooks/` to plot the equity curve and trade history.
-   Run parameter optimization sweeps by modifying `main.py` to loop through different strategy parameters. 