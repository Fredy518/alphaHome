# Derivative Data Task Implementation Plan

## 1. Goal

Implement derivative data calculations as `Task` subclasses within the existing `data_module` framework. This approach aims to:
- Leverage the existing `TaskFactory` for managing database connections (`DBManager`).
- Utilize a consistent task structure (`fetch_data`, `process_data`, `save_data`).
- Address the higher computational needs of derivative calculations by offloading CPU-intensive work to a process pool, utilizing multi-core CPUs effectively without blocking the main `asyncio` event loop.

## 2. Design Decisions

- **Task Subclasses**: Each distinct derivative calculation will be implemented as a class inheriting from `data_module.base_task.Task` (or a potential `BaseDerivativeTask`).
- **Database Access**: Tasks will use the `DBManager` instance injected by `TaskFactory` (`self.db`) within the `fetch_data` method to query necessary input data from local database tables. No separate "local data source" classes are needed.
- **Input Specification**: A new class attribute, `input_spec`, will be defined in each derivative task subclass to explicitly declare its input data dependencies.
  ```python
  # Example input_spec structure
  input_spec = {
      'input_key_1': { # Logical name for the input data block
          'table': 'source_table_name_1',   # Source database table
          'columns': ['col1', 'col2', 'date_col'], # Columns to fetch
          'date_col': 'date_col'           # Date column for filtering/joining
          # Optional: 'filter_conditions': {...} # Additional static filters
      },
      'input_key_2': {
          'table': 'source_table_name_2',
          'columns': ['colA', 'colB'],
          'date_col': 'another_date_col'
      }
      # ... more inputs if needed
  }
  ```
- **Calculation Logic**: The core calculation logic will reside within the `process_data` method of the task subclass.
- **Parallel Execution**: To handle CPU-bound calculations without blocking the `asyncio` event loop, `process_data` will:
    1. Define the actual calculation logic in a separate *synchronous* helper method (e.g., `_calculate_derivative`).
    2. Obtain a shared `ProcessPoolExecutor` managed by `TaskFactory`.
    3. Use `asyncio.get_running_loop().run_in_executor()` to submit the synchronous calculation method to the process pool.
    4. `await` the result from the executor.
- **Process Pool Management**: `TaskFactory` will be responsible for creating, providing access to, and shutting down a shared `concurrent.futures.ProcessPoolExecutor` instance.

## 3. Implementation Steps

**Step 1: Modify `TaskFactory` for Executor Management**
   - **File**: `data_module/task_factory.py`
   - **Actions**:
     - Import `concurrent.futures.ProcessPoolExecutor` and `os`.
     - Add class variable: `_process_executor = None`.
     - In `initialize()`: Create executor: `cls._process_executor = ProcessPoolExecutor(max_workers=os.cpu_count())`.
     - In `shutdown()`: Add `if cls._process_executor: cls._process_executor.shutdown(wait=True)`.
     - Add class method `get_process_executor(cls)` to return `cls._process_executor` after checking initialization status.

**Step 2: Create Derivatives Directory and Base Class (Optional)**
   - **Action**: Create directory `data_module/tasks/derivatives/`.
   - **File (Optional)**: `data_module/tasks/derivatives/base_derivative_task.py`
   - **Actions**:
     - Create `BaseDerivativeTask(Task)` inheriting from `data_module.base_task.Task`.
     - Define template `input_spec = {}`.
     - Consider adding helper methods if common patterns emerge.

**Step 3: Implement an Example Derivative Task**
   - **File**: `data_module/tasks/derivatives/moving_average_task.py`
   - **Actions**:
     - Create `MovingAverageTask(BaseDerivativeTask or Task)`.
     - Define `name`, `table_name` (e.g., `'derivative_moving_average'`), `primary_keys`, `date_column`.
     - Define `input_spec` (e.g., requiring `stock_daily`).
     - Implement `async def fetch_data(self, **kwargs)`:
       - Parse `input_spec`.
       - Use `self.db.fetch` (or similar `DBManager` methods) to query data based on `input_spec` and `kwargs`.
       - Return `{'stock_daily': df_result}`.
     - Implement synchronous `_calculate_ma(self, data_dict, window)`:
       - Extract `df_daily` from `data_dict`.
       - Perform Pandas rolling mean calculation.
       - Return result DataFrame.
     - Implement `async def process_data(self, data_dict, window=20)`:
       - Get executor: `executor = TaskFactory.get_process_executor()`.
       - Get loop: `loop = asyncio.get_running_loop()`.
       - Run in executor: `result = await loop.run_in_executor(executor, self._calculate_ma, data_dict, window)`.
       - Return `result`.

**Step 4: Update Task Registration**
   - **File**: `data_module/tasks/__init__.py`
   - **Actions**:
     - Import the new derivative task class(es) (e.g., `from .derivatives.moving_average_task import MovingAverageTask`).
     - Ensure the task is added to `__all__` or discoverable by the registration mechanism used (e.g., decorator).

**Step 5: Documentation Review**
   - **Action**: Review this plan and update relevant sections of the main project documentation (e.g., `docs/user_guide.md`) to reflect the new derivative task pattern.

## 4. Implementation Checklist

- [ ] Modify `data_module/task_factory.py` to add `ProcessPoolExecutor` management.
- [ ] Create directory `data_module/tasks/derivatives/`.
- [ ] (Optional) Create `data_module/tasks/derivatives/base_derivative_task.py`.
- [ ] Create example derivative task file `data_module/tasks/derivatives/moving_average_task.py` implementing `fetch_data`, `_calculate_ma`, and `process_data` using `run_in_executor`.
- [ ] Update `data_module/tasks/__init__.py` to include the new derivative task(s).
- [ ] Review and potentially update project documentation. 