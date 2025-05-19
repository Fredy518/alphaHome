# Placeholder for example_ma_task.py
# TODO: Add actual moving average calculation logic here

def calculate_ma(data, window):
    # This is a dummy implementation
    if len(data) < window:
        return []
    return [sum(data[i-window:i]) / window for i in range(window, len(data) + 1)] 