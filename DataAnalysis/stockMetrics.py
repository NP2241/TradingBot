import statistics
import numpy as np

def calculate_bollinger_bands(prices, window_size=20, num_std_dev=2):
    if len(prices) < window_size:
        return None, None, None

    moving_average = statistics.median(prices[-window_size:])
    std_dev = statistics.stdev(prices[-window_size:])
    upper_band = moving_average + (num_std_dev * std_dev)
    lower_band = moving_average - (num_std_dev * std_dev)

    return lower_band, moving_average, upper_band

def calculate_stock_metrics(prices):
    lower_band, moving_average, upper_band = calculate_bollinger_bands(prices)

    return {
        'lower_band': lower_band,
        'moving_average': moving_average,
        'upper_band': upper_band
    }

def calculate_volatility_index(prices):
    if not prices:
        return None

    metrics = calculate_stock_metrics(prices)
    if not metrics['lower_band'] or not metrics['moving_average'] or not metrics['upper_band']:
        return None

    median_price = statistics.median(prices)
    std_dev = statistics.stdev(prices)
    atr = np.median([max(prices[i] - prices[i-1], abs(prices[i] - prices[i-1]), abs(prices[i-1] - prices[i])) for i in range(1, len(prices))])

    # Assign weights to each metric
    std_dev_weight = 0.4
    atr_weight = 0.3
    bollinger_band_width_weight = 0.3

    bollinger_band_width = metrics['upper_band'] - metrics['lower_band']

    # Calculate volatility index
    volatility_index = (std_dev_weight * std_dev +
                        atr_weight * atr +
                        bollinger_band_width_weight * bollinger_band_width) / (median_price + 1) * 100

    return volatility_index
