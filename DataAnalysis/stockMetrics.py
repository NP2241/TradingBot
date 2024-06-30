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

def calculate_atr(prices, period=14):
    if len(prices) < period + 1:
        return None

    true_ranges = []
    for i in range(1, len(prices)):
        high_low = prices[i] - prices[i-1]
        high_close = abs(prices[i] - prices[i-1])
        low_close = abs(prices[i-1] - prices[i])
        true_range = max(high_low, high_close, low_close)
        true_ranges.append(true_range)

    atr = np.mean(true_ranges[-period:])
    return atr

def calculate_coefficient_of_variation(prices):
    if not prices:
        return None

    mean_price = np.mean(prices)
    std_dev = np.std(prices)

    if mean_price == 0:
        return None

    coefficient_of_variation = std_dev / mean_price
    return coefficient_of_variation

def calculate_stock_metrics(prices):
    lower_band, moving_average, upper_band = calculate_bollinger_bands(prices)

    return {
        'lower_band': lower_band,
        'moving_average': moving_average,
        'upper_band': upper_band
    }

def calculate_volatility_index(prices, volumes):
    if not prices or not volumes:
        return None

    metrics = calculate_stock_metrics(prices)
    if not metrics['lower_band'] or not metrics['moving_average'] or not metrics['upper_band']:
        return None

    # Calculate the standard deviation of the prices
    std_dev = statistics.stdev(prices)

    # Calculate the Average True Range (ATR)
    atr = calculate_atr(prices)

    # Calculate the Bollinger Band Width
    bollinger_band_width = metrics['upper_band'] - metrics['lower_band']

    # Calculate the Coefficient of Variation (CV)
    coefficient_of_variation = calculate_coefficient_of_variation(prices)

    # Calculate the Volume Weight
    volume_median = statistics.median(volumes)
    volume_std_dev = statistics.stdev(volumes)
    volume_weight = volume_std_dev / (volume_median + 1)

    # Assign weights to each metric
    std_dev_weight = 0.35
    atr_weight = 0.25
    bollinger_band_width_weight = 0.2
    cv_weight = 0.1
    volume_weight_weight = 0.1

    # Calculate volatility index
    volatility_index = (std_dev_weight * std_dev +
                        atr_weight * atr +
                        bollinger_band_width_weight * bollinger_band_width +
                        cv_weight * coefficient_of_variation +
                        volume_weight_weight * volume_weight) / (statistics.median(prices) + 1) * 100

    return volatility_index
