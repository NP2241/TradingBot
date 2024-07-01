import statistics
import numpy as np

def calculate_bollinger_bands(prices, window_size=20, num_std_dev=2):
    if len(prices) < window_size:
        return None, None, None

    moving_average = statistics.mean(prices[-window_size:])
    std_dev = statistics.stdev(prices[-window_size:])
    upper_band = moving_average + (num_std_dev * std_dev)
    lower_band = moving_average - (num_std_dev * std_dev)

    return lower_band, moving_average, upper_band

def calculate_atr(prices, window_size=14):
    tr = [max(prices[i] - prices[i-1], abs(prices[i] - prices[i-1]), abs(prices[i-1] - prices[i])) for i in range(1, len(prices))]
    return np.mean(tr[-window_size:])

def calculate_cv(prices):
    mean_price = np.mean(prices)
    std_dev = np.std(prices)
    return (std_dev / mean_price) * 100

def calculate_rsi(prices, window_size=14):
    deltas = np.diff(prices)
    seed = deltas[:window_size + 1]
    up = seed[seed >= 0].sum() / window_size
    down = -seed[seed < 0].sum() / window_size
    rs = up / down
    rsi = np.zeros_like(prices)
    rsi[:window_size] = 100. - 100. / (1. + rs)

    for i in range(window_size, len(prices)):
        delta = deltas[i - 1]  # because the diff is 1 shorter

        if delta > 0:
            upval = delta
            downval = 0.
        else:
            upval = 0.
            downval = -delta

        up = (up * (window_size - 1) + upval) / window_size
        down = (down * (window_size - 1) + downval) / window_size

        rs = up / down
        rsi[i] = 100. - 100. / (1. + rs)

    return rsi[-1]

def calculate_moving_averages(prices, window_size=20):
    if len(prices) < window_size:
        return None
    return np.mean(prices[-window_size:])

def calculate_volatility_index(prices, volumes):
    if not prices or not volumes:
        return None

    lower_band, moving_average, upper_band = calculate_bollinger_bands(prices)
    atr = calculate_atr(prices)
    cv = calculate_cv(prices)

    if lower_band is None or moving_average is None or upper_band is None:
        return None

    # Normalize the metrics
    std_dev = statistics.stdev(prices)
    bollinger_band_width = upper_band - lower_band
    average_volume = statistics.mean(volumes)
    median_price = statistics.median(prices)

    normalized_std_dev = std_dev / median_price
    normalized_atr = atr / median_price
    normalized_bollinger_band_width = bollinger_band_width / median_price
    normalized_volume = average_volume / 1_000_000  # Normalize volume by dividing by a large number

    # Assign weights to each metric
    std_dev_weight = 0.3
    atr_weight = 0.2
    bollinger_band_width_weight = 0.2
    volume_weight = 0.3

    # Calculate volatility index
    volatility_index = (std_dev_weight * normalized_std_dev +
                        atr_weight * normalized_atr +
                        bollinger_band_width_weight * normalized_bollinger_band_width +
                        volume_weight * normalized_volume) * 100

    return volatility_index

def calculate_stock_metrics(prices, volumes):
    lower_band, moving_average, upper_band = calculate_bollinger_bands(prices)
    atr = calculate_atr(prices)
    cv = calculate_cv(prices)
    rsi = calculate_rsi(prices)
    moving_average_value = calculate_moving_averages(prices)

    return {
        'lower_band': lower_band,
        'moving_average': moving_average,
        'upper_band': upper_band,
        'atr': atr,
        'cv': cv,
        'rsi': rsi,
        'moving_average_value': moving_average_value
    }