import pandas as pd
import numpy as np

def get_coin_metadata() -> dict:
    return {
        "targets": [
            {"symbol": "BONK", "timeframe": "1H"},
            {"symbol": "PEPE", "timeframe": "1H"},
            {"symbol": "LDO", "timeframe": "1H"},
        ],
        "anchors": [
            {"symbol": "BTC", "timeframe": "4H"},
            {"symbol": "ETH", "timeframe": "4H"},
            {"symbol": "SOL", "timeframe": "1D"},
            {"symbol": "DOGE", "timeframe": "12H"},
            {"symbol": "AVAX", "timeframe": "1D"},
        ]
    }


def generate_signals(anchor_df: pd.DataFrame, target_df: pd.DataFrame) -> pd.DataFrame:
    try:
        # Calculate anchor returns
        anchor_df['btc_return_4h'] = anchor_df['close_BTC_4H'].pct_change()
        anchor_df['eth_return_4h'] = anchor_df['close_ETH_4H'].pct_change()
        anchor_df['sol_return_1D'] = anchor_df['close_SOL_1D'].pct_change()
        anchor_df['doge_return_12H'] = anchor_df['close_DOGE_12H'].pct_change()
        anchor_df['avax_return_1D'] = anchor_df['close_AVAX_1D'].pct_change()

        all_results = []

        # Loop through each target coin
        for symbol in ['LDO', 'PEPE', 'BONK']:
            price_col = f'close_{symbol}_1H'

            df = pd.merge(
                target_df[['timestamp', price_col]],
                anchor_df[['timestamp',
                           'btc_return_4h', 'eth_return_4h',
                           'sol_return_1D', 'doge_return_12H', 'avax_return_1D']],
                on='timestamp',
                how='outer'
            ).sort_values('timestamp').reset_index(drop=True)


            signals = []
            position_sizes = []
            in_position = False
            entry_price = 0

            for i in range(len(df)):
                price = df[price_col].iloc[i]

                btc_pump = df['btc_return_4h'].iloc[i] > 0.05 if pd.notna(df['btc_return_4h'].iloc[i]) else False
                eth_pump = df['eth_return_4h'].iloc[i] > 0.05 if pd.notna(df['eth_return_4h'].iloc[i]) else False
                sol_pump = df['sol_return_1D'].iloc[i] > 0.01 if pd.notna(df['sol_return_1D'].iloc[i]) else False
                doge_pump = df['doge_return_12H'].iloc[i] > 0.01 if pd.notna(df['doge_return_12H'].iloc[i]) else False
                avax_pump = df['avax_return_1D'].iloc[i] > 0.01 if pd.notna(df['avax_return_1D'].iloc[i]) else False
                target_price = df[price_col].iloc[i]

                if not in_position:
                    if (btc_pump or eth_pump or sol_pump or doge_pump or avax_pump) and pd.notna(price):
                        signals.append('BUY')
                        position_sizes.append(0.5)
                        in_position = True
                        entry_price = price
                    else:
                        signals.append('HOLD')
                        position_sizes.append(0.0)
                else:
                    if pd.notna(price) and entry_price > 0:
                        profit_pct = (price - entry_price) / entry_price
                        if profit_pct >= 0.05 or profit_pct <= -0.03:
                            signals.append('SELL')
                            position_sizes.append(0.0)
                            in_position = False
                            entry_price = 0
                        else:
                            signals.append('HOLD')
                            position_sizes.append(0.5)
                    else:
                        signals.append('HOLD')
                        position_sizes.append(0.5 if in_position else 0.0)

            result_df = pd.DataFrame({
                'timestamp': df['timestamp'],
                'symbol': symbol,
                'signal': signals,
                'position_size': position_sizes
            })

            all_results.append(result_df)

        return pd.concat(all_results).reset_index(drop=True)

    except Exception as e:
        raise RuntimeError(f"Error in generate_signals: {e}")





# Test block
if __name__ == "__main__":
    timestamps = pd.date_range(start="2024-01-01", periods=10, freq="h")

    dummy_anchor_df = pd.DataFrame({
        "timestamp": timestamps,
        "close_BTC_4H": np.random.rand(10) * 100,
        "close_ETH_4H": np.random.rand(10) * 100,
        "close_SOL_1D": np.random.rand(10) * 100,
        "close_DOGE_12H": np.random.rand(10) * 100,
        "close_AVAX_1D": np.random.rand(10) * 100,
    })

    dummy_target_df = pd.DataFrame({
        "timestamp": timestamps,
        "close_LDO_1H": np.random.rand(10) * 10,
        "close_PEPE_1H": np.random.rand(10) * 5,
        "close_BONK_1H": np.random.rand(10) * 3,
    })

    signals = generate_signals(dummy_anchor_df, dummy_target_df)
    print(signals)
