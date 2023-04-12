# importing the required modules
from json import loads
from kafka import KafkaConsumer
import pandas as pd


if __name__ == '__main__':

    # Kafka Consumer
    trade_consumer = KafkaConsumer(
        'successful_trade',
        bootstrap_servers=['localhost : 9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    # initialise the reverse dataframe
    df = pd.DataFrame()

    for message in trade_consumer:
        df_iter = pd.json_normalize(message.value)
        df_iter = df_iter.drop(["id", "id_sell"], axis=1)
        df_iter['update_timestamp'] = pd.to_datetime(df_iter['update_timestamp'])
        df = df.append(
            df_iter,
            ignore_index=True
        )
        df["price_qty_product"] = df['price'] * df['quantity']

        # Initialise window size
        window_size = 2

        resampled_data = df.groupby('instrument').resample(
            '5T', on='update_timestamp'
        ).agg({
            'price': ['first'],
            'price_qty_product': ['sum'],
            'quantity': ['sum'],
        })

        # Calculate closing prices
        resampled_data["closing_price"] = (
            resampled_data['price_qty_product'] / resampled_data['quantity']
        )
        resampled_data["closing_price"] = resampled_data.groupby(["instrument"])[
            "closing_price"
        ].transform(
            lambda x: x.rolling(window=window_size, min_periods=1).mean()
        )

        resampled_data.columns = ["price", "price_qty_product", "quantity", "closing_price"]

        # Arrive at opening prices
        resampled_data["opening_price"] = resampled_data.groupby(["instrument"])[
            "price"
        ].transform(
            lambda x: x.rolling(window=window_size, min_periods=1).apply(lambda t: t[0])
        )

        # calculate profits
        resampled_data["profit"] = (
            resampled_data["closing_price"] - resampled_data["opening_price"]
        )

        output_path = "outputs/profit_calc.csv"
        resampled_data.to_csv(
            output_path,
            header=True,
        )
