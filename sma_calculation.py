# importing the required modules
from json import loads
from kafka import KafkaConsumer
import pandas as pd
import matplotlib.pyplot as plt


if __name__ == '__main__':

    # Kafka Consumer
    trade_consumer = KafkaConsumer(
        'successful_trade',
        bootstrap_servers=['localhost : 9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    # Initial dataframe for sma calc
    plt.ion()
    df = pd.DataFrame()

    for message in trade_consumer:
        df_iter = pd.json_normalize(message.value)
        df_iter = df_iter.drop(["id", "id_sell"], axis=1)
        df_iter['update_timestamp'] = pd.to_datetime(df_iter['update_timestamp'])
        df_iter["price_qty_product"] = df_iter['price'] * df_iter['quantity']
        df = df.append(
            df_iter,
            ignore_index=True
        )

        # initialise window and intervals
        window_size = 2
        interval_size = '5T'

        resampled_data = df.groupby('instrument').resample(
            interval_size, on='update_timestamp'
        ).sum()

        # calculate closing price for each time interval
        resampled_data["closing_price"] = (
            resampled_data['price_qty_product'] / resampled_data['quantity']
        )

        # calculate sma as per the window
        resampled_data["sma"] = resampled_data.groupby(
            ["instrument"]
        )["closing_price"].transform(
            lambda x: x.rolling(window=window_size, min_periods=1).mean()
        )

        # write to persistent storage
        output_path = "outputs/sma_calc.csv"
        resampled_data.drop(["price_qty_product"], axis=1).to_csv(
            output_path,
            header=True,
        )

        # ############################
        # # Visualisation
        # ############################
        # resampled_data = resampled_data.drop(
        #     ["quantity", "price", "price_qty_product"], axis=1
        # )
        # # loop through each instrument and plot the data
        # for instrument in resampled_data.index.levels[0]:
        #     # create a subplot for each instrument
        #     fig, ax = plt.subplots()
        #     # filter the data for the current instrument
        #     instrument_data = resampled_data.loc[instrument]
        #
        #     # plot the price data
        #     ax.plot(instrument_data.index, instrument_data['closing_price'], label='Price')
        #     # plot the moving average data
        #     ax.plot(instrument_data.index, instrument_data["sma"], label='Moving Average')
        #
        #     ax.set_title(instrument)
        #     ax.legend()
        #
        #     # update the plot
        #     plt.show()
