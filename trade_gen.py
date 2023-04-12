
# importing the required modules
import json
from json import loads
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
from datetime import datetime
import os

if __name__ == '__main__':

    # Kafka consumer configuration
    order_consumer = KafkaConsumer(
        'stock_exchange_orders',
        bootstrap_servers=['localhost : 9092'],
        value_deserializer=lambda x: loads(x.decode('utf-8'))
    )

    # Kafka producer configuration
    trade_producer = KafkaProducer(
        bootstrap_servers=['0.0.0.0:9092'],
        value_serializer=lambda m: json.dumps(m).encode('ascii')
    )

    # Initialise major variables
    df_buy = pd.DataFrame()
    df_sell = pd.DataFrame()
    df_match = pd.DataFrame()
    df_success_trades = pd.DataFrame()

    for message in order_consumer:
        df_iter = pd.json_normalize(message.value)
        trnx_type_tmp = df_iter.iloc[0]["trxn_type"]

        ############################
        # Match Maker Algorithm
        ############################

        # Split into buy/sell orders
        if trnx_type_tmp == "BUY":
            df_buy = df_buy.append(df_iter)
        else:
            df_sell = df_sell.append(df_iter)

        # Using "instrument", "quantity", "price" as keys
        # as criteria for match
        if (df_buy.shape[0] > 1) & (df_sell.shape[0] > 1):
            df_match = df_buy.merge(
                df_sell.drop("order_timestamp", axis=1).rename(columns={'id': 'id_sell'}),
                on=["instrument", "quantity", "price"],
                how="inner"
            )

        if df_match.shape[0] > 1:
            print("Order matched! Trade generated.")
            # eliminate multiple matches
            df_match = df_match.drop_duplicates(["instrument", "quantity", "price"])
            df_match_merge_buy = df_buy.merge(
                df_match[["id"]], how='outer', indicator=True
            )

            # Re-calibrate the BUY and SELL orders
            # according to the remaining orders
            df_buy = df_match_merge_buy[
                (df_match_merge_buy._merge == 'left_only')
            ].drop('_merge', axis=1)

            df_match_merge_sell = df_sell.merge(
                df_match[["id_sell"]].rename(columns={'id_sell': 'id'}),
                how='outer', indicator=True
            )
            df_sell = df_match_merge_sell[
                (df_match_merge_sell._merge == 'left_only')
            ].drop('_merge', axis=1)

            # Write successful orders into a serving zone
            df_match["update_timestamp"] = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")
            df_match = df_match[
                ["id", "id_sell", "instrument",
                 "quantity", "price", "update_timestamp"]
            ]
            output_path = "outputs/successful_orders.csv"
            df_match.to_csv(
                output_path,
                header=not os.path.exists(output_path),
                index=False, mode='a'
            )

            # Publish trades to "successful_trade" topic
            result = df_match.to_json(orient="records")
            parsed = json.loads(result)
            trade_producer.send("successful_trade",
                                value=parsed)

            # Refresh df_match
            df_match = pd.DataFrame()
