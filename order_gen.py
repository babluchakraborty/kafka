
# importing the required modules
import json
from kafka import KafkaProducer
from faker import Faker
import random
from faker.providers import BaseProvider
from datetime import datetime
import time


def produce_order(orderid=1):
    """
    Function to create orders of Buy/Sell using
    dynamically generated mock data
    :param orderid: ID for orders unique for this session
    :return: message: output order with details
    """
    instrument = fake.instr_name()
    trxn_type = fake.trxn_type()
    quantity = round(random.uniform(2, 5), 0)
    price = round(random.uniform(10, 20), 0)
    order_ts = datetime.now().strftime("%Y/%m/%d, %H:%M:%S")

    # message creation
    # this will be the structure of the stock order
    message = {
        'id': orderid + 1000000,
        'instrument': instrument,
        'trxn_type': trxn_type,
        'quantity': quantity,
        'price': price,
        'order_timestamp': order_ts,
    }
    return message


# Create mock data to generate stock specific data
class InstrumentProvider(BaseProvider):
    def instr_name(self):
        """ Function to initialise reference
        for mock data for instrument names"""
        instr_names = ["RIL", "INFY", "ONGC", "TCS"]
        return instr_names[random.randint(0, len(instr_names) - 1)]

    def trxn_type(self):
        """ Function to initialise reference
        for mock data for transaction types"""
        trxn_type = ["BUY", "SELL"]
        return trxn_type[random.randint(0, len(trxn_type) - 1)]


if __name__ == '__main__':

    # Kafka producer configuration
    producer = KafkaProducer(
        bootstrap_servers=['0.0.0.0:9092'],
        value_serializer=lambda m: json.dumps(m).encode('ascii')
    )

    # Initialise data mocker
    fake = Faker()
    fake.add_provider(InstrumentProvider)

    # Produce buy/sell orders
    # Orders are published to the "stock_exchange_orders" Kafka topic
    i = 1
    num_orders = 3000
    while i < num_orders:
        message = produce_order(i)
        print("Sending: {}".format(message))
        # sending the message to Kafka
        producer.send("stock_exchange_orders",
                      value=message)
        time.sleep(0.5)

        # Force sending of all messages
        if (i % 100) == 0:
            producer.flush()
        i += 1
    producer.flush()
