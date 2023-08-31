import time
from json import dumps
from kafka import KafkaProducer
from pyspark.sql import SparkSession

# Define the Kafka topic and bootstrap servers
kafka_topic = "RTEAD"
kafka_bootstrap_servers = "localhost:9092"


def main():
    print("Kafka Producer Application Started ...")

    # Create a Kafka producer object
    kafka_producer_obj = KafkaProducer(bootstrap_servers="localhost:9092",
                                       value_serializer=lambda x: dumps(x).encode('utf-8'))

    # Create a Spark session
    spark = SparkSession.builder.appName("KafkaProducer").getOrCreate()

    # importing csv files
    orders = spark.read.csv(r"D:\ModifiedProject\Data\olist_orders.csv", header=True)
    customers = spark.read.csv(r"D:\ModifiedProject\Data\olist_customers_dataset.csv", header=True)
    order_items = spark.read.csv(r"D:\ModifiedProject\Data\olist_order_items_dataset.csv", header=True)
    sellers = spark.read.csv(r"D:\ModifiedProject\Data\olist_sellers_dataset.csv", header=True)
    products = spark.read.csv(r"D:\ModifiedProject\Data\product_translated.csv", header=True)
    order_payments = spark.read.csv(r"D:\ModifiedProject\Data\olist_order_payments_dataset.csv", header=True)

    # Select the desired columns
    orders = orders.select('order_id', 'customer_id', 'order_purchase_timestamp', 'order_status', 'order_delivered_customer_date' , 'order_estimated_delivery_date')
    customers = customers.drop('customer_unique_id')
    order_items = order_items.drop('shipping_limit_date')
    products = products.select('product_id', 'product_category_name_english')
    order_payments = order_payments.select('order_id', 'payment_type')

    # Merge the DataFrames
    Streaming_DataSet = orders.join(customers, on='customer_id', how='outer')
    Streaming_DataSet = Streaming_DataSet.join(order_items, on='order_id', how='outer')
    Streaming_DataSet = Streaming_DataSet.join(products, on='product_id', how='outer')
    Streaming_DataSet = Streaming_DataSet.join(order_payments, on='order_id', how='outer')
    Streaming_DataSet = Streaming_DataSet.join(sellers, on='seller_id', how='outer')
    Streaming_DataSet = Streaming_DataSet.dropna(subset=['product_category_name_english'])
    Streaming_DataSet = Streaming_DataSet.dropna(subset=['payment_type'])

    # Convert the DataFrame to a list of JSON objects, where each JSON object represents a row in the DataFrame.
    orders_list = Streaming_DataSet.toJSON().collect()

    # Iterating through the list of JSON objects and sends each object to Kafka.
    for order in orders_list:
        message = order
        print(message)
        kafka_producer_obj.send(kafka_topic, message)
        time.sleep(1)


if __name__ == "__main__":
    main()
