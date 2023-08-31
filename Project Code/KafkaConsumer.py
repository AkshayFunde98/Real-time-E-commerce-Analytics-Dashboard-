import json
from kafka import KafkaConsumer
from pymongo import MongoClient

# Define the Kafka topic and bootstrap servers that the consumer will connect to.
kafka_topic = "RTEAD"
kafka_bootstrap_servers = "localhost:9092"

# Set up MongoDB connection
client = MongoClient('mongodb://localhost:27017/')
db = client['project']
collection = db['RTEAD']


def main():
    print("Kafka Consumer Application Started ...")

    kafka_consumer_obj = KafkaConsumer(kafka_topic, bootstrap_servers=kafka_bootstrap_servers)

    # Create a JSON file to store the messages
    json_file = open(r"D:\ModifiedProject\Output\RETADModified2.json", "a")

    i = 0
    for message in kafka_consumer_obj:
        kafka_data = json.loads(message.value)

        # Append the message to the JSON file
        json_file.write(kafka_data + "\n")
        
        # Creating dictionary of from JSON String
        obj_dict = eval(kafka_data)
        
        # Set the _id field value to i+1
        obj_dict["_id"] = i + 1
        print(obj_dict)
        
        collection.update_one(
            {'_id': obj_dict['_id']},
            {'$set': obj_dict},
            upsert=True  # Insert if not exists, otherwise update
        )

        i += 1

    # Close the JSON file
    json_file.close()


if __name__ == "__main__":
    main()
