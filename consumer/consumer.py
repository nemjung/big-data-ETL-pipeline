print("Starting Consumer...")
import json, uuid, time, os
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from dotenv import load_dotenv

envPath = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
load_dotenv(envPath)

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
CASSANDRA_HOST = "cassandra"
CASSANDRA_PORT = 9042

consumer = KafkaConsumer(
    "cars",
    bootstrap_servers=KAFKA_BROKER,
    group_id='1st-group',
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

def waitForCassandra():
    while True:
        try:
            cluster = Cluster([CASSANDRA_HOST], port=CASSANDRA_PORT)
            session = cluster.connect()
            session.execute("""
                CREATE KEYSPACE IF NOT EXISTS bigdata 
                WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
            """)
            
            session.set_keyspace('bigdata')

            with open("./cassandra-setup.cql", "r") as f:
                cql_commands = f.read()
            session.execute(cql_commands)
            
            print("Connected to Cassandra!")
            return session
        except Exception as e:
            print(f"Waiting for Cassandra... {e}")
            time.sleep(10)

session = waitForCassandra()


insert_query = session.prepare("""
INSERT INTO cars (
    id, new_used, name, money, exterior_color, interior_color, drivetrain, mpg, 
    fuel_type, transmission, engine, mileage, convenience, entertainment, exterior, 
    safety, seating, accidents_or_damage, clean_title, owner_vehicle, personal_use, 
    brand, year, model, currency
) VALUES (
    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
);
""")

BATCH_SIZE = 5000
batchData = []
message_limit = 175490 # for exit the loop 
message_received = 1

for message in consumer:
    if message_received >= message_limit: #when message_received == 175490 exit the loop
        break 
    data = message.value
    data["id"] = uuid.UUID(data["id"])

    batchData.append((
        data.get("id"),
        data.get("new&used"), data.get("name"), data.get("money"), data.get("Exterior color"),
        data.get("Interior color"), data.get("Drivetrain"), data.get("MPG"),
        data.get("Fuel type"), data.get("Transmission"), data.get("Engine"),
        data.get("Mileage"), data.get("Convenience"), data.get("Entertainment"),
        data.get("Exterior"), data.get("Safety"), data.get("Seating"),
        data.get("Accidents or damage"), data.get("Clean title"),
        data.get("1-owner vehicle"), data.get("Personal use only"),
        data.get("brand"), data.get("year"), data.get("Model"), data.get("currency")
    ))

    if len(batchData) >= BATCH_SIZE:
        print(data)
        execute_concurrent_with_args(session, insert_query, batchData)
        print(f"Inserted {len(batchData)} rows into Cassandra")
        batchData = []
    message_received += 1


if batchData:
    execute_concurrent_with_args(session, insert_query, batchData)
    print(f"Inserted {len(batchData)} remaining rows")

consumer.commit()
consumer.close()
print("All data inserted successfully")
