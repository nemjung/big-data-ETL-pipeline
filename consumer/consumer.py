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

def mappingKafkaCassandra(raw):
    return (
        uuid.UUID(raw["id"]),
        raw.get("_c0"),
        raw.get("_c1"),
        raw.get("_c2"),
        raw.get("_c3"),   
        raw.get("_c4"),
        raw.get("_c5"),
        raw.get("_c6"),
        raw.get("_c7"),
        raw.get("_c8"),
        raw.get("_c9"),
        raw.get("_c10"),
        raw.get("_c11"),
        raw.get("_c12"),
        raw.get("_c13"),
        raw.get("_c14"),
        raw.get("_c15"),
        raw.get("_c16"),
        raw.get("_c17"),
        raw.get("_c18"),
        raw.get("_c19"),
        raw.get("_c20"),
        raw.get("_c21"),
        raw.get("_c22"),
        raw.get("_c23"),
    )

BATCH_SIZE = 5000
batchData = []
message_limit = 175490
message_received = 1
cout = 1

for message in consumer:
    if message_received >= message_limit:
        break

    data = message.value
    batchData.append(mappingKafkaCassandra(data))

    if len(batchData) >= BATCH_SIZE:
        print(data)
        execute_concurrent_with_args(session, insert_query, batchData)
        print(f"Inserted {len(batchData)} rows into Cassandra (batch {cout})")
        batchData = []
        cout += 1

    message_received += 1

if batchData:
    execute_concurrent_with_args(session, insert_query, batchData)
    print(f"Inserted {len(batchData)} remaining rows")

consumer.commit()
consumer.close()
print("All data inserted successfully")