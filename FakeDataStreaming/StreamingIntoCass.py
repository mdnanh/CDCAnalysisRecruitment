from kafka import KafkaConsumer
from cassandra.cluster import Cluster
import json


KAFKA_HOST_IP="kafka"
TOPIC = 'Recruit'
KEYSPACE= "study_DE"
C_TABLE= "tracking"
CASS_HOST= "cassandra"


def streamingData():
    try:
        kafka_c = KafkaConsumer(TOPIC,
                         bootstrap_servers=[f'{KAFKA_HOST_IP}:9092'],
                         auto_offset_reset='earliest',
                         enable_auto_commit=True,
                         group_id='RecruitPlatform',
                         value_deserializer=lambda v: json.loads(v.decode('utf-8'))
                    )
        # Kết nối tới Cassandra
        cluster = Cluster([CASS_HOST])
        session = cluster.connect(KEYSPACE)
        # Chuẩn bị câu lệnh insert
        insert_statement = session.prepare("""INSERT INTO {C_TABLE} (create_time,bid,campaign_id,custom_track,group_id,job_id,publisher_id,ts) VALUES (?,?,?,?,?,?,?,?)""")
        # Đọc messages từ Kafka và ghi vào Cassandra
        for message in kafka_c:
            data = message.value
            session.execute(insert_statement, (data["create_time"], data["bid"], data["campaign_id"], data["custom_track"], data["group_id"], data["job_id"], data["publisher_id"], data["ts"]))
            print(f"Inserted: {data}")

    except Exception as e:
        print(f"An error occurred: {str(e)}")
    finally:
        # Đóng kết nối
        kafka_c.close()
        cluster.shutdown()

if __name__ == "__main__":
    streamingData()