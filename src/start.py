import os
import subprocess

subprocess.run(["/kafka_1.13-3.0.0/bin/zookeeper-server-start.sh /kafka_2.13-3.0.0/config/zookeeper.properties"])
subprocess.run(["/kafka_2.13-3.0.0/bin/kafka-server-start.sh /kafka_2.13-3.0.0/config/server.properties"])
subprocess.run(["/kafka_2.13-3.0.0/bin/kafka-console-producer.sh —broker-list localhost:9092 —topic mobile_client < /data/datasets/mobile_client.json"])
subprocess.run(["/kafka_2.13-3.0.0/bin/kafka-console-producer.sh —broker-list localhost:9092 —topic web_client < /data/datasets/web_client.json"])
subprocess.run(["/kafka_2.13-3.0.0/bin/kafka-console-consumer.sh —bootstrap-server localhost:9092 —topic out_cm —from-beginning"])

os.system("python3 /SoCPC-hackaton/src/main.py")
