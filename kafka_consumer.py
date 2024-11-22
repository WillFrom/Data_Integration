from kafka import KafkaConsumer
import json

# Création d'un consommateur Kafka
consumer = KafkaConsumer(
    'population-data',  # Nom du topic Kafka
    bootstrap_servers='localhost:9092',
    group_id='population-group',  # Nom du groupe de consommateurs
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),  # Désérialisation des données JSON
    auto_offset_reset='earliest'  # Lire depuis le début du topic
)

# Consommation des messages en temps réel
for message in consumer:
    data = message.value  # Données reçues
    print(f"Message reçu: {data}")
    # Ici, vous pouvez traiter les données (par exemple, les intégrer dans une base de données ou les analyser)
