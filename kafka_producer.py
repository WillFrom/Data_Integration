import pandas as pd
import time
from kafka import KafkaProducer
import json

# Chargement du fichier CSV
df = pd.read_csv(r'C:\Users\33660\Documents\Data Integrations\total-population.csv') 

# Création d'un producteur Kafka
producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Nom du topic Kafka
topic = 'population-data'

# Découpage du fichier en morceaux de 100 lignes
batch_size = 100
for start in range(0, len(df), batch_size):
    end = start + batch_size
    batch = df.iloc[start:end]
    
    # Conversion du batch en liste de dictionnaires pour chaque ligne
    records = batch.to_dict(orient='records')
    
    try:
        # Envoi du batch de données au topic Kafka
        future = producer.send(topic, value=records)
        
        # Attente de la confirmation de l'envoi (bloque jusqu'à ce que l'envoi soit confirmé)
        record_metadata = future.get(timeout=10)
        
        print(f"Envoi de {len(records)} enregistrements au topic Kafka.")
        print(f"Message envoyé à {record_metadata.topic} partition {record_metadata.partition} avec offset {record_metadata.offset}")
    
    except Exception as e:
        print(f"Erreur lors de l'envoi du message : {e}")
    
    # Attendre 10 secondes avant d'envoyer le prochain batch
    time.sleep(10)

# Exemple de message supplémentaires (Pas de nouveaux messages si le csv n'est pas maj)
additional_data = [
    {'city': 'Berlin', 'population': 3769000},
    {'city': 'London', 'population': 8982000},
    {'city': 'Los Angeles', 'population': 3990456}
]

# Envoi des nouveaux messages au topic Kafka
for record in additional_data:
    producer.send(topic, value=record)
    print(f"Envoi du message: {record}")
    time.sleep(1)

# Fermer le producteur Kafka
producer.close()
