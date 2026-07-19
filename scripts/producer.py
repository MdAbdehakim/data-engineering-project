import json
import os
import random
import time
import uuid
from datetime import datetime

from kafka import KafkaProducer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions_ecommerce")


def run_producer():
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print(f"🚀 Démarrage du Producer... Envoi des données vers le topic : {KAFKA_TOPIC}")

    try:
        while True:
            transaction = {
                "id_transaction": str(uuid.uuid4()),
                "user_id": random.randint(1, 100),
                "montant": round(random.uniform(10.5, 500.0), 2),
                "methode_paiement": random.choice(["Carte Bancaire", "PayPal", "Virement"]),
                "statut": random.choices(["SUCCESS", "FAILED"], weights=[0.9, 0.1])[0],
                "event_timestamp": datetime.utcnow().isoformat(),
            }

            producer.send(
                KAFKA_TOPIC,
                key=str(transaction["user_id"]).encode("utf-8"),
                value=transaction,
            )
            print(f"✅ Transaction envoyée : {transaction}")

            time.sleep(2)
    except KeyboardInterrupt:
        print("\n🛑 Arrêt du Producer.")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    run_producer()