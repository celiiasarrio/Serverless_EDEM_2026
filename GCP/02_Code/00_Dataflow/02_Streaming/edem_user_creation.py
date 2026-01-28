"""
User Creation for Spotify Podcast Data Generator
Generates synthetic user data and publishes to Pub/Sub topics.

EDEM. Master Big Data & Cloud 2025/2026
Professor: Javi Briones & Adriana Campos
"""

from google.cloud import firestore
import argparse
import logging
import random

""" Input Params """

parser = argparse.ArgumentParser(description=('Spotify User Data Generator.'))

parser.add_argument('--project_id',
    required=True,
    help='GCP cloud project name.')

parser.add_argument('--firestore_collection',
    required=False,
    default='users',
    help='Firestore collection name.')

args, opts = parser.parse_known_args()

def main(db, firestore_collection):

    """
    Generates synthetic user data and stores it in Firestore.
    
    Args:
        db: Firestore client instance.
        firestore_collection: Name of the Firestore collection to store user data.
    Returns:
        None
    """

    users_ref = db.collection(firestore_collection)

    for i in range(0, 20):
        user = f"user_{random.randint(1, 9999):04d}"

        payload = {
            'user_id': user,
            'country': random.choice(['US', 'UK', 'DE', 'FR', 'ES']),
            'language': random.choice(['EN', 'ES', 'DE', 'FR']),
            'plan': random.choice(['free', 'basic', 'premium']),
            'notifications_enabled': random.choice([True, False]),
            'device_type': random.choice(['iOS', 'Android', 'Web']),
            'created_at': firestore.SERVER_TIMESTAMP,
            'last_login': firestore.SERVER_TIMESTAMP
        }

        try:
            users_ref.add(payload, document_id=user)
            logging.info(f"Created user document: {user}")
        except Exception as e:
            logging.error(f"Error creating user document {user}: {e}")

if __name__ == "__main__":

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    # Run Generator
    logging.info('Initializing the data generator.')

    client = firestore.Client(project=args.project_id)
    main(db=client, firestore_collection=args.firestore_collection)
