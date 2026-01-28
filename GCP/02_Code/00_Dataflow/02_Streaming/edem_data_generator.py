"""
Spotify Podcast Data Generator
Generates synthetic podcast streaming data and publishes to Pub/Sub topics.

EDEM. Master Big Data & Cloud 2025/2026
Professor: Javi Briones & Adriana Campos
"""

# Import libraries
from datetime import datetime, timezone
from google.cloud import pubsub_v1
from google.cloud import firestore
import argparse
import requests
import logging
import random
import json
import uuid
import time

""" Code: Helpful functions """
def iso_now():
    """
    Get the current timestamp in ISO format.
    Returns:
        str: Current timestamp in ISO format.
    """
    
    return datetime.now(timezone.utc).isoformat()

def publish_message(topic_name, payload, publisher, project_id):
    """
    Publish a message to a Pub/Sub topic.
    Args:
        topic_name (str): Name of the Pub/Sub topic.
        payload (dict): Message payload.
        publisher (pubsub_v1.PublisherClient): Pub/Sub publisher client.
        project_id (str): GCP project ID.
    Returns:
        None
    """

    topic_path = publisher.topic_path(project_id, topic_name)
    publisher.publish(topic_path, json.dumps(payload).encode("utf-8"))
    logging.info(f"Published message to {topic_name}.")

# --- Replace these with Firestore/API later ---

def get_firestore_data(firestore_collection, project_id):
    """
    Pick a random user or episode from Firestore.
    Args:
        firestore_collection (str): Firestore collection name.
        project_id (str): GCP project ID.
    Returns:
        data (str): User/Episode data.
    """

    try:
        db = firestore.Client(project=project_id)

        doc_ref = db.collection(firestore_collection).stream()
        id = random.choice([doc.id for doc in doc_ref])

        data = db.collection(firestore_collection).document(id).get().to_dict()
        return data
    
    except Exception as e:
        raise Exception(f"Failed to pick user from Firestore: {e}")
    
    finally:
        db.close()
    
""" Simulate Session Data """
sessions = {} 

def new_session(user, episode):

    """
    Create a new session for a user and episode.
    Args:
        user (dict): User data.
        episode (dict): Episode data.
    Returns:
        None
    """
    
    sessions[user["user_id"]] = {
        "session_id": str(uuid.uuid4()),
        "episode": episode,
        "position": 0,
        "state": "IDLE",
    }

def playback_event(user, kind):

    """
    Create a playback event payload.
    Args:
        user (dict): User data.
        kind (str): Event type (PLAY/PAUSE/RESUME/STOP/COMPLETE).
    Returns:
        dict: Playback event payload.
    """

    s = sessions[user["user_id"]]
    episode = s["episode"]
    return {
        "event_id": str(uuid.uuid4()),
        "event_time": iso_now(),
        "event_type": kind,                  
        "user_id": user["user_id"],
        "session_id": s["session_id"],
        "episode_id": episode["episode_id"],
        "show_id": episode["show_id"],
        "position_sec": int(s["position"]),
        "duration_sec": episode["duration_sec"],
        "device_type": user["device_type"],
        "country": user["country"],
    }

def engagement_event(user, kind):

    """
    Create an engagement event payload.
    Args:
        user (dict): User data.
        kind (str): Event type (SAVE_EPISODE/FOLLOW_SHOW/SHARE).
    Returns:
        dict: Engagement event payload.
    """

    s = sessions[user["user_id"]]
    episode = s["episode"]
    payload = {
        "event_id": str(uuid.uuid4()),
        "event_time": iso_now(),
        "event_type": kind,
        "user_id": user["user_id"],
        "episode_id": episode["episode_id"],
        "show_id": episode["show_id"],
        "country": user["country"],
    }

    if kind == "SHARE":
        payload["share_channel"] = random.choice(["whatsapp", "copy_link", "email"])

    return payload

def quality_event(user, kind):

    """
    Create a quality event payload.
    Args:
        user (dict): User data.
        kind (str): Event type (BUFFERING_START/BUFFERING_END/DROPOUT).
    Returns:
        dict: Quality event payload.
    """

    s = sessions[user["user_id"]]
    episode = s["episode"]
    payload = {
        "event_id": str(uuid.uuid4()),
        "event_time": iso_now(),
        "event_type": kind,
        "user_id": user["user_id"],
        "session_id": s["session_id"],
        "episode_id": episode["episode_id"],
        "device_type": user["device_type"],
        "country": user["country"],
    }

    if kind in ("BUFFERING_START", "BUFFERING_END"):
        payload["buffer_ms"] = random.randint(200, 5000)

    if kind == "DROPOUT":
        payload["drop_reason"] = random.choice(["network", "device", "server"])

    return payload

def run_streaming(project_id, firestore_collection, playback_topic, engagement_topic, quality_topic, publisher, user):

    uid = user["user_id"]
    if uid not in sessions or sessions[uid]["state"] == "ENDED":
        ep = get_firestore_data(firestore_collection, project_id)
        new_session(user, ep)
        s = sessions[uid]
        s["state"] = "PLAYING"
        publish_message(playback_topic, playback_event(user, "PLAY"), publisher, project_id)
        return

    s = sessions[uid]
    ep = s["episode"]

    if s["state"] == "PLAYING":
        s["position"] += random.randint(5, 40)

        # occasionally emit QoE while playing
        if random.random() < 0.12:
            publish_message(quality_topic, quality_event(user, random.choice(["BUFFERING_START", "BUFFERING_END"])), publisher, project_id)
        if random.random() < 0.03:
            publish_message(quality_topic, quality_event(user, "DROPOUT"), publisher, project_id)

        # occasionally emit engagement signals
        if random.random() < 0.08:
            publish_message(engagement_topic, engagement_event(user, random.choice(["SAVE_EPISODE", "FOLLOW_SHOW", "SHARE"])), publisher, project_id)
        # state transitions
        if s["position"] >= ep["duration_sec"]:
            s["position"] = ep["duration_sec"]
            s["state"] = "ENDED"
            publish_message(playback_topic, playback_event(user, "COMPLETE"), publisher, project_id)
        elif random.random() < 0.15:
            s["state"] = "PAUSED"
            publish_message(playback_topic, playback_event(user, "PAUSE"), publisher, project_id)
        elif random.random() < 0.08:
            s["state"] = "ENDED"
            publish_message(playback_topic, playback_event(user, "STOP"), publisher, project_id)

    elif s["state"] == "PAUSED":
        # resume or end
        if random.random() < 0.75:
            s["state"] = "PLAYING"
            publish_message(playback_topic, playback_event(user, "RESUME"), publisher, project_id)
        else:
            s["state"] = "ENDED"
            publish_message(playback_topic, playback_event(user, "STOP"), publisher, project_id)

""" Input Params """

parser = argparse.ArgumentParser(description=('Spotify User Data Generator.'))

parser.add_argument('--project_id',
    required=True,
    help='GCP cloud project name.')

parser.add_argument('--playback_topic',
    required=True,
    help='GCP PubSub topic for playback data.')

parser.add_argument('--engagement_topic',
    required=True,
    help='GCP PubSub topic for engagement data.')

parser.add_argument('--quality_topic',
    required=True,
    help='GCP PubSub topic for quality data.')

parser.add_argument('--user_firestore_collection',
    required=True,
    help='Firestore collection name for user data.')

parser.add_argument('--episode_firestore_collection',
    required=True,
    help='Firestore collection name for episode data.') 

args, opts = parser.parse_known_args()

if __name__ == "__main__":

    # Set Logs
    logging.getLogger().setLevel(logging.INFO)

    # Run Generator
    logging.info('Initializing the data generator.')

    publisher = pubsub_v1.PublisherClient()

    while True:

        user_data = get_firestore_data(
            firestore_collection=args.user_firestore_collection, 
            project_id=args.project_id)

        run_streaming(
            project_id = args.project_id,
            firestore_collection=args.episode_firestore_collection,
            playback_topic = args.playback_topic,
            engagement_topic = args.engagement_topic,
            quality_topic = args.quality_topic,
            publisher = publisher,
            user = user_data
        )

        time.sleep(1)