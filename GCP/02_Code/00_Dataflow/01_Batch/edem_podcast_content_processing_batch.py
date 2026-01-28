""" 
Script: Dataflow Batch Pipeline

Description:
This script implements a Dataflow batch pipeline that processes podcast audio files stored in Google Cloud Storage (GCS). The pipeline performs the following steps:
1. Reads audio files from a specified GCS bucket.
2. Transcribes the audio content using a pre-trained Whisper model from Hugging Face.
3. Classifies the transcribed text into topics using a pre-trained RoBERTa model from Hugging Face.
4. Stores the transcription and topic classification results in both Firestore and BigQuery.

EDEM. Master Big Data & Cloud 2025/2026
Professor: Javi Briones & Adriana Campos
"""

""" Import Libraries """

# A. Apache Beam Libraries
import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems

# B. Apache Beam ML Libraries
from apache_beam.ml.inference.huggingface_inference import HuggingFacePipelineModelHandler
from apache_beam.ml.inference.huggingface_inference import PipelineTask
from apache_beam.ml.inference.base import KeyedModelHandler
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference

# C. Python Libraries
import soundfile as sf
import numpy as np
import argparse
import logging
import io

""" Code: Helpful functions """

# Model Handler

audio_model_handler = HuggingFacePipelineModelHandler(
    task=PipelineTask.AutomaticSpeechRecognition,
    model="openai/whisper-small",
    load_pipeline_args={"framework": "pt", "device": -1},
    inference_args={"return_timestamps": True}
)

topic_model_handler = HuggingFacePipelineModelHandler(
    task=PipelineTask.TextClassification,
    model="textattack/roberta-base-ag-news",
    load_pipeline_args={"framework": "pt", "device": -1},
    inference_args={
        "top_k": 1,
        "truncation": True
    }
)

def label_mapping(result):
    
    """
    Map model output labels to human-readable labels.
    Args:
        result (Tuple[str, PredictionResult]): Tuple containing the path and prediction result from the model.
    Returns:
        payload: Dictionary with 'transcription' and 'label' keys.
    """

    k,v = result

    LABEL_MAP = {
        "LABEL_0": "world",
        "LABEL_1": "sports",
        "LABEL_2": "business",
        "LABEL_3": "sci_tech",
    }

    pred = v.inference[0] if isinstance(v.inference, list) else v.inference
    label = pred["label"]
    
    payload =  {
        "transcription": v.example,                 
        "label": LABEL_MAP.get(label, label),
        "gcs": k
    }

    return payload

def read_audio_files(file_path):

    """
    Read audio file from GCS using SoundFile and return dict with array and sampling rate.
    Args:
        file_path (str): Path to the audio file in GCS.
    Returns:
        dict: Dictionary with 'array' and 'sampling_rate' keys.
    """
    
    with FileSystems.open(file_path) as f:
        data = f.read()

    # Decode audio from memory (without writing to disk)
    audio, sr = sf.read(io.BytesIO(data))

    # Whisper prefers mono
    if audio.ndim > 1:
        audio = np.mean(audio, axis=1)

    return {
        "array": audio, "sampling_rate": sr, "gcs": file_path
    }

def extract_text_from_prediction(prediction):

    """
    Extract transcription text from the prediction result.
    Args:
        prediction (PredictionResult): Prediction result from the model.
    Returns
        str: Transcription text.
    """
    return (prediction.example["gcs"],prediction.inference[0]["text"])


class GetMetadataFromFileDoFn(beam.DoFn):

    def __init__(self, project_id):
        self.project_id = project_id

    def setup(self):
        from google.cloud import storage
        self.client = storage.Client(project=self.project_id)

    def process(self, element):
        from urllib.parse import urlparse

        parsed = urlparse(element['gcs'])
        bucket = parsed.netloc
        blob_name = parsed.path.lstrip("/")

        blob = self.client.bucket(bucket).get_blob(blob_name)

        yield {
            "transcription": element['transcription'],
            "label": element['label'],
            "title": blob.metadata.get("title"),
            "show_id": blob.metadata.get("show_id"),
            "episode_id": blob.metadata.get("episode_id"),
            "duration": blob.metadata.get("duration"),
            "status": blob.metadata.get("status"),
            "duration_sec":blob.metadata.get("duration_sec")
        }

class FormatFirestoreDocument(beam.DoFn):

    def __init__(self,firestore_collection, project_id):
        self.firestore_collection = firestore_collection
        self.project_id = project_id

    def setup(self):
        from google.cloud import firestore
        self.db = firestore.Client(project=self.project_id)

    def process(self, element):

        doc_ref = self.db.collection(self.firestore_collection).document(element['episode_id'])
        doc_ref.set(element)

        logging.info(f"Document written to Firestore: {doc_ref.id}")

""" Code: Dataflow Process """

def run():

    """ Input Arguments """

    parser = argparse.ArgumentParser(description=('Input arguments for the Dataflow Streaming Pipeline.'))

    parser.add_argument(
                '--project_id',
                required=True,
                help='GCP cloud project name.')
    
    parser.add_argument(
                '--bucket_name',
                required=True,
                help='GCS Bucket name.')
    
    parser.add_argument(
                '--firestore_collection',
                required=True,
                help='Firestore collection name.')
    
    parser.add_argument(
                '--bigquery_dataset',
                required=True,
                help='BigQuery dataset name.')
    
    parser.add_argument(
                '--bigquery_table',
                required=True,
                help='BigQuery table name.')
    
    args, pipeline_opts = parser.parse_known_args()

    # Pipeline Options
    options = PipelineOptions(pipeline_opts,
        save_main_session=True, streaming=False, project=args.project_id)
    
    # Pipeline Object
    with beam.Pipeline(argv=pipeline_opts,options=options) as p:

        audio_files = (
            p
                | "MatchFiles" >> fileio.MatchFiles(f'gs://{args.bucket_name}/00_DocAux/audio/*.wav')
                | "ReadFiles" >> fileio.ReadMatches()
                | "ToGCSPath" >> beam.Map(lambda rf: rf.metadata.path)
        )


        processed_audio_files = (
            audio_files
                | "ReadAudioFiles" >> beam.Map(read_audio_files)
                | "TranscribeAudio" >> RunInference(audio_model_handler)
                | "ExtractTranscription" >> beam.Map(extract_text_from_prediction)
                | "ClassifyTopic" >> RunInference(KeyedModelHandler(topic_model_handler))
                | "MapLabelMapping" >> beam.Map(label_mapping)
                | "GetMetadataFromFile" >> beam.ParDo(GetMetadataFromFileDoFn(args.project_id))
        )

        processed_audio_files | "WriteToFirestore" >> beam.ParDo(FormatFirestoreDocument(args.firestore_collection, args.project_id))
        
        (
            (
            processed_audio_files |
            "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=f"{args.project_id}:{args.bigquery_dataset}.{args.bigquery_table}",
                schema='transcription:STRING, label:STRING, title:STRING, show_id:STRING, episode_id:STRING, duration:STRING, status:STRING, duration_sec:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=f"gs://{args.bucket_name}/temp"
            )
        )
        )

if __name__ == '__main__':

    # Set Logs
    logging.basicConfig(level=logging.INFO)

    # Disable logs from apache_beam.utils.subprocess_server
    logging.getLogger("apache_beam.utils.subprocess_server").setLevel(logging.ERROR)

    logging.info("The process started")

    # Run Process
    run()