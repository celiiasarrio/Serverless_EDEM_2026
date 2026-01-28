""" 
Script: Dataflow Batch Pipeline

Description:
This script implements a Dataflow batch pipeline that processes image thumbnail files stored in Google Cloud Storage (GCS). The pipeline performs the following steps:
1. Reads image files from a specified GCS bucket.
2. Analyzes the images using Google Cloud Vision API to detect adult content.
3. Stores the analysis results in both Firestore and BigQuery.

EDEM. Master Big Data & Cloud 2025/2026
Professor: Javi Briones
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
from apache_beam.ml.inference.base import PredictionResult
from apache_beam.ml.inference.base import RunInference
from more_itertools import bucket

# C. Python Libraries
import soundfile as sf
import numpy as np
import argparse
import logging
import io

""" Code: Helpful functions """

class VisionSafeSearchDoFn(beam.DoFn):

    def setup(self):
        from google.cloud import vision
        self.client = vision.ImageAnnotatorClient()

    def process(self, element):
        from google.cloud import vision
        image = vision.Image()
        path, image.content = element

        response = self.client.safe_search_detection(image=image)
        safe = response.safe_search_annotation

        likelihood_name = ('UNKNOWN', 'VERY_UNLIKELY', 'UNLIKELY', 'POSSIBLE', 'LIKELY', 'VERY_LIKELY')

        if safe.adult >= vision.Likelihood.LIKELY:
            logging.warning(f"Adult content detected with likelihood: {likelihood_name[safe.adult]}")

            is_sensitive = True

        else:
            is_sensitive = False

        yield {
            "path": path,
            "is_sensitive": is_sensitive
        }

def read_image_bytes(path: str):

    """
    Read image file from GCS and return its bytes.
    Args:
        path (str): GCS URI of the image file.
    Returns:
        bytes: Image file bytes.
    """
    
    with FileSystems.open(path) as f:
        data = f.read()

    return (path,data)

class FormatFirestoreDocument(beam.DoFn):
    def __init__(self, firestore_collection, project_id):
        self.firestore_collection = firestore_collection
        self.project_id = project_id

    def setup(self):
        from google.cloud import firestore
        self.db = firestore.Client(project=self.project_id)
    
    def process(self, element):
        doc_ref = self.db.collection(self.firestore_collection).document(element['episode_id'])

        doc_ref.update({
            "image_url": element['image_url'],
            "is_sensitive": element['is_sensitive']
        })

        logging.info(f"Document updated in Firestore: {doc_ref.id}")    

class GetMetadataFromFileDoFn(beam.DoFn):
    def __init__(self, project_id):
        self.project_id = project_id

    def setup(self):
        from google.cloud import storage
        self.client = storage.Client(project=self.project_id)

    def process(self, element):
        from urllib.parse import urlparse
        
        parsed = urlparse(element['path'])
        bucket_name = parsed.netloc
        blob_name = parsed.path.lstrip("/")

        blob = self.client.bucket(bucket_name).get_blob(blob_name)

        yield {
            "episode_id": blob.metadata.get("episode_id"),
            "is_sensitive": element['is_sensitive'],
            "image_url": element['path']
        }

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

        image_files = (
            p
                | "MatchFiles" >> fileio.MatchFiles(f'gs://{args.bucket_name}/00_DocAux/images/*.jpg')
                | "ReadFiles" >> fileio.ReadMatches()
                | "ToGCSPath" >> beam.Map(lambda rf: rf.metadata.path)
        )

        processed_image_data = (
            image_files
                | "ReadImageFiles" >> beam.Map(read_image_bytes)
                | "SafeSearchDetection" >> beam.ParDo(VisionSafeSearchDoFn())
                | "GetMetadataFromFile" >> beam.ParDo(GetMetadataFromFileDoFn(project_id=args.project_id))
        )

        processed_image_data | "WriteToFirestore" >> beam.ParDo(
            FormatFirestoreDocument(
                firestore_collection=args.firestore_collection, project_id=args.project_id))

        (
            processed_image_data |
            "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table=f"{args.project_id}:{args.bigquery_dataset}.{args.bigquery_table}",
                schema='episode_id:STRING, is_sensitive:BOOLEAN, image_url:STRING',
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                custom_gcs_temp_location=f"gs://{args.bucket_name}/temp"    
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