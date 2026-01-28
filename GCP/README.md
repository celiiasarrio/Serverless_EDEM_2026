![Logo](https://n3m5z7t4.rocketcdn.me/wp-content/plugins/edem-shortcodes/public/img/logo-Edem.png)

# Serverless Data Processing in Google Cloud
EDEM 2026

- Professors: 
    - [Javi Briones](https://github.com/jabrio)
    - [Adriana Campos](https://github.com/AdrianaC304)

#### Case description
<img src="https://www.logo.wine/a/logo/Spotify/Spotify-Icon-Logo.wine.svg" alt="WFM" width="170" h/>

Spotify seeks to enhance the listening experience of podcast users by combining offline content enrichment with real-time personalization. Podcast episodes uploaded to the platform must be processed to extract meaningful information from audio content, including audio transcription and thematic labelling, as well as the processing and optimization of visual assets used for content discovery across the platform.

At the same time, Spotify must process real-time user interaction data generated during listening sessions to compute live metrics, dynamically adapt recommendations, and trigger notifications when relevant. The system must handle continuous streams of user events, combine them with enriched content information, and react within seconds to changes in user behavior.

#### Business challenges

- Transform unstructured audio into structured information through transcription and labelling.

- Process and optimize visual assets associated with podcast content, while detecting and properly cataloging sensitive content to ensure a safe and compliant listening experience.

- Compute metrics and recommendations in real time during listening sessions and deliver timely notifications based on user behavior and content relevance.

- Design an architecture capable of supporting millions of concurrent listeners while maintaining consistency between offline and real-time data processing.

#### Homework Assignment

- The infrastructure must be managed as a Terraform project, allowing the entire architecture to be deployed seamlessly with a single **terraform apply** command. **[Homework Assignment]**

#### Data Architecture
<img src="00_DocAux/serverless_edem_2026_architecture.png" width="1500"/>

## Setup Requirements

- [Google Cloud Platform - Free trial](https://console.cloud.google.com/freetrial)
- [Install Cloud SDK](https://cloud.google.com/sdk/docs/install)
- Clone this **repo**
- For this demo, we will be able to work both **locally** and in the **cloud shell**.
- Run this command to authenticate yourself with your GCP account (only locally).

```
    gcloud init
    gcloud auth application-default login
```

- Enable required *Google Cloud APIs* by running the following commands:

```
gcloud services enable dataflow.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable vision.googleapis.com
gcloud services enable cloudbuild.googleapis.com
gcloud services enable cloudfunctions.googleapis.com
gcloud services enable run.googleapis.com
gcloud services enable logging.googleapis.com
gcloud services enable artifactregistry.googleapis.com
gcloud services enable eventarc.googleapis.com
gcloud services enable firestore.googleapis.com
gcloud services enable secretmanager.googleapis.com
```

- Create a Python environment using Anaconda, selecting **Python 3.12** as the version.

- **Activate** the recently created environment.
```
conda activate <ENVIRONTMENT_NAME>
```

*Alternatively, you can use venv, ensuring that your Python version is **3.12***

- Create Python environment (Windows - Using CMD terminal)
```
python -m venv <ENVIRONTMENT_NAME>
<ENVIRONMENT_NAME>\Scripts\activate.bat
```

- Create Python Environment (Mac)
```
python -m venv <ENVIRONTMENT_NAME>
source <ENVIRONMENT_NAME>/bin/activate
```

- Install python dependencies by running the following command:

```
cd /GCP
pip install -r requirements.txt
```

## PubSub

Go to the [Google Pub/Sub console](https://console.cloud.google.com/cloudpubsub) and **create the necessary topics** to simulate the different data sources, making sure to check the option to create a default subscription. These topics will be responsible for collecting all data emitted by user actions.

## Google Cloud Storage

Now, go to the [Google Cloud Storage](https://console.cloud.google.com/storage) and create two buckets. These buckets must have a **globally unique name**, be **regional**, and will serve as storage for temporary and staging files that Dataflow will need during its execution, as well as for server storage.

- A command to upload the necessary audio and image files to the corresponding storage locations for the practice:

```
gsutil -m cp -r ./02_Code/00_Dataflow/00_DocAux gs://<YOUR_BUCKET_NAME>/
```

- Set the required file metadata

```
gsutil setmeta \
  -h "x-goog-meta-title:Passive Income Expert: Buying A House Makes You Poorer Than Renting! Crypto Isn't A Smart Investment" \
  -h "x-goog-meta-duration:2:15:03" \
  -h "x-goog-meta-show_id:The Diary Of A CEO" \
  -h "x-goog-meta-status:processed" \
  -h "x-goog-meta-episode_id:ep_2020" \
  gs://edem-serverless-spotify-demo/audio/podcast_audio.wav
```

```
gsutil setmeta \
  -h "x-goog-meta-title:Thierry Henry: I Was Depressed, Crying & Dealing With Trauma!" \
  -h "x-goog-meta-duration:1:54:12" \
  -h "x-goog-meta-show_id:The Diary Of A CEO" \
  -h "x-goog-meta-status:processed" \
  -h "x-goog-meta-episode_id:ep_2021" \
  gs://<YOUR_BUCKET_NAME>/audio/podcast_audio_02.wav
```

```
gsutil setmeta \
  -h "x-goog-meta-episode_id:ep_2021" \
  gs://<YOUR_BUCKET_NAME>/images/doac_cover.jpg
```

## Google Cloud Firestore

Go to the [Google Cloud Firestore Console](https://console.cloud.google.com/firestore) and create a database **(default) in native mode**. This will allow us to store all the data sent by the different systems.

## Big Query

Crear una tabla en Big Query con el nombre edem_data.

## Google Cloud Artifact Registry

-  As a first step, go to the [Artifact Registry Console](https://console.cloud.google.com/artifacts) and create a repository with the default values. Alternatively, you can create it using the CLI:

```
gcloud artifacts repositories create <YOUR_REPOSITORY_NAME> \
 --repository-format=docker \
 --location=<YOUR_REGION_ID>
```

- Run the following command to ensure that Docker is properly configured to authenticate with Artifact Registry.

```
gcloud auth configure-docker <YOUR_REGION_ID>-docker.pkg.dev
```

## Run Dataflow

#### A. Batch

```
cd ./GCP/02_Code/00_Dataflow/01_Batch
```

- From **Local**

```
python edem_podcast_content_processing_batch.py \
    --project_id <YOUR_PROJECT_ID> \
    --bucket_name <YOUR_BUCKET_NAME> \
    --firestore_collection <YOUR_FIRESTORE_COLLECTION_NAME> \
    --bigquery_dataset <YOUR_BIGQUERY_DATASET_NAME> \
    --bigquery_table <YOUR_AUDIO_BIGQUERY_TABLE_NAME>
```

```
python edem_thumbnail_processing_batch.py \
    --project_id <YOUR_PROJECT_ID> \
    --bucket_name <YOUR_BUCKET_NAME> \
    --firestore_collection <YOUR_FIRESTORE_COLLECTION_NAME> \
    --bigquery_dataset <YOUR_BIGQUERY_DATASET_NAME> \
    --bigquery_table <YOUR_IMAGE_BIGQUERY_TABLE_NAME>
```

- Run Pipeline in GCP: **Dataflow**

```
python edem_podcast_content_processing_batch.py \
    --project_id <YOUR_PROJECT_ID> \
    --bucket_name <YOUR_BUCKET_NAME> \
    --firestore_collection <YOUR_FIRESTORE_COLLECTION_NAME> \
    --bigquery_dataset <YOUR_BIGQUERY_DATASET_NAME> \
    --bigquery_table <YOUR_AUDIO_BIGQUERY_TABLE_NAME> \
    --runner DataflowRunner \
    --job_name <YOUR_DATAFLOW_JOB_NAME> \
    --region <YOUR_REGION_ID> \
    --temp_location gs://<YOUR_GCS_BASE_BUCKET>/tmp \
    --staging_location gs://<YOUR_GCS_BASE_BUCKET>/stg \
    --requirements_file ./requirements.txt
```

```
python edem_thumbnail_processing_batch.py \
    --project_id <YOUR_PROJECT_ID> \
    --bucket_name <YOUR_BUCKET_NAME> \
    --firestore_collection <YOUR_FIRESTORE_COLLECTION_NAME> \
    --bigquery_dataset <YOUR_BIGQUERY_DATASET_NAME> \
    --bigquery_table <YOUR_IMAGE_BIGQUERY_TABLE_NAME> \
    --runner DataflowRunner \
    --job_name <YOUR_DATAFLOW_JOB_NAME> \
    --region <YOUR_REGION_ID> \
    --temp_location gs://<YOUR_GCS_BASE_BUCKET>/tmp \
    --staging_location gs://<YOUR_GCS_BASE_BUCKET>/stg \
    --requirements_file ./requirements.txt
```

<img src="./02_Code/00_Dataflow/00_DocAux/.images/edem_dataflow_batch_01.png" width="1500"/>

<img src="./02_Code/00_Dataflow/00_DocAux/.images/edem_dataflow_batch_02.png" width="1500"/>

- Model Output (Speech to Text,Label Detection & SafeSearch)

<img src="./02_Code/00_Dataflow/00_DocAux/.images/edem_dataflow_batch_03.png" width="1500"/>

```
Results for SpeechToText:
  Transcription: "On the other side, if you have a mortgage rate that's say 6% or higher, well, when you pay off that mortgage, essentially you're locking in a guaranteed return of that interest rate..."
  Label: business
```

```
Results for SafeSearch:
  is_sensitive: False
```

#### B. Streaming

```
cd ./GCP/02_Code/00_Dataflow/02_Streaming
```

- Run **Generator**

```
python edem_data_generator.py \
    --project_id <PROJECT_ID> \
    --playback_topic <YOUR_PLAYBACK_PUBSUB_TOPIC_NAME> \
    --engagement_topic <YOUR_ENGAGEMENT_PUBSUB_TOPIC_NAME> \
    --quality_topic <YOUR_QUALITY_PUBSUB_TOPIC_NAME> \
    --firestore_collection <YOUR_FIRESTORE_COLLECTION> 
```

- Run Streaming pipeline locally:

```
python edem_realtime_recommendation_engine.py \
    --project_id <PROJECT_ID> \
    --playback_pubsub_topic <YOUR_PLAYBACK_PUBSUB_TOPIC_NAME> \
    --engagement_pubsub_topic <YOUR_ENGAGEMENT_PUBSUB_TOPIC_NAME> \
    --quality_pubsub_topic <YOUR_QUALITY_PUBSUB_TOPIC_NAME> \
    --notifications_pubsub_topic <YOUR_NOTIFICATION_PUBSUB_TOPIC_NAME> \
    --firestore_collection <YOUR_FIRESTORE_COLLECTION> \
    --bigquery_dataset <YOUR_BIGQUERY_DATASET> \
    --user_bigquery_table <YOUR_USER_BIGQUERY_TABLE> \
    --episode_bigquery_table <YOUR_EPISODE_BIGQUERY_TABLE> 
```

- Run Pipeline in GCP: **Dataflow**

```
python edem_realtime_recommendation_engine.py \
    --project_id <PROJECT_ID> \
    --playback_pubsub_topic <YOUR_PLAYBACK_PUBSUB_TOPIC_NAME> \
    --engagement_pubsub_topic <YOUR_ENGAGEMENT_PUBSUB_TOPIC_NAME> \
    --quality_pubsub_topic <YOUR_QUALITY_PUBSUB_TOPIC_NAME> \
    --notifications_pubsub_topic <YOUR_NOTIFICATION_PUBSUB_TOPIC_NAME> \
    --firestore_collection <YOUR_FIRESTORE_COLLECTION> \
    --bigquery_dataset <YOUR_BIGQUERY_DATASET> \
    --user_bigquery_table <YOUR_USER_BIGQUERY_TABLE> \
    --episode_bigquery_table <YOUR_EPISODE_BIGQUERY_TABLE> \
    --runner DataflowRunner \
    --job_name <YOUR_DATAFLOW_JOB_NAME> \
    --region <YOUR_REGION_ID> \
    --temp_location gs://<YOUR_GCS_BASE_BUCKET>/tmp \
    --staging_location gs://<YOUR_GCS_BASE_BUCKET>/stg \
    --requirements_file ./requirements.txt
```

## Dataflow Flex Templates

- Build Dataflow Flex Template

```
gcloud dataflow flex-template build gs://<YOUR_BASE_BUCKET_NAME>/<YOUR_TEMPLATE_NAME>.json \
    --image-gcr-path "<YOUR_REGION_ID>-docker.pkg.dev/<YOUR_PROJECT_ID>/<YOUR_REPOSITORY_NAME>/<YOUR_IMAGE_NAME>:latest" \
    --sdk-language "PYTHON" \
    --flex-template-base-image "PYTHON3" \
    --py-path "." \
    --env "FLEX_TEMPLATE_PYTHON_PY_FILE=<YOUR_PATH_TO_THE_PY_FILE>" \
    --env "FLEX_TEMPLATE_PYTHON_REQUIREMENTS_FILE=YOUR_PATH_TO_THE_REQUIREMENTS_FILE"
```

- Run Dataflow Flex Template

```
gcloud dataflow flex-template run "<YOUR_DATAFLOW_JOB_NAME>" \
 --template-file-gcs-location="gs://<YOUR_BUCKET_NAME>/<YOUR_TEMPLATE_NAME>.json" \
 --parameters project_id "<PROJECT_ID>", playback_pubsub_topic="<YOUR_PLAYBACK_PUBSUB_TOPIC_NAME>", engagement_pubsub_topic="<YOUR_ENGAGEMENT_PUBSUB_TOPIC_NAME>", quality_pubsub_topic="<YOUR_QUALITY_PUBSUB_TOPIC_NAME>", notifications_pubsub_topic="<YOUR_NOTIFICATION_PUBSUB_TOPIC_NAME>", firestore_collection="<YOUR_FIRESTORE_COLLECTION>", bigquery_dataset="<YOUR_BIGQUERY_DATASET>", user_bigquery_table="<YOUR_USER_BIGQUERY_TABLE>", episode_bigquery_table="<YOUR_EPISODE_BIGQUERY_TABLE>" \
 --region=<YOUR_REGION_ID> \
 --max-workers=1
```

## CI/CD: Cloud Build

- Go to the [Cloud Build console](https://console.cloud.google.com/cloud-build)
- In the left panel, select *Repositories*.
- In the *2nd Gen* tab, click on **Link Repository**.
- In the *Connection* dropdown, click on **Create host connection** and link your GitHub account:
    - Select only the repositories associated with your account that you want to link.
    - Click install.
    - Verify that the connection is created successfully.

- In the left panel, select *Triggers*.
    - Give it a name and select a specific region.
    - The event will be **Push to a branch**.
    - In *Repository*, connect to a new repository and s**elect the repository previously chosen in the connection**.
    - Click on **Connect**.
    - Select the **branch** this trigger will listen for changes on.
    - As configuration, select **Cloud Build configuration file (yaml or json)**.
    - For location, add the path to your proper [build.yml](./02_Code/03_CICD) file. Alternatively, you can select inline and copy and paste the content of the file.
    - Select a service account with sufficient permissions to execute a Dataflow job (*If you do not specify a service account, it will use the default Compute Engine service account*)
    - Click on **Create**.

- Once the trigger is created, each new push to the specified branch will trigger the actions specified in the build file, following the steps we set.

## Clean Up

- List your Dataflow pipelines 

```
gcloud dataflow jobs list --region=<YOUR_REGION_ID>
```

- Stop the dataflow job:

```
gcloud dataflow jobs cancel <YOUR_JOB_ID> --region=<YOUR_REGION_ID>
```

- Remove your PubSub Topics and Subscriptions

```
gcloud pubsub topics delete <YOUR_TOPIC_NAME>
gcloud pubsub subscriptions delete <YOUR_SUBSCRIPTION_NAME>
```

- Remove your Artifact Registry Repository

```
gcloud artifacts repositories delete <YOUR_REPOSITORY_NAME> --location=<YOUR_REGION_ID>
```

- Remove your Cloud Function

```
gcloud functions delete <YOUR_CLOUD_FUNCTION_NAME> --region <YOUR_REGION_ID>
```

- Remove your Cloud Run Service

```
gcloud run services delete <YOUR_CLOUR_RUN_SERVICE_NAME> --platform=managed --region=<YOUR_REGION_ID>
```

- Remove your Cloud Run Job

```
gcloud run jobs delete <YOUR_CLOUD_RUN_JOB_NAME> --region=<YOUR_REGION_ID>
```

- Disable the required Google APIs

```
gcloud services disable dataflow.googleapis.com
gcloud services disable pubsub.googleapis.com
gcloud services disable vision.googleapis.com
gcloud services disable cloudbuild.googleapis.com
gcloud services disable cloudfunctions.googleapis.com
gcloud services disable run.googleapis.com
gcloud services disable logging.googleapis.com
gcloud services disable artifactregistry.googleapis.com
gcloud services disable eventarc.googleapis.com
gcloud services disable firestore.googleapis.com
gcloud services disable secretmanager.googleapis.com

```

## Bibliography & Additional Resources

- Dataflow

    - [Apache Beam Basics](https://beam.apache.org/documentation/programming-guide/)
    
    - [Apache Beam ML](https://beam.apache.org/documentation/ml/about-ml/)
    
    - [Dataflow Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
    
    - [Dataflow Practical Exercises Guide](https://cloud.google.com/dataflow/docs/guides/)

- IAM
    - https://cloud.google.com/iam/docs/service-accounts-create
    - https://cloud.google.com/iam/docs/understanding-roles
 
- Firestore
    - https://firebase.google.com/docs/firestore/quickstart#python

- Cloud Functions
    - https://cloud.google.com/functions/docs/console-quickstart

- Artifact Registry
    - https://cloud.google.com/artifact-registry/docs/repositories/create-repos

- Cloud Run
    - https://cloud.google.com/run/docs/deploying
    - https://cloud.google.com/sql/docs/postgres/connect-run