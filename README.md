# playground-apachebeam

Dataflow へパイプラインを構築する方法は２つある。１つ目は CLI 経由で直接 Dataflow へパイプラインを構築する方法。２つ目は GCS 経由でテンプレートをインポートし、値を指定する方法。

### via CLI

```sh
PROJECT_ID=''
REGION=''
BUCKET_NAME=''
TOPIC_ID=''

$ poetry shell
$ python PubSubToGCS.py \
  --project=$PROJECT_ID \
  --region=$REGION \
  --input_topic=projects/$PROJECT_ID/topics/$TOPIC_ID \
  --output_path=gs://$BUCKET_NAME/samples/output \
  --runner=DataflowRunner \
  --window_size=2 \
  --num_shards=2 \
  --temp_location=gs://$BUCKET_NAME/temp
```

```sh
$ gcloud --project=$PROJECT_ID pubsub topics publish $TOPIC_ID --message='hello k-jun!'
```