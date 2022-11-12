import argparse
from datetime import datetime
import logging
import random

from apache_beam import (
    DoFn,
    GroupByKey,
    io,
    ParDo,
    Pipeline,
    PTransform,
    WindowInto,
    WithKeys,
)
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows


class GroupMessagesByFixedWindows(PTransform):
    """A composite transform that groups Pub/Sub messages based on publish time
    and outputs a list of tuples, each containing a message and its publish time.
    """

    def __init__(self, window_size, num_shards=5):
        # Set window size to 60 seconds.
        self.window_size = int(window_size * 60)
        self.num_shards = num_shards

    def expand(self, pcoll):
        return (
            pcoll
            # Bind window info to each element using element timestamp (or publish time).
            | "Window into fixed intervals"
            >> WindowInto(FixedWindows(self.window_size))
            | "Add timestamp to windowed elements" >> ParDo(AddTimestamp())
            # Assign a random key to each windowed element based on the number of shards.
            | "Add key" >> WithKeys(lambda _: random.randint(0, self.num_shards - 1))
            # Group windowed elements by key. All the elements in the same window must fit
            # memory for this. If not, you need to use `beam.util.BatchElements`.
            | "Group by key" >> GroupByKey()
        )


class AddTimestamp(DoFn):
    def process(self, element, publish_time=DoFn.TimestampParam):
        """Processes each windowed element by extracting the message body and its
        publish time into a tuple.
        """
        yield (
            element.decode("utf-8"),
            datetime.utcfromtimestamp(float(publish_time)).strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            ),
        )


class WriteToGCS(DoFn):
    def __init__(self, output_path):
        self.output_path = output_path

    def process(self, key_value, window=DoFn.WindowParam):
        """Write messages in a batch to Google Cloud Storage."""

        ts_format = "%H:%M"
        window_start = window.start.to_utc_datetime().strftime(ts_format)
        window_end = window.end.to_utc_datetime().strftime(ts_format)
        shard_id, batch = key_value
        filename = "-".join([self.output_path, window_start, window_end, str(shard_id)])

        with io.gcsio.GcsIO().open(filename=filename, mode="w") as f:
            for message_body, publish_time in batch:
                f.write(f"{message_body},{publish_time}\n".encode("utf-8"))


class Print(DoFn):
    def process(self, element):
        print(element)
        return []


class CustomOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            "--input_topic",
            help="The Cloud Pub/Sub topic to read from."
            '"projects/<PROJECT_ID>/topics/<TOPIC_ID>".',
        )
        parser.add_value_provider_argument(
            "--window_size",
            type=float,
            default=1.0,
            help="Output file's window size in minutes.",
        )
        parser.add_value_provider_argument(
            "--output_path",
            help="Path of the output GCS file including the prefix.",
        )
        parser.add_value_provider_argument(
            "--num_shards",
            type=int,
            default=5,
            help="Number of shards to use when writing windowed elements to GCS.",
        )


def run():
    # Set `save_main_session` to True so DoFns can access globally imported modules.
    custom_options = PipelineOptions().view_as(CustomOptions)
    pipeline_options = PipelineOptions(streaming=True, save_main_session=True)
    # print(custom_options)

    with Pipeline(options=pipeline_options) as p:
        x = p | "Read from Pub/Sub" >> io.ReadFromPubSub(topic=custom_options.input_topic)
        x | (ParDo(Print()))
        y = x | "Window into" >> GroupMessagesByFixedWindows(custom_options.window_size, custom_options.num_shards)
        y | "Write to GCS" >> ParDo(WriteToGCS(custom_options.output_path))


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    run()
