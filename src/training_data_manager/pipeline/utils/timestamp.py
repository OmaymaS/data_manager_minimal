from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.pvalue import PBegin
from apache_beam.typehints import with_output_types


@with_output_types(str)
class ReadCurrentTimestamp(beam.PTransform):
    """
    This transform will read the current time into a PCollection.
    """

    ZULU_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

    def _get_now_str(self) -> str:
        now = datetime.now(timezone.utc)
        now_str = now.strftime(self.ZULU_FORMAT)

        return now_str

    def expand(self, begin: PBegin):
        return (
            begin
            | "Create Empty Element" >> beam.Create([None])
            | "Create UTC Timestamp"
            >> beam.Map(
                lambda _: self._get_now_str(),
            )
        )
