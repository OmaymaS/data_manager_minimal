from typing import Any, Dict, Iterable, Tuple

import apache_beam as beam
from apache_beam.pvalue import AsDict, TaggedOutput


def sample_fraction(sample_fraction: float):
    from math import ceil
    from random import sample
    from typing import Any, Dict, Iterable, Tuple

    def _do_sample(
        input: Tuple[Any, Iterable[Any]], counts_per_key: Dict[Any, int]
    ) -> Iterable[Tuple[Any, Any]]:
        key, elements = input
        count = counts_per_key[key]
        size = ceil(count * sample_fraction)

        for s in sample(elements, size):
            yield key, s

    return _do_sample


class SamplePercentagePerKey(beam.PTransform):
    def __init__(self, sample_fraction: float, label=None):
        super().__init__(label)
        self._sample_fraction = sample_fraction

    def expand(
        self, input: beam.PCollection[Tuple[Any, Any]]
    ) -> beam.PCollection[Tuple[Any, Iterable[Any]]]:
        count_per_key = input | "CountPerKey" >> beam.combiners.Count.PerKey()

        count_per_key_dict_si = AsDict(count_per_key)

        sampled_data = (
            input
            | "GroupByKey" >> beam.GroupByKey()
            | "DoSamplePercentagePerKey"
            >> beam.ParDo(
                sample_fraction(self._sample_fraction),
                counts_per_key=count_per_key_dict_si,
            )
        )

        return sampled_data


class SplitStratified(beam.PTransform):
    def __init__(self, sample_fraction: float, label=None):
        super().__init__(label)
        self._sample_fraction = sample_fraction

    @staticmethod
    def _tag_by_sample_state(
        joined_data: Dict[str, Iterable[Any]]
    ) -> Iterable[TaggedOutput]:
        sampled = list(joined_data["sampled_data"])
        for element in joined_data["all"]:
            tag = "sampled_subset" if element in sampled else "unsampled_subset"
            yield TaggedOutput(tag, element)

    def expand(self, input):
        sampled_data = input | "Sample part 01" >> SamplePercentagePerKey(
            sample_fraction=self._sample_fraction
        )

        data_by_sampled_tag = (
            {
                "all": input,
                "sampled_data": sampled_data,
            }
            | "CoGroupByKey" >> beam.CoGroupByKey()
            | "DropKeys" >> beam.Values()
            | "Tag difference"
            >> beam.ParDo(self._tag_by_sample_state).with_outputs(
                "sampled_subset", "unsampled_subset"
            )
        )

        return data_by_sampled_tag
