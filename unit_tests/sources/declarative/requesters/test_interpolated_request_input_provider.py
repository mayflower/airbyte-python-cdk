#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import pytest as pytest

from airbyte_cdk.sources.declarative.interpolation.interpolated_mapping import InterpolatedMapping
from airbyte_cdk.sources.declarative.requesters.request_options.interpolated_request_input_provider import (
    InterpolatedRequestInputProvider,
)


@pytest.mark.parametrize(
    "test_name, input_request_data, expected_request_data",
    [
        (
            "test_static_map_data",
            {"a_static_request_param": "a_static_value"},
            {"a_static_request_param": "a_static_value"},
        ),
        (
            "test_map_depends_on_stream_slice",
            {"read_from_slice": "{{ stream_slice['slice_key'] }}"},
            {"read_from_slice": "slice_value"},
        ),
        (
            "test_map_depends_on_config",
            {"read_from_config": "{{ config['config_key'] }}"},
            {"read_from_config": "value_of_config"},
        ),
        (
            "test_map_depends_on_parameters",
            {"read_from_parameters": "{{ parameters['read_from_parameters'] }}"},
            {"read_from_parameters": "value_of_parameters"},
        ),
        ("test_defaults_to_empty_dictionary", None, {}),
    ],
)
def test_initialize_interpolated_mapping_request_input_provider(
    test_name, input_request_data, expected_request_data
):
    config = {"config_key": "value_of_config"}
    stream_slice = {"slice_key": "slice_value"}
    parameters = {"read_from_parameters": "value_of_parameters"}
    provider = InterpolatedRequestInputProvider(
        request_inputs=input_request_data, config=config, parameters=parameters
    )
    actual_request_data = provider.eval_request_inputs(stream_slice=stream_slice)

    assert isinstance(provider._interpolator, InterpolatedMapping)
    assert actual_request_data == expected_request_data
