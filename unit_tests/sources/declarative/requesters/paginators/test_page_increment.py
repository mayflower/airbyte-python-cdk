#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import json
from typing import Any, Optional

import pytest
import requests

from airbyte_cdk.sources.declarative.requesters.paginators.strategies.page_increment import (
    PageIncrement,
)


@pytest.mark.parametrize(
    "page_size, start_from, last_page_size, last_record, last_page_token_value, expected_next_page_token, expected_offset",
    [
        pytest.param(2, 1, 2, {"id": 1}, 3, 4, 2, id="test_same_page_size_start_from_1"),
        pytest.param(3, 1, 2, {"id": 1}, 3, None, 1, id="test_larger_page_size_start_from_1"),
        pytest.param(2, 0, 2, {"id": 1}, 3, 4, 1, id="test_same_page_size_start_from_0"),
        pytest.param(3, 0, 2, {"id": 1}, 3, None, 0, id="test_larger_page_size_start_from_0"),
        pytest.param(None, 0, 0, None, 2, None, 0, id="test_no_page_size"),
        pytest.param("2", 0, 2, {"id": 1}, 3, 4, 1, id="test_page_size_from_string"),
        pytest.param(
            "{{ config['value'] }}", 0, 2, {"id": 1}, 3, 4, 1, id="test_page_size_from_config"
        ),
        pytest.param(
            2, 0, 2, {"id": 1}, None, 1, 2, id="test_start_from_not_injected_returns_second_page"
        ),
        pytest.param(
            2,
            10,
            2,
            {"id": 1},
            None,
            11,
            2,
            id="test_non_default_start_from_not_injected_returns_next_page",
        ),
    ],
)
def test_page_increment_paginator_strategy(
    page_size,
    start_from,
    last_page_size,
    last_record,
    last_page_token_value,
    expected_next_page_token,
    expected_offset,
):
    paginator_strategy = PageIncrement(
        page_size=page_size, parameters={}, start_from_page=start_from, config={"value": 2}
    )
    assert paginator_strategy.start_from_page == start_from

    response = requests.Response()

    response.headers = {"A_HEADER": "HEADER_VALUE"}
    response_body = {"next": "https://airbyte.io/next_url"}
    response._content = json.dumps(response_body).encode("utf-8")

    next_page_token = paginator_strategy.next_page_token(
        response, last_page_size, last_record, last_page_token_value
    )
    assert expected_next_page_token == next_page_token

    # Validate that the PaginationStrategy is stateless and calling next_page_token() again returns the same result
    next_page_token = paginator_strategy.next_page_token(
        response, last_page_size, last_record, last_page_token_value
    )
    assert expected_next_page_token == next_page_token


@pytest.mark.parametrize(
    "page_size", [pytest.param("{{ config['value'] }}"), pytest.param("not-an-integer")]
)
def test_page_increment_paginator_strategy_malformed_page_size(page_size):
    with pytest.raises(Exception, match=".* is of type <class '.*'>. Expected <class 'int'>"):
        PageIncrement(
            page_size=page_size,
            parameters={},
            start_from_page=0,
            config={"value": "not-an-integer"},
        )


@pytest.mark.parametrize(
    "inject_on_first_request, start_from_page, expected_initial_token",
    [
        pytest.param(True, 0, 0, id="test_with_inject_offset_page_start_from_0"),
        pytest.param(True, 12, 12, id="test_with_inject_offset_page_start_from_12"),
        pytest.param(False, 2, None, id="test_without_inject_offset"),
    ],
)
def test_page_increment_paginator_strategy_initial_token(
    inject_on_first_request: bool, start_from_page: int, expected_initial_token: Optional[Any]
):
    paginator_strategy = PageIncrement(
        page_size=20,
        parameters={},
        start_from_page=start_from_page,
        inject_on_first_request=inject_on_first_request,
        config={},
    )

    assert paginator_strategy.initial_token == expected_initial_token
