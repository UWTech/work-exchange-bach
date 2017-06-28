"""Tests for the orchestration"""
# Import base modules
import os
import json
import pytest
import unittest
import logging
import mock
# from unittest.mock import patch, create_autospec

# Import pip modules
import pika

# Import code
os.environ['LOG_LEVEL'] = 'DEBUG'
import bach

def output_to_stdout(channel, routing_key, value, body, reply_to=None):
    print(channel)
    print(routing_key)

# def test_request_list():
#     """Test adding and removing requests"""
#     request_list = bach.Bach()
#     request_id = request_list.add_request_to_queue("score", "rubric", {"body":"body"})
#     assert isinstance(request_id, str) is True
#     assert request_list.check_in_list(request_id)
#     request = request_list.get_request(request_id)
#     assert request.rubric == "rubric"
#     assert request.body == {"body":"body"}
#     assert request_list.remove_request_from_queue(request_id) is True
#     assert request_list.check_in_list(request_id) is False

@mock.patch('bach.send_to_rabbit', side_effect=output_to_stdout)
def test_request_validator(mockStR, caplog):
    caplog.setLevel(logging.DEBUG)
    request_list = bach.Bach(init_empty=True)
    request_id = request_list.add_request_to_queue("cf", "new_org", {"body":"body"})
    assert 'Invalid request' in caplog.text()
    request = request_list.get_request(request_id)
    # print(request)
    # request_list.process_request(request, None)
    mockStR.assert_not_called()
    assert request == 404

@mock.patch('bach.send_to_rabbit', side_effect=output_to_stdout)
def test_request_processor(mockStR, caplog):
    request_list = bach.Bach()
    body = {
        "test_key1": "test",
        "test_key2": ["string"],
        "test_key3": True,
        "test_key4": 34
    }
    request_id = request_list.add_request_to_queue("test_bach", "test_rubric1", body)
    assert 'Invalid request' not in caplog.text()
    request = request_list.get_request(request_id)
    assert request != 404
    request_list.process_request(request, None)
    mockStR.assert_called_once()
    mockStR.assert_called_with(None,
                               "tester.task1",
                               1,
                               {'assign_to_key': 'test_task1_results',
                                'clone_url': 'test',
                                'pipeline_name': 'test-34-test',
                                'include_git': True},
                               "request.id.{0}".format(request_id))
    assert 'Task complete' in caplog.text()

@mock.patch('bach.send_to_rabbit', side_effect=output_to_stdout)
@mock.patch('pika.BlockingConnection.channel')
def test_request_router(channel, mockStR, caplog):
    test_pika_method = pika.spec.Basic.Deliver()
    test_pika_props = pika.spec.BasicProperties()
    request_list = bach.Bach()
    body = {
        "test_key1": "test",
        "test_key2": ["string"],
        "test_key3": True,
        "test_key4": 34
    }
    request_id = request_list.add_request_to_queue("test_bach", "test_rubric1", body)
    request = request_list.get_request(request_id)
    request_list.process_request(request, None)
    assert 'Task complete' in caplog.text()
    test_pika_method.routing_key = "request.id.{}".format(request_id)
    test_pika_method.delivery_tag = "blahs"
    test_pika_props.correlation_id = 1
    response = {
        'key':'test_task1_results',
        'value':'https://new-url.com'
    }
    request_list.router(channel, test_pika_method, test_pika_props, str.encode(json.dumps(response)))
    # assert ' [x] request.id' in caplog.text()
    # assert 'We need to keep processing request: {}'.format(request_id) in caplog.text()
    assert request_list.get_request(request_id).current == 1
    assert request_list.get_request(request_id).pending == 3
    channel.basic_ack.assert_called_once()

# class TestHelperFuncs:
#     """Test struct for the helper functions"""
#     def test_generate_uuid_output(self):
#         """Test output generate_uuid"""
#         assert isinstance(bach.generate_uuid("test"), str)

