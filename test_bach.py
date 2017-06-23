"""Tests for the orchestration"""
# Import base modules
import os
import pytest
import unittest
import logging
import mock
# from unittest.mock import patch, create_autospec

# Import pip modules

# Import code
os.environ['LOG_LEVEL'] = 'DEBUG'
import bach

def output_to_stdout(channel, routing_key, value, body, reply_to=None):
    print(channel)
    print(routing_key)

def test_request_list():
    """Test adding and removing requests"""
    request_list = bach.Bach()
    request_id = request_list.add_request_to_queue("request_type", {"body":"body"})
    assert isinstance(request_id, str) is True
    assert request_list.check_in_list(request_id)
    request = request_list.get_request(request_id)
    assert request.type == "request_type"
    assert request.body == {"body":"body"}
    assert request_list.remove_request_from_queue(request_id) is True
    assert request_list.check_in_list(request_id) is False

@mock.patch('bach.send_to_rabbit', side_effect=output_to_stdout)
def test_request_processor_empty(mockStR, caplog):
    caplog.setLevel(logging.DEBUG)
    request_list = bach.Bach(init_empty=True)
    request_id = request_list.add_request_to_queue("new_org", {"body":"body"})
    request = request_list.get_request(request_id)
    # print(request)
    request_list.process_request(request, None)
    mockStR.assert_not_called()
    assert 'There are 0 request definitions!' in caplog.text()

@mock.patch('bach.send_to_rabbit', side_effect=output_to_stdout)
def test_request_processor(mockStR):
    request_list = bach.Bach()
    request_id = request_list.add_request_to_queue("new_org", {"body":"body"})
    request = request_list.get_request(request_id)
    print(request)
    bach.initialize_processable_requests()
    request_list.process_request(request, None)
    mockStR.assert_called_once()
    mockStR.assert_called_with(None,
                               "request.id.{0}.error".format(request_id),
                               1,
                               {"key":"validation", "value":'Missing required key: org_name'},
                               "request.id.{0}".format(request_id))

# @mock.patch('bach.send_to_rabbit', side_effect=output_to_stdout)
# @mock.patch('bach.process_request', side_effect=output_to_stdout)
# def test_request_router(mockPR, mockStR, caplog):

class TestHelperFuncs:
    """Test struct for the helper functions"""
    def test_generate_uuid_output(self):
        """Test output generate_uuid"""
        assert isinstance(bach.generate_uuid("test"), str)

