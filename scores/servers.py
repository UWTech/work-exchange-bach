"""This module loads in the tasks and rubrics that handle cf related things."""
############################### Building a score ###############################
# Start with the following code at the start of your file.
# import pika
# import json
# import os
# from os import environ as env
# EXCHANGE = os.getenv('EXCHANGE_QUEUE_NAME', 'work_exchange')
# class Rubric:
#     """Defines the format of a Rubric. A rubric tells Bach what to do for a request."""
#     def __init__(self, name, tasks):
#         self.name = name
#         self.tasks = tasks
#     def __str__(self):
#         return "(Name:{0}, Tasks:{1})".format(self.name, self.tasks)
#     def __repr__(self):
#         return "(Name:{0}, Tasks:{1})".format(self.name, self.tasks)
#
# class Task:
#     """
#     Defines the format of a Task.
#     A task defines what function is assigned to what bitvalue and what bitvalues it requires.
#     """
#     def __init__(self, target, value, req_state):
#         self.target = target
#         self.value = value
#         self.req_state = req_state
#     def __str__(self):
#         return "(Target:{0}, Value:{1}, Required_state:{2})".format(self.target,
#                                                                     self.value,
#                                                                     self.req_state)
#     def __repr__(self):
#         return "(Target:{0}, Value:{1}, Required_state:{2})".format(self.target,
#                                                                     self.value,
#                                                                     self.req_state)
#
# Define a function called load_rubrics; Bach calls this to import your rubric
#
# Every function should add 'assign_to_key':<key> to the body before sending to allow
# bach to add the response to the request. For example, if you added this code
# body['assign_to_key'] = 'github_url'
# upon recieving a response back, bach would add the ms repsonse to that key.
#
# Every task function should accept the follow params: the request, the channel, and
# the bit value

import os
import json
import pika

GITHUB_ADMIN_TEAM = int(os.getenv('GITHUB_ADMIN_TEAM_ID', '-1'))
EXCHANGE = os.getenv('EXCHANGE_QUEUE_NAME', 'work_exchange')
class Rubric:
    """Defines the format of a Rubric. A rubric tells Bach what to do for a request."""
    def __init__(self, name, tasks):
        self.name = name
        self.tasks = tasks
    def __str__(self):
        return "(Name:{0}, Tasks:{1})".format(self.name, self.tasks)
    def __repr__(self):
        return "(Name:{0}, Tasks:{1})".format(self.name, self.tasks)

class Task:
    """
    Defines the format of a Task.
    A task defines what function is assigned to what bitvalue and what bitvalues it requires.
    """
    def __init__(self, target, value, req_state):
        self.target = target
        self.value = value
        self.req_state = req_state
    def __str__(self):
        return "(Target:{0}, Value:{1}, Required_state:{2})".format(self.target,
                                                                    self.value,
                                                                    self.req_state)
    def __repr__(self):
        return "(Target:{0}, Value:{1}, Required_state:{2})".format(self.target,
                                                                    self.value,
                                                                    self.req_state)

# returns a list of rubrics
def load_rubrics():
    """Required function for loading in custom rubrics"""
    rubrics = []
    new_vm_tasks = [
        Task("servers.validate_new_vms_request", 1, 0),
        Task("servers.retrieve_vm_info", 2, 1),
        Task("servers.send_vm_info", 4, 2)]
    rubrics.append(Rubric("new_vms", new_vm_tasks))
    return rubrics

def validate(keys, ring):
    """Validates that all of the keys are on the ring"""
    print("Validating input...")
    for key in keys:
        try:
            i = 1
            matching = False
            while i < len(key) and not matching:
                if type(ring[key[0]]) == key[i]:
                    matching = True
                i += 1
            if not matching:
                return str(key[0])+" is not the correct type. Need: "+str(key[i-1]), 400
        except KeyError:
            return "Missing required key: "+key[0], 400
    return "All good", 200

def validate_new_vms_request(input_stuff, ch, value):
    """Task for validating the input"""
    keys = [["datacenter", str],
            ["env_type", str],
            ["quantity", int],
            ["vm_type", str],
            ["respond_to", str]]
    print("Sending off to validation")
    check = validate(keys, input_stuff['body'])
    print("Got back: {0}".format(check))
    reply_to = "request.id."+str(input_stuff['def'].id)
    if check[1] == 200:
        ch.basic_publish(exchange=EXCHANGE,
                         routing_key=reply_to,
                         properties=pika.BasicProperties(correlation_id=str(value),delivery_mode=2),
                         body='{{"key":"{0}","value":"{1}"}}'.format('validation',
                                                                     check[0]))
    else:
        ch.basic_publish(exchange=EXCHANGE,
                         routing_key=reply_to+'.error',
                         properties=pika.BasicProperties(correlation_id=str(value),delivery_mode=2),
                         body='{{"key":"{0}","value":"{1}"}}'.format('validation',
                                                                     check[0]))
    # return
def retrieve_vm_info(input_stuff, ch, value):
    """Sending out the new info"""
    body = {}
    body['assign_to_key'] = "login_information"
    body['datacenter'] = input_stuff['body']['datacenter']
    body['env_type'] = input_stuff['body']['env_type']
    body['quantity'] = input_stuff['body']['quantity']
    body['vm_type'] = input_stuff['body']['vm_type']
    body['respond_to'] = input_stuff['body']['respond_to']
    try:
        body['business_service'] = input_stuff['body']['business_service']
    except KeyError:
        body['business_service'] = input_stuff['body']['business_service'] = 'UNCD'
    try:
        body['vm_size'] = input_stuff['body']['vm_size']
    except KeyError:
        body['vm_size'] = input_stuff['body']['vm_size'] =  'S'
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="vms.retrieve_vms",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    # return "A github url!!!"+input_stuff
    return

def send_vm_info(input_stuff, ch, value):
    """Sending out the new info"""
    body = {}
    body['assign_to_key'] = "notification_sent"
    body['username'] = input_stuff['body']['respond_to']
    body['status'] = input_stuff['body']['login_information']
    reply_to = "request.id."+str(input_stuff['def'].id)
    if '@' in input_stuff['body']['respond_to']:
        ch.basic_publish(exchange=EXCHANGE,
                         routing_key="email.send_email",
                         properties=pika.BasicProperties(reply_to=reply_to,
                                                         correlation_id=str(value),
                                                         delivery_mode=2),
                         body=json.dumps(body))
    else:
        ch.basic_publish(exchange=EXCHANGE,
                         routing_key="hubot.send_dm",
                         properties=pika.BasicProperties(reply_to=reply_to,
                                                         correlation_id=str(value),
                                                         delivery_mode=2),
                         body=json.dumps(body))
    # return "A github url!!!"+input_stuff
    return
