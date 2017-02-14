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
        Task("servers.validate_new_org_request", 1, 0),
        Task("servers.generate_team_repo_url", 2, 1)]
    rubrics.append(Rubric("new_org", new_org_tasks))
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

def validate_new_org_request(input_stuff, ch, value):
    """Task for validating the input"""
    keys = [["org_name",str],
            ["team_manager",str],
            ["spaces",list],
            ["app_team_github_team",int],
            ["github_url",str],
            ["app_team_manager_github_user",str]]
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
