"""
This is the orchestration module for Work Exchange. It's used to pick up and keep track of incoming
requests. It pulls in 'scores' as extra modules to call for different rubrics.
"""
# Import base modules
import os
import sys
import json
import uuid
import random
import logging
import hashlib

# Import dependancies
import pika
from scores import *

###########################
##### Global Variabls #####
###########################
REQUEST_LIST = {} # Current list of requests
REQUEST_DEFINITIONS = [] # List of requests that can be processed
SCORES = []
RMQ_SERVICE = str(os.getenv('RMQ_SERVICE', 'p-rabbitmq'))
SERVICE = json.loads(os.getenv('VCAP_SERVICES'))[RMQ_SERVICE][0]
AMQP_URL = SERVICE['credentials']['protocols']['amqp']['uri']
EXCHANGE = os.getenv('EXCHANGE_QUEUE_NAME', 'work_exchange')
LOGGER = logging.getLogger('bach')
LOGGER.addHandler(logging.StreamHandler(sys.stdout))
LOG_LEVEL = str(os.getenv('LOG_LEVEL', 'WARNING')).upper()
print("Setting log level to {0}".format(LOG_LEVEL))
if LOG_LEVEL == 'DEBUG':
    LOGGER.setLevel(10)
elif LOG_LEVEL == 'INFO':
    LOGGER.setLevel(20)
elif LOG_LEVEL == 'WARNING':
    LOGGER.setLevel(30)
elif LOG_LEVEL == 'ERROR':
    LOGGER.setLevel(40)
elif LOG_LEVEL == 'CRITICAL':
    LOGGER.setLevel(50)
#################################
##### Orchestration Classes #####
#################################

class R_List:
    """Defines the construct for a list of requests"""

class Request:
    """Defines the format of a request"""
    def __init__(self, r_id, type_name, pending=0, current=0, paused=False, retry_count=0):
        self.id = int(r_id)
        self.type = str(type_name)
        self.pending = int(pending)
        self.current = int(current)
        self.paused = paused
        self.retry_count = int(retry_count)
    def __str__(self):
        return "(ID:{0}, Type:{1}, Current_state:{2}, Pending_state:{3}, Retries:{4}, Paused:{5})" \
               "".format(self.id, self.type,
                         self.current, self.pending,
                         self.retry_count, self.paused)
    def __repr__(self):
        return "(ID:{0}, Type:{1}, Current_state:{2}, Pending_state:{3}, Retries:{4}, Paused:{5})" \
               "".format(self.id, self.type,
                         self.current, self.pending,
                         self.retry_count, self.paused)

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

def generate_uuid(input_string):
    """Generate a uuid based on the hash of the input and a random python uuid"""
    input_md = hashlib.md5(input_string.encode())
    return hashlib.sha256(input_md.digest()+uuid.uuid4().bytes)

def initialize_processable_requests():
    """Loads the rubrics from the scores"""
    LOGGER.info("Initializing requests!")
    files = os.listdir('scores')
    for file_name in files:
        name = file_name.split('.')
        if len(name) > 1 and 'py' in name[1] and 'init' not in name[0]:
            SCORES.append(name[0])
            func = eval(name[0]+'.load_rubrics')
            REQUEST_DEFINITIONS.extend(func())

def add_request_to_queue(request_id, request_type, body):
    """Add a request to the master queue"""
    new_req = Request(request_id, request_type)
    REQUEST_LIST[request_id] = {"def":new_req, "body":body}

def remove_request_from_queue(request):
    """Removes a request from the queue"""
    LOGGER.info('Deleting request, id: %r', request['def'].id)
    del REQUEST_LIST[request['def'].id]

def send_to_rabbit(channel, routing_key, value, body, reply_to=None):
    """Generic function for sending messages into rabbitmq"""
    if reply_to:
        channel.basic_publish(exchange=EXCHANGE,
                              routing_key=routing_key,
                              properties=pika.BasicProperties(reply_to=reply_to,
                                                              correlation_id=str(value),
                                                              delivery_mode=2),
                              body=json.dumps(body))
    else:
        channel.basic_publish(exchange=EXCHANGE,
                              routing_key=routing_key,
                              properties=pika.BasicProperties(correlation_id=str(value),
                                                              delivery_mode=2),
                              body=json.dumps(body))

def process_request(request, channel):
    """Finds the next task for the request"""
    found = False
    LOGGER.debug("Looking for "+request['def'].type)
    LOGGER.debug("There are %r request definitions!", len(REQUEST_DEFINITIONS))
    for req in REQUEST_DEFINITIONS:
        LOGGER.debug("Will %r work?", req.name)
        if req.name == request['def'].type:
            LOGGER.debug("Found the framework...checking tasks")
            task_list = req.tasks
            LOGGER.debug("The current state of the request is %r", request['def'].current)
            for task in task_list:
                if (task.value&request['def'].pending) == 0: # If it has not been called
                    if (task.req_state&request['def'].current) == task.req_state:
                        LOGGER.debug("Running task: "+str(task.target))
                        request['def'].pending += task.value
                        task_func = eval(str(task.target))
                        response = task_func(request)
                        send_to_rabbit(channel,
                                       response['routing_key'],
                                       task.value,
                                       response['body'],
                                       response['reply_to'])
                        found = True
                        LOGGER.debug("Task complete")
            if not found:
                LOGGER.debug("No tasks to preform...")
            if request['def'].pending == request['def'].current:
                remove_request_from_queue(request)
            return
    LOGGER.debug("Could not find definition for "+request['def'].type)

def router(channel, method, properties, body):
    """Callback function for incoming messages. Routes the message to the correct function."""
    LOGGER.info(" [x] %r:%r", method.routing_key, body)
    try:
        body = json.loads(str(body, "utf-8"))
    except:
        LOGGER.debug("Unable to process %r", body)
        send_to_rabbit(channel,
                       "logger.info",
                       -1,
                       json.dumps({'key':'Parsing failed', 'value':method.routing_key}))
    # ch.basic_ack(delivery_tag = method.delivery_tag)
    else:
        routing_key = method.routing_key.split('.', 1)
        new_key = routing_key[1]
        checker = new_key.split('.')
        LOGGER.debug("Can we process %r?", checker[0])
        try:
            if checker[0] == 'id':
                LOGGER.debug("We need to keep processing request: %r", checker[1])
                if checker[1] in REQUEST_LIST:
                    # print("Request %r came through!" % checker[1])
                    curr_request = REQUEST_LIST[checker[1]]
                    if len(checker) >= 3:
                        # This request got an error!
                        if 'error' in checker[2]:
                            LOGGER.info("Request %r got an error!!", checker[1])
                            remove_request_from_queue(curr_request)
                    else:
                        LOGGER.debug("We have the request, adding %r to the state",
                                     properties.correlation_id)
                        curr_request['def'].current += int(properties.correlation_id)
                        LOGGER.debug("Assigning %r to key %r", body['value'], body['key'])
                        curr_request['body'][body['key']] = body['value']
                        LOGGER.info("Sending request off to process...")
                        process_request(curr_request, channel)
                    # print(body)
            elif 'cf' in checker[0]:
                if 'new_org' in checker[1]:
                    request_id = generate_uuid(json.dumps(body))
                    LOGGER.info("Making new request. ID: %r", request_id)
                    # new_req = Request(request_id, "new_org")
                    # REQUEST_LIST[request_id] = {"def":new_req, "body":body}
                    add_request_to_queue(request_id, "new_org", body)
                    send_to_rabbit(channel, "logger.info", -1,
                                   json.dumps({'key':'new_request_id', 'value':request_id}))
                    LOGGER.info("Sending request off to process...")
                    process_request(REQUEST_LIST[request_id], channel)
                elif 'build_org_from_cf' in checker[1]:
                    request_id = generate_uuid(json.dumps(body))
                    LOGGER.info("Making new request. ID: %r", request_id)
                    # new_req = Request(request_id, "build_org_from_cf")
                    # REQUEST_LIST[request_id] = {"def":new_req, "body":body}
                    add_request_to_queue(request_id, "build_org_from_cf", body)
                    send_to_rabbit(channel, "logger.info", -1,
                                   json.dumps({'key':'new_request_id', 'value':request_id}))
                    LOGGER.info("Sending request off to process...")
                    process_request(REQUEST_LIST[request_id], channel)
                elif 'delete_org' in checker[1]:
                    request_id = generate_uuid(json.dumps(body))
                    LOGGER.info("Making new request. ID: %r", request_id)
                    # new_req = Request(request_id, "delete_org")
                    # REQUEST_LIST[request_id] = {"def":new_req, "body":body}
                    add_request_to_queue(request_id, "delete_org", body)
                    send_to_rabbit(channel, "logger.info", -1,
                                   json.dumps({'key':'new_request_id', 'value':request_id}))
                    LOGGER.info("Sending request off to process...")
                    process_request(REQUEST_LIST[request_id], channel)
            elif 'vm' in checker[0]:
                if 'new_vms' in checker[1]:
                    request_id = generate_uuid(json.dumps(body))
                    LOGGER.info("Making new request. ID: %r", request_id)
                    # new_req = Request(request_id, "new_vms")
                    # REQUEST_LIST[request_id] = {"def":new_req, "body":body}
                    add_request_to_queue(request_id, "new_vms", body)
                    send_to_rabbit(channel, "logger.info", -1,
                                   json.dumps({'key':'new_request_id', 'value':request_id}))
                    LOGGER.info("Sending request off to process...")
                    process_request(REQUEST_LIST[request_id], channel)

        except:
            LOGGER.info("Unexpected error: %r", sys.exc_info()[0])
            response = "{0}".format(sys.exc_info()[0]), 500
    finally:
        LOGGER.debug("Sending ack!")
        channel.basic_ack(delivery_tag=method.delivery_tag)

def main():
    """Setup the connection to the work exchange queue"""
    # Setup queue comm
    connection = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, type='topic')
    result = channel.queue_declare(queue='bach_queue',
                                   durable=True)
    queue_name = result.method.queue
    binding_key = "request.#"
    channel.queue_bind(exchange=EXCHANGE, queue=queue_name, routing_key=binding_key)
    initialize_processable_requests()
    LOGGER.info(' [*] Waiting for logs. To exit press CTRL+C')
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(router, queue=queue_name)
    channel.start_consuming()

if __name__ == "__main__":
    main()
