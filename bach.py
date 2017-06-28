"""
This is the orchestration module for Work Exchange. It's used to pick up and keep track of incoming
requests. It pulls in 'scores' as extra modules to call for different rubrics.
"""
# Import base modules
import os
import sys
from pydoc import locate
import json
import uuid
import logging
import hashlib

# Import dependancies
import pika
import yaml

##################################
######### Start logging! #########
##################################
LOGGER = logging.getLogger('bach')
LOGGER.addHandler(logging.StreamHandler(sys.stderr))
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

class Bach:
    """Defines the construct for a list of requests"""
    def __init__(self, init_empty=False):
        """Loads a Request List object"""
        self.list = {}
        self.definitions = {}
        self.scores = {}
        if not init_empty:
            LOGGER.info("Initializing requests!")
        files = os.listdir('scores')
        for file_name in files:
            name = file_name.split('.')
            if len(name) > 1 and 'yml' in name[1]:
                file_input = None
                with open('scores/{0}'.format(file_name)) as file:
                    file_input = yaml.load(file)
                score_name = file_input['score']
                rubrics = {}
                for rubr in file_input['rubrics']:
                    rubr_tasks = []
                    for task in rubr['task_list']:
                        rubr_tasks.append(Task(task['name'], task['value'], task['required']))
                    rubrics[rubr['name']] = Rubric(rubr['name'], rubr_tasks, rubr['validate'])
                self.scores[score_name] = rubrics
                for key, value in file_input['definitions'].items():
                    if key not in self.definitions:
                        self.definitions[key] = value
    def validate(self, keys, ring):
        """Validates that all of the keys are on the ring"""
        LOGGER.debug("Validating input...")
        for key in keys:
            try:
                i = 1
                matching = False
                while i < len(key) and not matching:
                    LOGGER.debug("%r", key[i])
                    if isinstance(ring[key[0]], locate(key[i])):
                        matching = True
                    i += 1
                if not matching:
                    return str(key[0])+" is not the correct type. Need: "+str(key[i-1]), 400
            except KeyError:
                return "Missing required key: "+key[0], 400
        return "All good", 200
    def get_request(self, request_id):
        """Attempt to retrieve request"""
        try:
            return self.list[request_id]
        except KeyError:
            return 404

    def check_in_list(self, request_id):
        """Check if request is in list"""
        return request_id in self.list

    def add_request_to_queue(self, score, rubric, body):
        """Add a request to the master queue"""
        try:
            request_id = generate_uuid(json.dumps(body))
            if self.validate(self.scores[score][rubric].validation_keys, body)[1] == 200:
                self.list[request_id] = Request(request_id, score, rubric, body)
                return request_id
            LOGGER.warning("Invalid request")
            return False
        except KeyError:
            LOGGER.ERROR("No valid score by name: %r", score)
            return False
        except:
            LOGGER.warning("Unexpected error: %r\n%r", sys.exc_info()[0], sys.exc_info()[1])
            return False

    def remove_request_from_queue(self, request_id):
        """Removes a request from the queue"""
        LOGGER.info('Attempting to delete request, id: %r', request_id)
        try:
            del self.list[request_id]
            return True
        except KeyError:
            LOGGER.warning('Request %r not found!', request_id)
            return False

    def process_request(self, request, channel):
        """Finds the next task for the request"""
        found = False
        LOGGER.debug("Looking for "+request.rubric)
        if request.score in self.scores:
            for req in self.scores[request.score]:
                LOGGER.debug("Will %r work?", req)
                if req == request.rubric:
                    LOGGER.debug("Found the framework...checking tasks")
                    task_list = self.scores[request.score][request.rubric].tasks
                    LOGGER.debug("The current state of the request is %r", request.current)
                    for task in task_list:
                        if (task.value&request.pending) == 0: # If it has not been called
                            if (task.req_state&request.current) == task.req_state:
                                LOGGER.debug("Running task: "+str(task.target))
                                request.pending += task.value
                                # task_func = eval(str(task.target))
                                # response = task_func(request)
                                task_def = self.definitions[task.target]
                                routing_key = task_def['routing_key']
                                packet = {}
                                packet['assign_to_key'] = task_def['assign_to_key']
                                for key, value_list in task_def['keys'].items():
                                    if value_list[0] not in ['primitive', 'direct', 'compound']:
                                        # TODO write error check for invalid format
                                        LOGGER.error("%r's value list is invalid format", key)
                                    if value_list[0] == 'primitive':
                                        packet[key] = value_list[1]
                                    elif value_list[0] == 'direct':
                                        packet[key] = request.body[value_list[1]]
                                    elif value_list[0] == 'compound':
                                        body_vars = []
                                        for value in value_list[2:len(value_list)]:
                                            body_vars.append(request.body[value])
                                        packet[key] = value_list[1].format(*body_vars)
                                LOGGER.debug("Sending %r", packet)
                                send_to_rabbit(channel,
                                               routing_key,
                                               task.value,
                                               packet,
                                               'request.id.{0}'.format(request.id))
                                found = True
                                LOGGER.debug("Task complete")
                    if not found:
                        LOGGER.debug("No tasks to preform...")
                    if request.pending == request.current:
                        self.remove_request_from_queue(request.id)
                    return
            LOGGER.debug("Could not find definition for "+request.rubric)
        LOGGER.debug("Could not find score for "+request.rubric)
    def router(self, channel, method, properties, body):
        """Callback function for incoming messages. Routes the message to the correct function."""
        LOGGER.info(" [x] %r:%r", method.routing_key, body)
        try:
            body = json.loads(str(body, "utf-8"))
        except:
            LOGGER.debug("Unable to process %r", body)
            send_to_rabbit(channel, "logger.info", -1,
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
                    if self.check_in_list(checker[1]):
                        # print("Request %r came through!" % checker[1])
                        curr_request = self.get_request(checker[1])
                        if len(checker) >= 3:
                            # This request got an error!
                            if 'error' in checker[2]:
                                LOGGER.info("Request %r got an error!!", checker[1])
                                self.remove_request_from_queue(curr_request.id)
                        else:
                            LOGGER.debug("We have the request, adding %r to the state",
                                         properties.correlation_id)
                            curr_request.current += int(properties.correlation_id)
                            LOGGER.debug("Assigning %r to key %r", body['value'], body['key'])
                            curr_request.body[body['key']] = body['value']
                            LOGGER.info("Sending request off to process...")
                            self.process_request(curr_request, channel)
                        # print(body)
                elif checker[0] in self.scores:
                    if checker[1] in self.scores[checker[0]]:
                        request_id = self.add_request_to_queue(checker[0], checker[1], body)
                        LOGGER.info("Generating %r request. ID: %r", checker[1], request_id)
                        send_to_rabbit(channel, "logger.info", -1,
                                       json.dumps({'key':'new_request_id', 'value':request_id}))
                        LOGGER.info("Sending request off to process...")
                        self.process_request(self.get_request(request_id), channel)
                    else:
                        LOGGER.info("Rubric %r not found in Score %r", checker[1], checker[0])
                else:
                    LOGGER.info("Score %r not found", checker[0])
            except:
                LOGGER.info("Unexpected error: %r", sys.exc_info()[0])
                response = "{0}".format(sys.exc_info()[0]), 500
        finally:
            LOGGER.debug("Sending ack!")
            channel.basic_ack(delivery_tag=method.delivery_tag)

class Request:
    """Defines the format of a request"""
    def __init__(self, r_id, score, rubric, body, pending=0, current=0, paused=False,
                 retry_count=0):
        self.id = r_id
        self.score = str(score)
        self.rubric = str(rubric)
        self.pending = int(pending)
        self.current = int(current)
        self.paused = paused
        self.retry_count = int(retry_count)
        self.body = body
    def __str__(self):
        return "{{ID:{0}, Type:{1}, Current_state:{2}, Pending_state:{3}, Retries:{4}, Paused:{5}" \
               ", Contents:{6}}}".format(self.id, self.rubric,
                                         self.current, self.pending,
                                         self.retry_count, self.paused, self.body)
    def __repr__(self):
        return "{{ID:{0}, Type:{1}, Current_state:{2}, Pending_state:{3}, Retries:{4}, Paused:{5}" \
               ", Contents:{6}}}".format(self.id, self.rubric,
                                         self.current, self.pending,
                                         self.retry_count, self.paused, self.body)

class Rubric:
    """Defines the format of a Rubric. A rubric tells Bach what to do for a request."""
    def __init__(self, name, tasks, validation_keys):
        self.name = name
        self.tasks = tasks
        self.validation_keys = validation_keys
    def __str__(self):
        return "(Name:{0}, Tasks:{1}, Validation Keys:{2})" \
               "".format(self.name, self.tasks, self.validation_keys)
    def __repr__(self):
        return "(Name:{0}, Tasks:{1}, Validation Keys:{2})" \
               "".format(self.name, self.tasks, self.validation_keys)

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

###########################
##### Global Variabls #####
###########################
RMQ_SERVICE = ""
SERVICE = {}
AMQP_URL = ""
EXCHANGE = ""

def generate_uuid(input_string):
    """Generate a uuid based on the hash of the input and a random python uuid"""
    input_md = hashlib.md5(input_string.encode())
    return hashlib.sha256(input_md.digest()+uuid.uuid4().bytes).hexdigest()

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
    # initialize_processable_requests()
    LOGGER.info(' [*] Waiting for logs. To exit press CTRL+C')
    channel.basic_qos(prefetch_count=1)
    REQUEST_LIST = Bach()
    channel.basic_consume(REQUEST_LIST.router, queue=queue_name)
    channel.start_consuming()

if __name__ == "__main__":
    RMQ_SERVICE = str(os.getenv('RMQ_SERVICE', 'p-rabbitmq'))
    SERVICE = json.loads(os.getenv('VCAP_SERVICES'))[RMQ_SERVICE][0]
    AMQP_URL = SERVICE['credentials']['protocols']['amqp']['uri']
    EXCHANGE = os.getenv('EXCHANGE_QUEUE_NAME', 'work_exchange')
    REQUEST_LIST = Bach()
    main()
