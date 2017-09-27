"""Bach (Orchestration Engine) Microservice
This is the orchestration module for Work Exchange. It's used to pick up and keep track of incoming
requests. It pulls in 'scores' as extra modules to call for different rubrics.

Attributes:
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
import redis

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
    """Orchestration Engine
    Attributes:
        list (dict) The default in-memory store for requests if Redis is unavailable
        tracking_list = {} # The default in-memory store for request tracking keys
        request_list = None
        definitions = {} # Store for task definitions
        scores = {} # Store for loaded scores
    Todo:
        Error check for invaild rubric formating
        Need to notify the requestor if the input fails validation for creation.
    """

    def __init__(self, init_empty=False, redis_env=None):
        """Loads a Request List object.

        Args:
            init_empty (bool, optional): Currently unused; would be set if we wanted to do an empty init
            redis_env (dict): The user, pass, and url for a redis db
        """
        self.list = {} # The default in-memory store for requests if Redis is unavailable
        self.tracking_list = {} # The default in-memory store for request tracking keys
        self.definitions = {} # Store for task definitions
        self.scores = {} # Store for loaded scores
        try: # Ateempt to load the redis env config
            self.request_list = redis.StrictRedis(**redis_env)
            self.request_list.info()
        except redis.ConnectionError:
            LOGGER.critical("Unable to connect to redis")
            self.request_list = None
        except TypeError:
            LOGGER.critical("No redis config available; no persistance")
            self.request_list = None
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
                            LOGGER.debug("Task: %r", task)
                            rubr_tasks.append(Task(**task))
                        rubrics[rubr['name']] = Rubric(rubr['name'], rubr_tasks, rubr['validate'])
                    self.scores[score_name] = rubrics
                    for key, value in file_input['definitions'].items():
                        if key not in self.definitions:
                            self.definitions[key] = value

    def validate(self, keys, ring):
        """Validates that all of the keys are on the ring.

        Args:
            keys (list<list>): A list of keys that need to be present, and their types
            ring (dict): The body to validate
        Returns:
            tuple: a str, int tuple with the results of the validation
        Notes:
            Tuple return codes:
            - 400, incorrect input; first part of tuple contains error message
            - 200, correct input
        """
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
        """Attempt to retrieve request.

        Args:
            request_id (str): UUID of the request
        Returns:
            str | int: If a valid value is found, the UUID is returned as a str; otherwise a 404 int is returned.
        """
        if self.request_list:
            try:
                redis_string = self.request_list.get(":".join(["REQUEST_LIST", request_id]))
                return Request(**json.loads(str(redis_string, "utf-8")))
            except:
                return 404
        else:
            try:
                return self.list[request_id]
            except KeyError:
                return 404

    def get_request_by_tracking_key(self, tracking_key):
        """Attempt to retrieve request

        Args:
            tracking_key (str): Tracking key of the request
        Returns:
            str | int: If a valid value is found, the UUID is returned as a str; otherwise a 404 int is returned."""
        if self.request_list:
            try:
                redis_string = self.request_list.get(":".join(["TRACKING", tracking_key]))
                request_string = self.request_list.get(":".join(["REQUEST_LIST",
                                                       str(redis_string, "utf-8")]))
                return Request(**json.loads(str(request_string, "utf-8")))
            except:
                return 404
        else:
            try:
                request_id = self.tracking_list[tracking_key]
                return self.list[request_id]
            except KeyError:
                return 404

    def update_request(self, request_id, request):
        """Update the request.

        Args:
            request_id (str): UUID of the request
            request (bach.Request): the full request object to be updated.
        Returns:
            int: Status code from Redis Update command; otherwise 404 is returned.
        """
        if self.request_list:
            try:
                request_s = json.dumps(request.__dict__)
                return self.request_list.set(":".join(["REQUEST_LIST", request_id]), request_s)
            except:
                return 404
        else:
            try:
                self.list[request_id] = request
            except KeyError:
                return 404

    def check_in_list(self, request_id):
        """Check if request is in list.

        Args:
            request_id (str): UUID of the request
        Returns:
            bool: True if request is found in redis or internal list; False otherwise.
        """
        if self.request_list:
            value = self.request_list.exists(":".join(["REQUEST_LIST", request_id]))
            if value == 1:
                return True
            return False
        return request_id in self.list

    def check_tracking_in_list(self, tracking_key):
        """Check if request is in list using tracking key.

        Args:
            tracking_key (str): Tracking key of the request
        Returns:
            bool: True if request is found in redis or internal list; False otherwise.
        """
        if self.request_list:
            value = self.request_list.exists(":".join(["TRACKING", tracking_key]))
            if value == 1:
                return True
            return False
        return tracking_key in self.tracking_list

    def add_tracking_key(self, tracking_key, request_id):
        """Add request's tracking key.

        Args:
            tracking_key (str): tracking key to pair with the request id
            request_id (str): request id the tracking key will be paired with
        """
        if self.request_list:
            try:
                return self.request_list.set(":".join(["TRACKING", tracking_key]), request_id)
            except:
                return 404
        else:
            try:
                self.tracking_list[tracking_key] = request_id
            except KeyError:
                return 404

    def add_request_to_queue(self, score, rubric, body):
        """Add a request to the master queue.

        Args:
            score (str): name of the score the request will be using
            rubric (bach.Rubric): rubric the request will be using
            body (dict): user input of for the request to be validated
        Returns:
            str | bool: Returns the request uuid if input passes validation, returns False otherwise.
        """
        try:
            request_id = generate_uuid(json.dumps(body))
            if self.validate(self.scores[score][rubric].validation_keys, body)[1] == 200:
                if 'metadata' not in body:
                    body['metadata'] = {
                        'tracking_key': request_id
                    }
                else:
                    if 'tracking_key' not in body['metadata']:
                        body['metadata']['tracking_key'] = request_id
                self.update_request(request_id, Request(request_id, score, rubric, body))
                self.add_tracking_key(body['metadata']['tracking_key'], request_id)
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
        """Removes a request from the queue
        Args:
            request_id (str): UUID of the request to remove.
        Returns:
            bool: True if sucessful, otherwise False.
        """
        LOGGER.info('Attempting to delete request, id: %r', request_id)
        if self.request_list:
            try:
                # Let's keep the request around a bit before deleting it
                tracking_key = self.get_request(request_id).body['metadata']['tracking_key']
                self.request_list.expire(":".join(["TRACKING", tracking_key]), 604800)
                return bool(self.request_list.expire(":".join(["REQUEST_LIST", request_id]), 604800))
            except:
                LOGGER.warning('Request %r not found!', request_id)
                return False
        else:
            try:
                del self.list[request_id]
                return True
            except KeyError:
                LOGGER.warning('Request %r not found!', request_id)
                return False

    def process_request(self, request_id, channel):
        """Finds and sends out the next task for the request.

        This function is one of the two core functions of Bach. It sifts through the tasks within
        the rubric to determine the state of the request and which tasks need to be sent off.

        Args:
            request_id (str): the UUID of the request
            channel (pika.channel.Channel): The RMQ channel to send tasks out on
        """
        found = False
        request = self.get_request(request_id)
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
                                LOGGER.debug("Running task: "+str(task.name))
                                request.pending += task.value
                                task_def = self.definitions[task.name]
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
                                LOGGER.debug("Task complete")
                                self.update_request(request.id, request)
                    LOGGER.info("State: Pending - %r; Current - %r", request.pending, request.current)
                    if not found:
                        LOGGER.debug("No tasks to preform...")
                        if request.pending == request.current:
                            self.remove_request_from_queue(request.id)
                    return
            LOGGER.debug("Could not find definition for "+request.rubric)
        LOGGER.debug("Could not find score for "+request.rubric)

    def router(self, channel, method, properties, body):
        """Callback function for incoming messages. Routes the message to the correct function.

        Args:
            chan (pika.channel.Channel): The string used for hashing
            method (pika.spec.Basic.Deliver): Information about the received message
            props (pika.spec.BasicProperties): The properties of the received message
            body (dict): The body included in the received message
        """
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
                        LOGGER.debug("Request %r came through!", checker[1])
                        curr_request = self.get_request(checker[1])
                        if len(checker) >= 3:
                            # This request got an error!
                            if 'error' in checker[2]:
                                LOGGER.info("Request %r got an error!!", checker[1])
                                # self.remove_request_from_queue(curr_request.id)
                                if curr_request.failed_tasks == -1:
                                    curr_request.failed_tasks = int(properties.correlation_id)
                                elif curr_request.failed_tasks > 0:
                                    curr_request.failed_tasks += int(properties.correlation_id)
                                curr_request.paused = True
                            self.update_request(curr_request.id, curr_request)
                        else:
                            LOGGER.debug("We have the request, adding %r to the state",
                                         properties.correlation_id)
                            curr_request.current += int(properties.correlation_id)
                            LOGGER.debug("Assigning %r to key %r", body['value'], body['key'])
                            curr_request.body[body['key']] = body['value']
                            self.update_request(curr_request.id, curr_request)
                            LOGGER.info("Sending request off to process...")
                            self.process_request(curr_request.id, channel)
                        # print(body)
                    else:
                        LOGGER.warning("Unable to find request %r", checker[1])
                elif checker[0] == 'process':
                    if checker[1] in self.scores:
                        if checker[2] in self.scores[checker[1]]:
                            if 'assign_to_key' in body:
                                # Check if a custom key is desired
                                assign_to_key = body['assign_to_key']
                                del body['assign_to_key']
                            else:
                                assign_to_key = '{}-{}-tracking_key'.format(checker[1],checker[2])
                            request_id = self.add_request_to_queue(checker[1], checker[2], body)
                            LOGGER.info("Generating %r request. ID: %r", checker[2], request_id)
                            if properties.reply_to:
                                LOGGER.info("Replying back")
                                send_to_rabbit(channel,
                                               properties.reply_to,
                                               properties.correlation_id,
                                               json.dumps({'key': assign_to_key, 'value': request_id}))
                            else:
                                LOGGER.info("Making a log for new request")
                                send_to_rabbit(channel, "logger.info", -1,
                                            json.dumps({'key':'new_request_id', 'value':request_id}))
                            LOGGER.info("Sending request off to process...")
                            self.process_request(request_id, channel)
                        else:
                            LOGGER.info("Rubric %r not found in Score %r", checker[2], checker[1])
                    else:
                        LOGGER.info("Score %r not found", checker[1])
                elif checker[0] == 'query':
                    LOGGER.debug("Need to make a query!")
                    if properties.reply_to:
                        LOGGER.info("Replying back")
                        LOGGER.debug("Tracking key: %r", checker[1])
                        request = self.get_request_by_tracking_key(checker[1])
                        LOGGER.debug("Request is %r", request)
                        if request == 404:
                            send_to_rabbit(channel, properties.reply_to, properties.correlation_id, str(request))
                        else:
                            send_to_rabbit(channel, properties.reply_to, properties.correlation_id, json.dumps(request.__dict__))
                    else:
                        LOGGER.warning("Someone is trying to query but didn't tell me how to talk")
                elif checker[0] == 'restart':
                    LOGGER.debug("Need to restart a request")
                    LOGGER.debug("Tracking key: %r", checker[1])
                    request = self.get_request_by_tracking_key(checker[1])
                    LOGGER.debug("Request is %r", request)
                    if request == 404:
                        LOGGER.warning("Tried to retry a request that doesn't exist")
                    else:
                        LOGGER.debug("Retrying task")
                        if request.failed_tasks > -1:
                            request.pending -= request.failed_tasks
                            request.failed_tasks = -1
                        request.paused = False
                        request.retry_count += 1
                        self.update_request(request.id, request)
                        self.process_request(request.id, channel)
                else:
                    LOGGER.warning("Unrecognized command submitted: %r", routing_key)
            except:
                LOGGER.info("Unexpected error: %r", sys.exc_info()[0])
                response = "{0}".format(sys.exc_info()[0]), 500
        finally:
            LOGGER.debug("Sending ack!")
            channel.basic_ack(delivery_tag=method.delivery_tag)

class Request:
    """Defines the format of a request"""
    def __init__(self, id, score, rubric, body, pending=0, current=0, paused=False,
                 retry_count=0, failed_tasks=-1):
        self.id = id # Internel ID used for processing within the exchange
        self.score = str(score) # Name of the score being used to process the request
        self.rubric = str(rubric) # Name of the rubric being used to process the request
        self.pending = int(pending) # Used to track which tasks have been requested
        self.current = int(current) # Used to track which tasks have been completed successfully
        self.paused = paused # If a request is paused for processing
        self.retry_count = int(retry_count) # How many times the request has been processed
        self.body = body # Request details; contains user input and task outputs in json/dict format
        self.failed_tasks = failed_tasks # Value(s) of any failed tasks; -1 if none
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
    def __init__(self, name, value, req_state, mandatory):
        self.name = name # Name of the task; correlates with the task definition name
        self.value = value # The bit to flip for
        self.req_state = req_state # Which bits need to be present before processing
        self.mandatory = mandatory # If a task is mandatory for the request to process.
    def __str__(self):
        return "(Target:{0}, Value:{1}, Required_state:{2}, Mandatory:{3})".format(self.name,
                                                                                   self.value,
                                                                                   self.req_state,
                                                                                   self.mandatory)
    def __repr__(self):
        return "(Target:{0}, Value:{1}, Required_state:{2}, Mandatory:{3})".format(self.name,
                                                                                   self.value,
                                                                                   self.req_state,
                                                                                   self.mandatory)

###########################
##### Global Variabls #####
###########################
RMQ_SERVICE = ""
SERVICE = {}
AMQP_URL = ""
EXCHANGE = ""

def generate_uuid(input_string):
    """Generate a uuid based on the hash of the input and a random python uuid
    
    Args:
        input_string (str): The string used for hashing
    Returns:
        str: A string representation of the input's hash mixed with a rotating UUID
    """
    input_md = hashlib.md5(input_string.encode())
    return hashlib.sha256(input_md.digest()+uuid.uuid4().bytes).hexdigest()

def send_to_rabbit(channel, routing_key, value, body, reply_to=None):
    """Generic function for sending messages into rabbitmq

    Args:
        chan (pika.channel.Channel): The string used for hashing
        method (pika.spec.Basic.Deliver): Information about the received message
        props (pika.spec.BasicProperties): The properties of the received message
        body (dict): The body included in the received message
        reply_to (str, optional): What to respond to, if needed
    """
    LOGGER.debug(json.dumps(body))
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
    if 'VCAP_SERVICES' in os.environ:
        services = json.loads(os.getenv('VCAP_SERVICES'))
        try:
            redis_env = services[os.getenv('REDIS_SERVICE', 'p-redis')][0]['credentials']
            redis_env['port'] = int(redis_env['port'])
        except KeyError:
            LOGGER.warning("NO REDIS SERVICE AVAILABLE! Persistance will be unavailable.")
            redis_env = None
    else:
        redis_env = {'host':'localhost', 'port':6379, 'password':''}

    request_list = Bach(redis_env=redis_env)
    channel.basic_consume(request_list.router, queue=queue_name)
    channel.start_consuming()

if __name__ == "__main__":
    RMQ_SERVICE = str(os.getenv('RMQ_SERVICE', 'p-rabbitmq'))
    SERVICE = json.loads(os.getenv('VCAP_SERVICES'))[RMQ_SERVICE][0]
    AMQP_URL = SERVICE['credentials']['protocols']['amqp']['uri']
    EXCHANGE = os.getenv('EXCHANGE_QUEUE_NAME', 'work_exchange')
    main()
