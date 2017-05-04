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
    new_org_tasks = [
        Task("cf.validate_new_org_request", 1, 0),
        Task("cf.generate_team_repo_url", 2, 1),
        Task("cf.generate_org_repo_url", 4, 2),
        Task("cf.build_team_repo", 8, 6),
        Task("cf.build_org_repo", 16, 8),
        Task("cf.upload_org_pipeline", 32, 16),
        Task("cf.update_cf_mgmt_repo", 64, 32),
        Task("cf.upload_cf_mgmt_pipeline", 128, 64),
        Task("cf.email_members", 256, 128)]
    rubrics.append(Rubric("new_org", new_org_tasks))
    delete_org_tasks = [
        Task("cf.validate_delete_org_request", 1, 0),
        Task("cf.remove_org_from_pipeline_repo", 2, 1),
        Task("cf.remove_org_from_pipeline", 4, 2)]
        # Task("cf.remove_org_from_github", 8, 4),
        # Task("cf.remove_team_from_github", 16, 8)]
    rubrics.append(Rubric("delete_org", delete_org_tasks))
    org_from_cf_tasks = [
        Task("cf.validate_build_cf_org_request", 1, 0),
        Task("cf.generate_team_repo_url", 2, 1),
        Task("cf.generate_org_repo_url", 4, 2),
        Task("cf.build_from_cf_org", 8, 6),
        Task("cf.build_team_repo", 16, 8),
        Task("cf.upload_org_pipeline", 32, 16),
        Task("cf.update_cf_mgmt_repo", 64, 32),
        Task("cf.upload_cf_mgmt_pipeline", 128, 64)
    ]
    rubrics.append(Rubric("build_org_from_cf", org_from_cf_tasks))
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
            ["team_manager",str, list],
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

def generate_team_repo_url(input_stuff, ch, value):
    """Task for generating a team repo url in github"""
    body = {}
    body['assign_to_key'] = "team_ssl_remote_url"
    body['github_url'] = input_stuff['body']['github_url']
    name = input_stuff['body']['env_type']+'-'+input_stuff['body']['org_name']+'-team'
    body['name'] = name
    body['github_org'] = 'cloudfoundry-mgmt' #TODO: abstract this out into a variable
    teams = []
    team = {
        "id":input_stuff['body']['app_team_github_team'],
        "permission":"pull"
    }
    teams.append(team)
    if GITHUB_ADMIN_TEAM > 0:
        teams.append({"id":GITHUB_ADMIN_TEAM, "permission":"admin"})
    body['teams'] = teams
    body['collabs'] = [{
        'name':input_stuff['body']['app_team_manager_github_user'],
        'permission':'push'}]
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="github.generate_repo_url",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    # return "A github url!!!"+input_stuff
    return

def generate_org_repo_url(input_stuff, ch, value):
    """Task for generating an org repo url in github"""
    body = {}
    body['assign_to_key'] = "org_ssl_remote_url"
    body['github_url'] = input_stuff['body']['github_url']
    name = input_stuff['body']['env_type']+'-'+input_stuff['body']['org_name']+'-org'
    body['name'] = name
    body['github_org'] = 'cloudfoundry-mgmt' #TODO: abstract this out into a variable
    teams = []
    team = {
        "id":input_stuff['body']['app_team_github_team'],
        "permission":"pull"
    }
    teams.append(team)
    if GITHUB_ADMIN_TEAM > 0:
        teams.append({"id":GITHUB_ADMIN_TEAM, "permission":"admin"})
    body['teams'] = teams
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="github.generate_repo_url",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return

def build_team_repo(input_stuff, ch, value):
    """Task for building the file structure for the team repo"""
    body = {}
    body['assign_to_key'] = "team_repo_complete"
    body['github_url'] = input_stuff['body']['github_url']
    body['ssl_url'] = input_stuff['body']['team_ssl_remote_url']
    body['org_name'] = input_stuff['body']['org_name']
    body['team_manager'] = input_stuff['body']['team_manager']
    body['spaces'] = input_stuff['body']['spaces']
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="template_repos.cf_team_repo",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return

def build_org_repo(input_stuff, ch, value):
    """Task for building the file structure for the org repo"""
    body = {}
    body['assign_to_key'] = "org_repo_complete"
    body['github_url'] = input_stuff['body']['github_url']
    body['team_ssl_url'] = input_stuff['body']['team_ssl_remote_url']
    body['org_ssl_url'] = input_stuff['body']['org_ssl_remote_url']
    body['org_name'] = input_stuff['body']['org_name']
    body['spaces'] = input_stuff['body']['spaces']
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="template_repos.cf_org_repo",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return

def upload_org_pipeline(input_stuff, ch, value):
    """Task for uploading the org to concourse"""
    body = {}
    body['assign_to_key'] = "org_pipeline_upload"
    url = input_stuff['body']['github_url']+'/'+input_stuff['body']['org_ssl_remote_url'].split(':', 1)[1]
    body['clone_url'] = url
    name = input_stuff['body']['env_type']+'-'+input_stuff['body']['org_name']+'-pipeline'
    body['pipeline_name'] = name
    body['include_git'] = True
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="fly.set_pipeline",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return

def update_cf_mgmt_repo(input_stuff, ch, value):
    """Task for uploading cf-mgmt to concourse"""
    body = {}
    reply_to = "request.id."+str(input_stuff['def'].id)
    try:
        body['assign_to_key'] = "cf_mgmt_update"
        body['org_name'] = input_stuff['body']['org_name']
        body['org_ssl_url'] = input_stuff['body']['org_ssl_remote_url']
        body['github_url'] = input_stuff['body']['github_url']
        body['control_repo_url'] = input_stuff['body']['control_repo_url']
        body['compiler_repo_url'] = input_stuff['body']['compiler_repo_url']
    except KeyError as err:
        ch.basic_publish(exchange=EXCHANGE,
                         routing_key=reply_to+'.error',
                         properties=pika.BasicProperties(correlation_id=str(value),delivery_mode=2),
                         body=json.dumps({'key':'KeyError','value':err}))
    else:
        ch.basic_publish(exchange=EXCHANGE,
                         routing_key="template_repos.add_submodule",
                         properties=pika.BasicProperties(reply_to=reply_to,
                                                         correlation_id=str(value),
                                                         delivery_mode=2),
                         body=json.dumps(body))
    return

def upload_cf_mgmt_pipeline(input_stuff, ch, value):
    """Task for uploading cf-mgmt to concourse"""
    body = {}
    body['assign_to_key'] = "cf_mgmt_pipeline_upload"
    body['clone_url'] = input_stuff['body']['compiler_repo_url']
    name = input_stuff['body']['env_type']+'-compiler'
    body['pipeline_name'] = name
    body['include_git'] = True
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="fly.set_pipeline",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return

def validate_delete_org_request(input_stuff, ch, value):
    """Task for validating the input"""
    keys = [["org_name",str],
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

def remove_org_from_pipeline_repo(input_stuff, ch, value):
    """Task for deleting an org from the pipeline"""
    body = {}
    body['assign_to_key'] = "remove_org_from_pipeline_repo"
    body['control_repo_url'] = input_stuff['body']['control_repo_url']
    body['compiler_repo_url'] = input_stuff['body']['compiler_repo_url']
    body['submodule'] = input_stuff['body']['org_name']
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="template_repos.remove_submodule",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return

def remove_org_from_pipeline(input_stuff, ch, value):
    """Task for deleting an org from the pipeline"""
    body = {}
    body['assign_to_key'] = "remove_org_from_pipeline"
    body['clone_url'] = input_stuff['body']['compiler_repo_url']
    name = input_stuff['body']['env_type']+'-compiler'
    body['pipeline_name'] = name
    body['include_git'] = True
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="fly.set_pipeline",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return
def remove_org_from_cf(input_stuff, ch, value):
    """Task for deleting an org from the pipeline"""
    body = {}
    body['assign_to_key'] = "remove_org_from_cf"
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="cloudfoundry.remove_org",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return
def remove_org_from_github(input_stuff, ch, value):
    """Task for deleting an org from the pipeline"""
    body = {}
    body['assign_to_key'] = "remove_org_from_github"
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="github.remove_repo",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return
def remove_team_from_github(input_stuff, ch, value):
    """Task for deleting an org from the pipeline"""
    body = {}
    body['assign_to_key'] = "remove_team_from_github"
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="github.remove_repo",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return

def validate_build_cf_org_request(input_stuff, ch, value):
    """Task for validating the input"""
    keys = [["org_name",str],
            ["team_manager",str],
            ["app_team_github_team",int],
            ["github_url",str],
            ["app_team_manager_github_user",str],
            ["foundation",str]]
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
def build_from_cf_org(input_stuff, ch, value):
    """Task for building the file structure for the org repo"""
    body = {}
    body['assign_to_key'] = "spaces"
    body['github_url'] = input_stuff['body']['github_url']
    body['team_ssl_url'] = input_stuff['body']['team_ssl_remote_url']
    body['org_ssl_url'] = input_stuff['body']['org_ssl_remote_url']
    body['org_name'] = input_stuff['body']['org_name']
    body['foundation'] = input_stuff['body']['foundation']
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="template_repos.build_from_cf_org",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return

def email_members(input_stuff, ch, value):
    """Task for sending an email to notify users about a completed cf org"""
    body = {}
    body['assign_to_key'] = "cf_org_complete_email"
    body['org_name'] = input_stuff['body']['org_name']
    body['team_manager'] = input_stuff['body']['team_manager']
    body['spaces'] = input_stuff['body']['spaces']
    reply_to = "request.id."+str(input_stuff['def'].id)
    ch.basic_publish(exchange=EXCHANGE,
                     routing_key="email.send_cf_org_mail_template",
                     properties=pika.BasicProperties(reply_to=reply_to,
                                                     correlation_id=str(value),
                                                     delivery_mode=2),
                     body=json.dumps(body))
    return
