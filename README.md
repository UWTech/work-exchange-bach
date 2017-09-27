# Bach (Orchestration Engine)
Bach is named after Johann Sebastian Bach, the famous composer and orchestrator. Its sole purpose is to manage incoming user requests and execute the needed tasks to fulfill that request in the appropriate order. The microservice is composed of the following parts:
- [Scores](#Score)
- [Bach Class](#Bach (class))

## Classes
### Task / Task Definition
The class named Task is what a rubric uses to correlate a task definition to a rubrics operation order. The value and req_state vars are used in Bach's bitwise logic used for processing requests. The value is what bit should be flipped when the task is complete, and the req_state is what bits need to be flipped before starting the task.

The task definition is located in the score file. It defines the name of the task, the key the task output should be assigned to, and what inputs are needed for the task. It also handles mapping the user inputs into the correct microservice inputs.
### Rubric
A 'order of operations' like class, it contains information about which tasks need to be executed and in which order. This is done via bitwise logic; each task is assigned a bit to flip and has a required set of bits needed before execution.
### Bach (class)
Where the magic lives. This object contains a master list of scores and rubrics, and it is responsible for sending tasks out and updating requests when tasks come in.

Internally, it keeps track of three things: scores, requests, and task definitions. Both scores and task definitions are read from configuration and stored in memory. Requests are stored in a Redis DB; when a task reports in, the request is loaded from Redis, updated, then pushed back to Redis.
### Request
This is how a user request is defined. Bach uses this to track state, and adds any new info to this object. This is pushed and stored in Redis. A tracking key is used to reference the request; it is either given by the user, or the internal request ID is used.
## Inferred Constructs
### Score
This is a single file containing similar rubrics (ie. creating and deleting objects) and the task definitions for those rubrics.
