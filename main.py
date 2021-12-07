
from collections.abc import Mapping
from typing import Any, Callable, Tuple

import pickle
from os.path import exists
import collections
import sys

class Result:
	def __init__(self, operatorId: str, payload: Any):
		self.operatorId = operatorId
		self.payload = payload

class Operator:
	def __init__(self, operatorId: str, code: Callable):
    # `__init__` takes in an operator ID and the `code` of this operator.
    # `code` is of type `Callable` that takes in a list of the operator's
    # inputs and returns the operator's output payload (of type `Any`).
    # During testing, you may use any `code` of your choice.
		self.operatorId = operatorId
		self.code = code
		

		# set over list

		self.upstream = set()
		self.downstream = set()

	def execute(self, inputs: list[Result]) -> Result:
		output_payload = self.code(inputs)
		return Result(self.operatorId, output_payload)

class Workflow:
	def __init__(self, workflowId: str, dag: Mapping[str, Operator]):
		self.workflowId = workflowId
		self.dag = dag



def register_workflow(
	workflowId: str, 
	operators: Mapping[str, Callable],
	dependencies: list[Tuple[str, str]]):
	"""
	Register a workflow given the workflow id, its operators, and dependencies.
	Args:
	    workflowId: the ID of the workflow.
		operators: a map whose keys are the IDs of the operators of this workflow,
								and whose values are the code of the operators.
		dependencies: a list of tuple, each of which contains the operators'
				dependencies. For example, Tuple(a, b) means operator (with ID)
				b depends on a.
	Returns:
	    this API does not return anything.
	"""
	# TODO: implement this API. Feel free to use helper classes from above and/or create new ones if necessary. 
	# validate_input_type(workflowId, operators, dependencies)
	# validate_workflow(dependencies)
	# save_workflow()

	registered_workflows = {}
	operators = {k:Operator(k, v) for k, v in operators.items()}

	for a, b in dependencies:
		operators[a].downstream.add(b)
		operators[b].upstream.add(a)

	# testing
	for op in operators.values():
		print(op.operatorId)
		print(op.upstream)
		print(op.downstream)


	if exists('workflow_object.txt'):
		print("running one")
		with open('workflow_object.txt', 'r+b') as workflows_file:
			registered_workflows = pickle.load(workflows_file)
		registered_workflows[workflowId] = Workflow(workflowId, operators)
		print(registered_workflows)
		with open('workflow_object.txt', 'w+b') as workflows_file:
			pickle.dump(registered_workflows, workflows_file)
	else:
		print("running two")
		registered_workflows[workflowId] = Workflow(workflowId, operators)
		with open('workflow_object.txt', 'w+b') as workflows_file:
			pickle.dump(registered_workflows, workflows_file)


# def validate_input_type(
# 	workflowId: str, 
# 	operators: Mapping[str, Callable],
# 	dependencies: list[Tuple[str, str]]):
# 	"""
# 	Compares the given input type with assumed data type, raise a error when there is a dismatch
# 	Args:
# 		workflowId: str, 
# 		operators: Mapping[str, Callable],
# 		dependencies: list[Tuple[str, str]]):
# 	Returns:
# 	    None
# 	"""

#     pass

# def validate_workflow(dependencies: list[Tuple[str, str]]):
# 	"""
# 	Uses DFS to detect whether the dependencies forms a DAG or not
# 	Args:
# 		workflowId: str, 
# 		operators: Mapping[str, Callable],
# 		dependencies: list[Tuple[str, str]]):
# 	Returns:
# 	    None
# 	"""
#     pass


# def save_workflow():
# 	"""
# 	serialize the current workflow and save it to local disk for persistance
# 	Args:
# 		workflowId: str, 
# 		operators: Mapping[str, Callable],
# 		dependencies: list[Tuple[str, str]]):
# 	Returns:
# 	    True / False otheriwse raise a error
# 	"""

#     pass

def execute_workflow(workflowId: str) -> list[Result]:
	"""
	Execute a workflow given the workflow id.
	Args:
	    workflowId: the ID of the workflow to be executed.
	Returns:
	    a list of `result` of operators who are not dependencies of 
			any other operators (also called the "sink" operators).
	"""
	# TODO: implement this API. You may reuse any implementation in
  	# question 1 and create/use helper classes if necessary.
	with open('workflow_object.txt', 'r+b') as workflows_file:
		registered_workflows = pickle.load(workflows_file)
		print(registered_workflows)
	dag = registered_workflows[workflowId].dag

	todo =[]
	executions = {}
	sinkID = set()
	results = []

	# extract source operators
	for op in dag.values():
		if not op.upstream:
			todo.append(op)
		if not op.downstream:
			sinkID.add(op.operatorId)
			print(op.operatorId)
	# execute source operators
	while todo:
		op = todo.pop()
		inputs = [executions[id] for id in op.upstream]
		executions[op.operatorId] = op.execute(inputs)
		if op.operatorId in sinkID:
			results.append(executions[op.operatorId])
		for downstream in op.downstream:
			# make a copy for upstream and downstream
			dag[downstream].upstream.remove(op.operatorId)
			if not dag[downstream].upstream:
				todo.append(dag[downstream])
	return results



def funcOne(a):
	return 1

def funcTwo(b):
	return 2

def funcThree(c):
	return 3

def funcFour(c):
	return 4

def funcFive(c):
	return 5

workflowIdOne = "123456"
workflowIdTwo = "234567"
workflowIdThree = "345678"
operatorDictOne = {"one":funcOne, "two":funcTwo, "three":funcThree}
operatorDictTwo = {"one":funcOne, "two":funcTwo, "three":funcThree, "four":funcFour, "five":funcFive}
dependencyLstOne = [("one", "two"), ("two", "three"), ("one", "three")]
dependencyLstTwo = [("one", "two"), ("two", "three"), ("one", "three"), ("four", "one"), ("three", "five")]
register_workflow(workflowIdOne, operatorDictOne, dependencyLstOne)
register_workflow(workflowIdTwo, operatorDictTwo, dependencyLstTwo)
register_workflow(workflowIdThree, operatorDictTwo, dependencyLstTwo)

print(execute_workflow(workflowIdOne))
print(execute_workflow(workflowIdTwo))
print(execute_workflow(workflowIdThree))




# Skeleton for the cache class.
class Cache:

	def __init__(self, capacity: bytes):
		self.capacity = capacity
		self.remain = capacity
		self.least_freq = 1
		self.node_for_freq = collections.defaultdict(collections.OrderedDict)
		self.node_for_key = dict()

	def update(self, key: str, value: Workflow):
		"""
		update the frequency count of the workflow
		Args:
			key: a unique workflow id, 
			value: a workflow object identified with a unique workflow id
		Returns:
			None
		"""
		_, freq = self.node_for_key[key]
		self.node_for_freq[freq].pop(key)
		if len(self.node_for_freq[self.least_freq]) == 0:
			self.least_freq += 1
		self.node_for_freq[freq+1][key] = (value, freq+1)
		self.node_for_key[key] = (value, freq+1)
	def get(self, key: str):
		"""
		given a workflow id, return a workflow object that is either read from disk or from cache
		Args:
			key: a unique workflow id, 
		Returns:
			None
		"""
		if key not in self.node_for_key:
			with open('workflow_object.txt', 'r+b') as workflows_file:
				value = pickle.load(workflows_file)
			return value
		value = self.node_for_key[key][0]
		self.update(key, value)
		return value
	def put(self, key: str, value: Workflow):
		"""
		used to set or insert the workflow object into cache if the key is not already present
		Args:
			key: a unique workflow id, 
			value: a workflow object identified with a unique workflow id
		Returns:
			None
		"""
		if key in self.node_for_key:
			self.update(key, value)
		else:
			objSize = sys.getsizeof(value)
			self.node_for_key[key] = (value,1)
			self.node_for_freq[1][key] = (value,1)
			while self.remain < objSize:
				removed = self.node_for_freq[self.least_freq].popitem(last=False)
				self.remain += sys.getsizeof(removed[1])
				self.node_for_key.pop(removed[0])
			else:
				self.remain -= objSize
				self.least_freq = 1
