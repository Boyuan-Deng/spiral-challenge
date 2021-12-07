
import pickle
import copy
import os
from cache import Cache
from collections.abc import Mapping
from typing import Any, Callable, Tuple
from os.path import exists


class Result:
	def __init__(self, operatorId: str, payload: Any):
		self.operatorId = operatorId
		self.payload = payload

class Operator:
	def __init__(self, operatorId: str, code: Callable):
		"""
		`__init__` takes in an operator ID and the `code` of this operator.
		`code` is of type `Callable` that takes in a list of the operator's
		inputs and returns the operator's output payload (of type `Any`).
		During testing, you may use any `code` of your choice.
		"""
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
	registered_workflows = {}
	operators = {k:Operator(k, v) for k, v in operators.items()}

	for a, b in dependencies:
		operators[a].downstream.add(b)
		operators[b].upstream.add(a)

	fileName = "workflow_" + workflowId + ".txt"
	dirPath = os.path.join(os.getcwd(), "workflows")
	filePath = os.path.join(dirPath, fileName)
	if not os.path.isdir(dirPath):
		os.mkdir(dirPath)
	with open(filePath, 'w+b') as workflows_file:
		pickle.dump(Workflow(workflowId, operators), workflows_file)

def execute_workflow(workflowId: str, lfuCache) -> list[Result]:
	"""
	Execute a workflow given the workflow id.
	Args:
	    workflowId: the ID of the workflow to be executed.
	Returns:
	    a list of `result` of operators who are not dependencies of 
			any other operators (also called the "sink" operators).
	"""

	curWorkflow = lfuCache.get(workflowId)
	if curWorkflow == -1:
		print("Oops!  The workflow is not registered.  Try again...")
		return
	lfuCache.put(workflowId, curWorkflow)
	dag = copy.deepcopy(curWorkflow.dag)

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
			print(sinkID)
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





