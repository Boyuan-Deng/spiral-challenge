from main import *
from cache import Cache

lfuCache = Cache(96)

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
register_workflow("456789", operatorDictTwo, dependencyLstTwo)

execute_workflow(workflowIdOne, lfuCache)
execute_workflow(workflowIdOne, lfuCache)
execute_workflow(workflowIdTwo, lfuCache)
execute_workflow(workflowIdTwo, lfuCache)
execute_workflow(workflowIdThree, lfuCache)
execute_workflow(workflowIdOne, lfuCache)
