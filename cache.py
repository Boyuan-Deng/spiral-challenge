import collections
import sys
from main import *

class Cache:
    def __init__(self, capacity: bytes):
        self.capacity = capacity
        self.remain = capacity
        self.least_freq = 1
        self.node_for_freq = collections.defaultdict(collections.OrderedDict)
        self.node_for_key = dict()
    def update(self, key: str, value):
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
        fileName = "workflow_" + key + ".txt"
        dirPath = os.path.join(os.getcwd(), "workflows")
        filePath = os.path.join(dirPath, fileName)
        if not os.path.isdir(dirPath) or not os.path.isfile(filePath):
            return -1
        if key not in self.node_for_key:
            with open(filePath, 'r+b') as workflows_file:
                value = pickle.load(workflows_file)
            return value
        value = self.node_for_key[key][0]
        return value
    def put(self, key: str, value):
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
                self.remain += sys.getsizeof(removed[1][0])
                self.node_for_key.pop(removed[0])
            else:
                self.remain -= objSize
                self.least_freq = 1
