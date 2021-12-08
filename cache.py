import collections
import sys
from main import *

class Cache:

    """
    Cache Design:
	I built this cache with the choice of least frequently used eviction policy. 
    When the cache reached its maximum capacity, the cache should invalidate the least 
    frequently used element (First In First Out as a tie breaker) before inserting a new element.

    Trade-offs:
    After internet research, I chose LFU eviction policy because data scientists tend to use a small 
    portion of workflows way more frequently comparing to the rest of the workflows. Given the above 
    assumption, LFU eviction policy will produce the highest hit rate which means faster execution due 
    to less disk reading

    One potential alternative is least recently used policy. Least recently used eviction policy also generate 
    excellent hit rate if there are workflows that are executed way more often than other workflows because 
    it organizes items in order of use and invalidate the least recently used element when the cache reaches 
    maximum. LRU eviction policy also has a simpler implementation and a better space complexity because LRU 
    is basically storing a list of workflows. Hence, I would argue LRU is also a valid option but LFU is a 
    straight-forward solution because LFU directly relies on frequency of usage. 
	"""

    def __init__(self, capacity: bytes):
        self.capacity = capacity                          # maximum capacities of the cache
        self.remain = capacity                            # remaining capacities of the cache
        self.least_freq = 1                               # current least frequency
        self.node_for_freq = collections.defaultdict(collections.OrderedDict)       # a dictionary of ordered dictionary {frequency: {workflow id: (workflow object, frequency)}} - easy to find the least frequently used workflow
        self.node_for_key = dict()                        # a dictionary {workflow id: (workflow object, frequency)} of cached workflows

    def get(self, key: str):
        """
		Given a workflow id, return a workflow object that is either read from disk or from cache
		Args:
			key: a unique workflow id, 
		Returns:
			a workflow object
		"""

        # checks whether the workflow is registered
        dirPath = os.path.join(os.getcwd(), "workflows")
        filePath = os.path.join(dirPath, "workflow_" + key + ".txt")
        if not os.path.isdir(dirPath) or not os.path.isfile(filePath):
            print("Oops!  The workflow is not registered.  Try again...")
            return

        # read from disk if the requested workflow is not in cache
        if key not in self.node_for_key:
            with open(filePath, 'r+b') as workflows_file:
                value = pickle.load(workflows_file)
            return value
        
        # read from cache
        value = self.node_for_key[key][0]
        return value

    def update(self, key: str, value):
        """
		used to set or insert the workflow object into cache if the key is not already present
		Args:
			key: a unique workflow id, 
			value: a workflow object identified with a unique workflow id
		Returns:
			None
		"""
        # if current workflow exist in the cache, increase its frequency by one
        if key in self.node_for_key:
            _, freq = self.node_for_key[key]
            self.node_for_freq[freq].pop(key)
            if len(self.node_for_freq[self.least_freq]) == 0:
                self.least_freq += 1
            self.node_for_freq[freq+1][key] = (value, freq+1)
            self.node_for_key[key] = (value, freq+1)

        # if current workflow does not exist in the cache, remove least frequently used target if necessary and put it into the cache
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
