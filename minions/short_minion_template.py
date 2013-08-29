'''
 AUTHOR: Gabriel Bassett
 DATE: 08-27-2013
 DEPENDANCIES: py2neo, requests
 Copyright 2013 Gabriel Bassett

 LICENSE:
 This program is free software:  you can redistribute it and/or modify
 it under the terms of the GNU General Public License as published by
 the Free Software Foundation, either version 3 or the LIcense, or
 (at your option) any later version.

 This program is distributed in the hope that it will be useful,
 but WITHOUT ANY WARRANTY; without even the implied warranty of
 MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 GNU General Public LIcense for more dtails.

 You should have received a copy of the GNU General Public License
 along with theis program.  If not, see <http://www.gnu.org/licenses/>.

 DESCRIPTION:
 Executes a breath first search of the graph with potential to warp,
  running a set function at each node visited.

'''


from py2neo import neo4j, cypher
#import networkx as nx
import random
import time
import requests

## STATIC VARIABLES
NEODB = "http://192.168.56.101:7474/db/data"
R = 10 # Number from 0 to 100 indicating the % chance to warp


## SETUP
# Connect to database
G = neo4j.GraphDatabaseService(NEODB)
# The query that defines nodes to search from
q = """ START n = node({0})
        MATCH n-[]->m
        RETURN DISTINCT n, m;
    """
# a list of node IDs to start with
seed = [82719]
# The maximum depth to search.  Set to 0 to search indefinitely
maxDepth = 0



## EXECUTION
def warp():
    # Magic to get est # of nodes in graph from restful API
    resp = requests.get(NEODB[:-4] +
     "manage/server/jmx/domain/org.neo4j/instance%3Dkernel%230%2Cname%3DPrimitive%20count?_=1342719685294")
    rdict = resp.json()[0]
    for i in range(len(rdict['attributes'])):
            if rdict['attributes'][i]['name'] == "NumberOfNodeIdsInUse":
                    nodeCount = rdict['attributes'][i]['value']
    
    # try 10 times to get a random node
    for i in range(10):
        r = random.randrange(0,nodeCount)
        query = q.format("*")[:-6] + "        SKIP {0} LIMIT 1;\n".format(r)
        neoNodes, metadata = cypher.execute(G, query)
        if len(neoNodes) > 0:
            node = neoNodes[0][0]
            return node._id
    # if we can't find a random node, return node 0
    return 0

def getNext(nID):
    # find children 
    query = q.format(nID)
    neoNodes, metadata = cypher.execute(G, query)
    enqueue = []
    for n in neoNodes:
       enqueue.append(n[1]._id)
    return enqueue


def printQueue(queue, l):
    if l > len(queue):
        l = len(queue)
    print "-----{0}".format(len(queue))
    if l > 0:
        for i in range(0,l):
            print queue[i]
        time.sleep(2)


def printStatus(nid, queue, completed, d):
    print "Current Node: {1}, Length of Queue: {0}, Depth: {2}".format(
                                                   len(queue), nid, d)
    print "Completed {0}: {1}".format(len(completed), completed)
    time.sleep(2)


def main(seed):
    # Initialize Queue and completed
    queue = seed
    completed = []
    depth = 0
    
    # Crawl Indefinitely
    while depth <= maxDepth:
        
        r = random.randrange(0, 101)
        # if there's nothing in the queue, warp
        if maxDepth == 0 and len(queue) == 0:
            completed = []
            queue.insert = [warp()]
        # Else, try a random warp
        #  random warp only works if maxDepth isn't set
        elif maxDepth == 0 and r < R:
            queue = [warp()]

        # If still nothing in the queue, quit
        if len(queue) == 0:
            break

        # Pop the current Node
        nID = queue.pop(0)
        # if it's a depth divider, increment depth & continue
        if type(nID) == str:
            depth = len(nID)
            continue
        # if it's a node thats been visited, continue
        elif nID in completed:
            continue
        else:
            completed.append(nID)



        ##### DO SOMETHING RIGHT HERE #####
#        printQueue(queue, 5)
        printStatus(nID, queue, completed, depth)



        # add children and divider to the queue
        if maxDepth == 0:
            queue = queue + getNext(nID)
        else:
            queue = queue + ['-' * (depth + 1)] + getNext(nID)
        

if __name__ == "__main__":
    main(seed)    
