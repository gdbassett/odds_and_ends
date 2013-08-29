'''
 AUTHOR: Gabriel Bassett
 DATE: 08-27-2013
 DEPENDANCIES: py2neo
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
seed = 30185



## EXECUTION
def warp():
    query = q.format("*")
    neoNodes, metadata = cypher.execute(G, query)
    node = neoNodes[0][0]
    return node._id


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


def printStatus(queue, completed):
    print "Length of Queue: {0}".format(len(queue))
    print "Completed {0}\n\r{1}".format(len(completed), completed)
    time.sleep(2)


def main(seed):
    # Initialize Queue and completed
    queue = [seed]
    completed = []
    
    # Crawl Indefinitely
    while 1:
        r = random.randrange(0, 101)
        # if there's nothing in the queue, warp
        if len(queue) == 0:
            completed = []
            queue.insert(0,warp())
        # Else, try a random warp
        elif r < R:
            queue.insert(0, warp())

        # If still nothing in the queue, quit
        if len(queue) == 0:
            break

        # Pop the current Node and skip execution if
        #  it's been visited
        nID = queue.pop(0)
        if nID in completed:
            continue
        else:
            completed.append(nID)

        # do something
#        printQueue(queue, 5)
        printStatus(queue, completed)

        # choose a child of n to walk to
        queue = queue + getNext(nID) 
        

if __name__ == "__main__":
    main(seed)    
