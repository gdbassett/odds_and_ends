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
 Executes a drunken walk of the graph with potential to warp,
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
q = """ START n = node({0})
        MATCH n-[]->m
        RETURN DISTINCT n, m;
    """
# a node ID to start with
seed = 30185


## EXECUTION
def warp():
    """ NoneType -> int

        Takes nothing.  Returns a random node ID matching the query
        criteria.

    """
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
    """ int -> list ints

        Takes a node ID.  returns a list of children node IDs.

    """

    # find children 
    query = q.format(nID)
    neoNodes, metadata = cypher.execute(G, query)
    # randomly choose 1
    r = random.randrange(0,len(neoNodes))
    return neoNodes[r][1]._id


def printPeers(nID):
    """ int -> NoneType

        Takes a node ID, prints the node's peers.  Returns nothing.

        This is primarily used as a test payload.

    """
    query = q.format(nID)
    neoNodes, metadata = cypher.execute(G, query)
    print "----"
    for n in neoNodes:
        print n[1]._id
    time.sleep(2)


def main(seed):
    nID = seed
    
    # Crawl Indefinitely
    while 1:
        r = random.randrange(0, 101)
        # if there's nothing in the queue, warp
        if not nID:
            nID = warp()
        # Else, try a random warp
        elif r < R:
            nID = warp()

        # If still nothing in the queue, quit
        if not nID:
            break

        # do something
        printPeers(nID)

        # choose a child of n to walk to
        nID = getNext(nID)

        # pause
        time.sleep(1)
        

if __name__ == "__main__":
    main(seed)    
