'''
 AUTHOR: Gabriel Bassett
 DATE: 08-27-2013
 DEPENDANCIES: py2neo, requests, networkx
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
 Executes a breath first search of the graph, copying all nodes and
  relationships into a networkx graph.  Once parsed, safe the networkx
  graph in gexf format, (readably by gephi).

'''


from py2neo import neo4j, cypher
import networkx as nx
import random
import time
import requests
import sys

## STATIC VARIABLES
# Warp assumes this link ends with "/data".
NEODB = "http://192.168.56.101:7474/db/data"
GEXF_FILE = "somefile.gexf"

## SETUP
# Connect to database
G = neo4j.GraphDatabaseService(NEODB)
# The query that defines nodes to search from
# warp assumes this query ends with the ";" on.  Do not change from ; on.
q = """ START n = node({0})
        MATCH n-[r]->m
        RETURN ID(n), r, type(r), m, ID(m);
    """
# Create the networkx graph.  use g = nx.Graph() for undirected graph
g = nx.DiGraph()
# Create a set of completed nodes so we don't repeat parsing
#  of a node
complete = set()
# A list of seed node IDs to start from.
# All nodes you want to export to should be reachable
#  from the seed nodes.
seed = [82719]
# The maximum depth to search.  Set to 0 to search indefinitely
maxDepth = 0
# Number from 0 to 100 indicating the % chance to warp
R = 0


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
            return node
    # if we can't find a random node, return node 0
    return 0

def getNext(nID):
    """ int -> list ints

        Takes a node ID.  returns a list of children node IDs.

    """

    # find children 
    query = q.format(nID)
    neoNodes, metadata = cypher.execute(G, query)
    enqueue = []
    for n in neoNodes:
       enqueue.append(n[1]._id)
    return enqueue


def printQueue(queue, l):
    """ list, int -> NoneType

        Takes a queue and an int L.  Prints the length of the queue and
        the first L items in the queue.  Returns nothing.

        This is primarily used as a test payload.

    """
    if l > len(queue):
        l = len(queue)
    print "-----{0}".format(len(queue))
    if l > 0:
        for i in range(0,l):
            print queue[i]
        time.sleep(2)


def printStatus(nid, queue, completed, d):
    """ int, list, list, int -> NoneType

        Takes the current node ID, queue, list of completed node IDs,
        and depth.  Prints the information.  Returns nothing.

        This is primarily used as a test payload.

    """
    print "Current Node: {1}, Length of Queue: {0}, Depth: {2}".format(
                                                   len(queue), nid, d)
    print "Completed {0}: {1}".format(len(completed), completed)
    time.sleep(2)


def addNodeAttributes(attr):
    """ dict -> dict

        Takes a dictionary of node attributes.  Adds any additional
        desired attributes.  Returns the dictionary.

    """
    # If you want to add additional attributes to nodes, do it here
    #  by adding key:value pairs to the attr dictionary
#    attr["N"] = "blue"
    
    return attr


def addRelationshipAttributes(attr):
    """ dict -> dict

        Takes a dictionary of relationship attributes.  Adds any
        additional desired attributes.  Returns the dictionary.

    """
    # If you want to add additional attributes to relationships, do it here
    #  by adding key:value pairs to the attr dictionary
#    attr["E"] = "green"
    
    return attr
    

def addChildrenToNX(nID):
    """ int, list -> set of ints

        Takes a node ID.  Queries the neo4j db for children.
        Parses the children into the networkx graph. Returns
        a list of children ids.

        Query REturn format:
            [ID of source node int,
             relationship properties dict,
             relationship type str,
             target node properties dict,
             ID of target node int]

    """
    # setup
    children = set()

#    # either skip if visited
#    if nID in complete:
#        return children

    # create the parent node
    g.add_node(nID)
    attr = G.node(nID).get_properties()
    attr = addNodeAttributes(attr)
    g.node[nID] = attr

    # get the children
    query = q.format(nID)
    neoNodes, metadata = cypher.execute(G, query)

    # parse them
    for row in neoNodes:
        # add relationships (must be before checking complete
        #  in case we reached the node in a different way)
        g.add_edge(row[0], row[4])
        row[1] = row[1].get_properties()
        row[1] = addRelationshipAttributes(row[1])
        g[row[0]][row[4]] = row[1]
        g[row[0]][row[4]]["Relationship_Type"] = row[2]

        # Make sure we haven't done this node
        if row[4] in complete:
            continue        

        # add the node to the networkx graph
        g.add_node(row[4])
        row[3] = row[3].get_properties()
        row[3] = addNodeAttributes(row[3])
        g.node[row[4]] = row[3]
        complete.add(row[4])

        # Get children of the node
        query = q.format(row[4])
        neo_nodes, metadata = cypher.execute(G, query)

        # Add the child to the set of children
        children.add(row[4])    

    # return the children
    return children


def main(seed):
    # Initialize Queue and completed
    queue = seed
    depth = 0
    global complete

    print "Starting Node Export"
    # Crawl Indefinitely
    while depth <= maxDepth:

        # progress bar
        if len(complete) % 10 == 0:
            sys.stdout.write("*")
        
        r = random.randrange(0, 101)
        # if there's nothing in the queue, warp
        if maxDepth == 0 and len(queue) == 0 and R is not 0:
            complete = set()
            queue = [warp()]
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


        ##### DO SOMETHING RIGHT HERE #####
        children = addChildrenToNX(nID)
#        printStatus(nID, queue, complete, depth)

        # add children and divider to the queue
        if maxDepth == 0:
            queue = queue + list(children)
        else:
            queue = queue + ['-' * (depth + 1)] + list(children)

    print ""

    print "Saving File"
    nx.write_gexf(g, GEXF_FILE)


    print "Done"
        

if __name__ == "__main__":
    main(seed)    
