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
 Executes a breath first search of the graph, copying all nodes and
  relationships into a networkx graph.  Once parsed, safe the networkx
  graph in gexf format, (readably by gephi).

'''


from py2neo import neo4j, cypher
import networkx as nx
import random
import time

## STATIC VARIABLES
NEODB = "http://192.168.56.101:7474/db/data"
R = 0 # Number from 0 to 100 indicating the % chance to warp
GEXF_FILE = "somefile.gexf"


## SETUP
# Connect to database
G = neo4j.GraphDatabaseService(NEODB)
# The query that defines nodes to search from
q = """ START n = node({0})
        MATCH n-[r]->m
        RETURN ID(n), r, type(r), m, ID(m);
    """
# Create the networkx graph
g = nx.Graph()
# Create a set of completed nodes so we don't repeat parsing
#  of a node
complete = set()
# A list of seed node IDs to start from.
# All nodes you want to export to should be reachable
#  from the seed nodes.
seed = [30185]



## EXECUTION
def warp():
    """ NoneType -> int

        Takes nothing.  Returns a random node ID matching the query
        criteria.

    """
    query = q.format("*")
    neoNodes, metadata = cypher.execute(G, query)
    node = neoNodes[0][0]
    return node._id


def getNext(nID):
    """ int -> list ints

        Takes a node ID.  returns a list of children node IDs.

    """

    # find children 
    query = q.format(nID)
    neoNodes, metadata = cypher.execute(G, query)
    enqueue = []
    for n in neoNodes:
       enqueue.append(n[4])
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


def addChildrenToNX(nID):
    """ int -> list ints

        Takes a node ID.  Queries the neo4j db for children.  Parses the
        children into the networkx graph.  returns a list of children ids.

    """
    # setup
    children = set()

    # get the children
    query = q.format(nID)
    neoNodes, metadata = cypher.execute(G, query)

    # parse them
    for row in neoNodes:
        # add relationships (must be before checking complete
        #  in case we reached the node in a different way)
        g.add_edge(row[0], row[4])
        g[row[0]][row[4]] = row[1]
        g[row[0]][row[4]]["Relationship_Type"] = row[2]

        # Make sure we haven't done this node
        if row[4] in complete:
            continue        


        # If you want to add additional attributes to node or
        #  relationship properties, do it here by adding key:
        #  pairs to row[3] (node) and row[1] (relationship)
        pass
        

        # add the node to the python graph
        g.add_node(row[4])
        g.node[row[4]] = row[3]
        complete.add(row[4])

        # Connect to Neo4j
        graph_db = neo4j.GraphDatabaseService(NEODB)

        # Get children of the node
        query = q.format(row[4])
        neo_nodes, metadata = cypher.execute(graph_db, query)

        # Replace the target node & relationship with their properties
        for row in neo_nodes:
            row[3] = row[3].get_properties()
            row[1] = row[1].get_properties()

        # Add the child to the set of children
        children.add(row[4])    

    # return the children
    return children

def printStatus(queue, completed):
    """ list, list -> NoneType

        Takes a queue and a list of completed.  Prints the length
        of the queue and tthe list of completed ids.  Returns nothing.

        This is primarily used as a test payload.

    """
    print "Length of Queue: {0}".format(len(queue))
    print "Completed {0}\n\r{1}".format(len(completed), completed)
    time.sleep(2)


def main(seed):
    # Initialize Queue and completed
    queue = seed
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
        children = addChildrenToNX(nID)

        # choose a child of n to walk to
        queue = queue + children 
        

if __name__ == "__main__":
    main(seed)    
