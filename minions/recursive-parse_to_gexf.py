'''
 AUTHOR: Gabriel Bassett
 DATE: 08-27-2013
 DEPENDANCIES: py2neo, networkx
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
 Executes a recursive depth first search of the graph, copying all nodes and
  relationships into a networkx graph.  Once parsed, safe the networkx
  graph in gexf format, (readably by gephi).

'''

## IMPORTS
from py2neo import neo4j, cypher
import networkx as nx
from datetime import datetime

## STATIC VARIABLES

NEODB = "http://192.168.56.101:7474/db/data"
GEXF_FILE = "somefile.gexf"



## SETUP
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
seed = [82719]
maxDepth = 5



## EXECUTION
def addNodeAttributes(attr):
    """ dict -> dict

        Takes a dictionary of node attributes.  Adds any additional
        desired attributes.  Returns the dictionary.

    """
    # If you want to add additional attributes to nodes, do it here
    #  by adding key:value pairs to the attr dictionary
    # Get the IP and CIDR and add it to the attributes
    pass

    return attr


def addRelationshipAttributes(attr):
    """ dict -> dict

        Takes a dictionary of relationship attributes.  Adds any
        additional desired attributes.  Returns the dictionary.

    """
    # If you want to add additional attributes to relationships, do it here
    #  by adding key:value pairs to the attr dictionary
    pass
    
    return attr


def dfs_parse_nodes(rows, depth):
    """ (list of list of [int, dict, str, dict, int]), int -> NoneType

    [ID of source node int,
     relationship properties dict,
     relationship type str,
     target node properties dict,
     ID of target node int]

    Takes the output of a cypher query and parses the nodes in it.
    Executes Depth First Search
    Operates Recursively

    """
    if maxDepth is not 0 and depth > maxDepth:
        return    
    
    for row in rows:
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


        # add the node to the python graph
        g.add_node(row[4])
        row[3] = row[3].get_properties()
        row[3] = addNodeAttributes(row[3])
        g.node[row[4]] = row[3]
        complete.add(row[4])

        # Connect to Neo4j
        graph_db = neo4j.GraphDatabaseService(NEODB)

        # Get children of the node
        query = q.format(row[4])
        neo_nodes, metadata = cypher.execute(graph_db, query)

       # Parse Children
        dfs_parse_nodes(neo_nodes, depth + 1)        
    

def main(seed):
    depth = 0
    global g

    # Connect to Neo4j
    graph_db = neo4j.GraphDatabaseService(NEODB)

    print "Starting Node Export at {0}.".format(
        datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))

    for s in seed:
        query = q.format(s)
        neo_nodes, metadata = cypher.execute(graph_db, query)

        # add the node
        g.add_node(neo_nodes[0][0])
        attr = graph_db.node(neo_nodes[0][0]).get_properties()
        attr = addNodeAttributes(attr)
        g.node[neo_nodes[0][0]] = attr
        complete.add(neo_nodes[0][0])

        # pass them to the recursive DFS
        dfs_parse_nodes(neo_nodes, depth + 1)


    print "Saving File"
    nx.write_gexf(g, GEXF_FILE)


    print "Done at {0}.".format(datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))


if __name__ == '__main__':
    main(seed)
