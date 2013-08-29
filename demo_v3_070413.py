"""
SCRIPT NAME:
Graph CSV Import


COPYWRITE:
Copywrite 2013 Gabriel Bassett


LICENSE:
This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public LIcense as published by
the Free Sofware Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy fo the GNU General Public License
along witht his program.  If not, see <http://www.gnu.org/licenses/>.


DESCRIPTION:
This script takes paths listed one per line in a csv file and creates
four different types of graphs (neo4j, ubigraph, networkx, gephi)
from the data.  It also writes out a gexf file based on the networkx
graph.  Nodes with the same name are combined into a single node.


DEPENDS ON:
py2neo
networkx
websocket-client
jenkins (used for getting 32int hashes for use with xml-rpc)

NOTES:
-To remove any of the 4 graphs, comment out the appropriate part of the
 following sections:
 1. Static Variables
 2. Setup
 3. Node and edge import (both the 1st node & looping section
-Jenkins is a binary package and required I move lookup3.so on install
 It can be replaced with any 32bit hash function.

"""

## IMPORTS
from py2neo import neo4j, cypher
import networkx as nx
import xmlrpclib # For UBIGRAPH
from time import sleep
from websocket import create_connection # for gephi
import json # for gephi
import csv
import jenkins

## STATIC VARIABLES
GEXF_FILE = "/home/gabe/Development/CFP/Derbycon/output.gexf"
CSV_FILE = "/home/gabe/Development/CFP/Derbycon/attack_paths_v2.csv"
NEODB = "http://localhost:7474/db/data"
UBIGRAPH = "http://localhost:20738/RPC2"
GEPHI = "ws://localhost:8080/workspace0"
CLASSES = {"ac":"actor", "at":"attribute", "e":"event", "c":"condition"}
sleep1 = .3 # time in seconds to sleep between adding nodes
sleep2 = 2 # time in seconds to sleep between adding paths
sleep3 = .3 # time to sleep between setup steps
sleep4 = 3 # time to sleep between setup and import

## SETUP

# A little verbosity
print "Setting up the graphs"
print "---------------------"

# Connect to Neo4j
print "Setting up Neo4j"
graph_db = neo4j.GraphDatabaseService(NEODB)
graph_db.clear()

# Connect to Ubigraph
sleep(sleep3)
print "Setting up Ubigraph"
server = xmlrpclib.Server(UBIGRAPH)
u = server.ubigraph
u.clear()

# Set up Networkx Graph
sleep(sleep3)
print "Setting up networkx"
g = nx.DiGraph()

# Connect to Gephi Websocket
sleep(sleep3)
print "Setting up the Gephi websocket"
graph_ws = create_connection(GEPHI)

# Open the CSV file
sleep(sleep3)
print "Opening the CSV file"
csvfile = file(CSV_FILE, "rb")
lineReader = csv.reader(csvfile, delimiter=",", quotechar="\"")


print "Setup Complete"
print ""
sleep(sleep4)


## EXECUTION
def import_node_to_neo(graph_db, node):
    """py2neo graph object, dict -> py2neo node, bool

        Takes a py2neo graph object and a node dictionary and adds
         the node to the graph (if it doesn't already exist).
         returns the node.
         
    """
    # Build the query string
    s = ""
    for key in node:
        s = s + "n.{0}! = \"{1}\" AND ".format(key, node[key])
    s = s.rstrip("AND ")
    query = "START n=node(*) WHERE " + s + " RETURN n;"
    
    # Query for the node
    data, metadata = cypher.execute(graph_db, query)

    # If the query is empty
    if len(data) == 0:
        # create node with the attributes
        d, = graph_db.create(node)
        b = False
    # If the query has something
    else:
        # assign the node to the first returned node
        d = data[0][0]
        b = True

    return d, b


def import_node_to_ubigraph(u, node):
    """xmlrpc server, dict -> NoneType 

        Takes an xml rpc server and a node dictionary and sends
         the node to the ubigraph server.
         
    """
    name = jenkins.hashlittle(node["name"])

    # Create Node
    u.new_vertex_w_id(name)

    # Add node name as a label
    u.set_vertex_attribute(name, "label", node["name"])


def import_node_to_networkx(g, node):
    """networkx graph object, dict -> NoneType

        Takes a networkx graph object and a node dictionary and adds
         the node to the graph.
         
    """
    # Create a copy of the node dictionary
    n = node.copy()
    
    name = n.pop("name")
    
    # create node with attributes
    g.add_node(name, attr_dict=n)


def import_node_to_gephi(graph_ws, node):
    """websocket, dict -> NoneType

        Takes a websocket and a node dictionary and adds the node to
          gephi through a websocket.

    """
    # Create a copy of the node dictionary
    n = node.copy()   
    
    name = n.pop("name")

    # Create the string formatted for the gephi websocket plugin
    s = json.dumps({"an":{name:n}})

    # Send the node (including attributes)
    graph_ws.send(s)


def import_edge_to_neo(graph_db, edge):
    """py2neo graph object, dict -> py2neo edge object, bool

        Takes a py2neo graph object and a edge dictionary and adds
         the edge to the graph.  Returns the edge and a bool of
         if an edge was found.
         
    """
    source = edge.pop("source")
    target = edge.pop("target")
    relationship = "leads_to"
    
    # Check for edge
    query = "START n=node(*) MATCH n-[r:{0}]->m WHERE ID(n) = {1} AND ID(m) = {2} RETURN r;".format(relationship, source._id, target._id)
    data, metadata = cypher.execute(graph_db, query)        

    # If the relationship doesn't exist
    if len(data) == 0:
        # Create a relationship from the node to the exporter
        r, = graph_db.create((source, relationship ,target))
        b = False
    else:
        r = data[0][0]
        b = True

    # Add attributes
    r.update_properties(edge)

    return r, b


def import_edge_to_ubigraph(u, edge):
    """xmlrpc server, dict -> NoneType

        Takes an xml rpc server and a edge dictionary and sends
         the edge to the ubigraph server.
         
    """
    # Create a copy of the edge
    e = edge.copy()

    # Hash the source/target to make an ID for the edge
    edgeID = jenkins.hashlittle("{0}{1}".format(e["source"], e["target"]))
    sourceID = jenkins.hashlittle(e.pop("source"))
    targetID = jenkins.hashlittle(e.pop("target"))

    # Create the node
    u.new_edge_w_id(edgeID, sourceID, targetID)


def import_edge_to_networkx(g, edge):
    """networkx graph object, dict -> NoneType

        Takes a networkx graph object and a edge dictionary and adds
         the edge to the graph.
         
    """
    # Create a copy of the edge
    e = edge.copy()
    
    source = e.pop("source")
    target = e.pop("target")

    # Add the edge with attributes
    g.add_edge(source, target, e)


def import_edge_to_gephi(graph_ws, edge):
    """websocket, dict -> NoneType

        Takes a websocket and a edge dictionary and adds the edge to
          gephi through a websocket.

    """
    # Make sure the edge is directional
    edge["directed"] = True
    
    # Hash the source/target to make an ID for the edge
    edgeID = jenkins.hashlittle("{0}{1}".format(edge["source"], edge["target"]))

    # Create the string formatted for the gephi websocket plugin
    s = json.dumps({"ae":{edgeID:edge}})

    # Add the edge with attributes
    graph_ws.send(s)


def main():
    # Node dictionaries should be {"name":text, "class":class}
    # Edge dictionaries should be {"source":sourceID, "target":TargetID}

    # A little verbosity
    print "Starting attack path import"
    print "---------------------------"


    # Make sure we're on the first line
    csvfile.seek(0)

    # Read each line of the csv file
    for line in lineReader:
        print "Importing Attack Path {0}".format(line[0])

        # Define the first nodes characteristics
        name = line[1].split(":")[0]
        Class = line[1].split(":")[1]
        node = {"name":name, "Class":Class}

        # Import the first node
        neoSource, b = import_node_to_neo(graph_db, node)
        import_node_to_ubigraph(u, node)
        import_node_to_networkx(g, node)
        import_node_to_gephi(graph_ws, node)

        # Define the Source for future edges
        nodeSource = node["name"]

        for i in range(2,len(line)):
            sleep(sleep1) # slow the process down a bit

            # Define the first nodes characteristics
            name = line[i].split(":")[0]
            Class = line[i].split(":")[1]
            node = {"name":name, "Class":Class}

            # Import the node
            neoTarget, b = import_node_to_neo(graph_db, node)
            import_node_to_ubigraph(u, node)
            import_node_to_networkx(g, node)
            import_node_to_gephi(graph_ws, node)

            # Define the edges
            neoEdge = {"source":neoSource, "target":neoTarget}
            edge = {"source":nodeSource, "target":node["name"]}
            
            # Import the edge
            import_edge_to_neo(graph_db, neoEdge)
            import_edge_to_ubigraph(u, edge)
            import_edge_to_networkx(g, edge)
            import_edge_to_gephi(graph_ws, edge)

            # update the source node for edge imports
            neoSource = neoTarget
            nodeSource = node["name"]

        sleep(sleep2) # pause between path imports

    print "Attack path import complete"
    print ""
    

    # Save the GEXF File from networkx
    print "Saving graph to file"
    nx.write_gexf(g, GEXF_FILE)
    sleep(sleep2)
    print "Complete"
        
if __name__ == "__main__":
    main()
