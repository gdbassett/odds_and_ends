'''
 AUTHOR: Gabriel Bassett
 DATE: 07-11-2013
 DEPENDANCIES: uuid, py2neo
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
 Imports a csv file into a database.

 TODO:
 1. Parameterize queries
 2. Work on get_parent and create_row_anchor including function definitions

'''


from py2neo import neo4j, cypher
#import networkx as nx
import csv
from datetime import datetime # used to calculate times
import logging


## EDIT THESE VARIABLES ##
# The database location
NEODB = "http://192.168.56.101:7474/db/data"
# The file to read in
CSV_FILE = "input_file.csv"
# List of the columns in the CSV file to import
#  Current values are for example only and should be replaced
import_list = [0,1,2]
# Establish the list of attribute names from the CSV file:
#  Current values are for example only and should be replaced
attributes = ["IP", "nb_name", "mac_address", "start_time", "os_name"]
# Set a relationship type between row anchor nodes and parents
#  Current value is for example only and should be replaced
parentRelationshipType = "has_child"
# The row to start on.  0 to start at beginning of file.
startRow = 0
# If the first row is column headers, set to true.
columnHeaders = True
##

## SETUP
# Connect to database
G = neo4j.GraphDatabaseService(NEODB)
# Logging Setup
FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(format=FORMAT, level=logging.ERROR)



## EXECUTION
def get_or_create_node(attr, match = ""):
    """ (dict of propertiy key:value pairs), str -> py2neo node object, bool

    Takes a dict of node properties and either gets the first node matching it or
    creates the node.  Returns the node and True if the node previously existed.
    Should not be used to update properties

    Optionally pass a cypher match statement to narrow search.  The initial node
     is 'n'.  If match statement causes a cypher error, query will be rerun without it.

    """
    global G
    
    # Build the query string
    s = ""
    for key in attr:
        s = s + "n.{0}! = {1} AND ".format(key, "{"+key+"}")
    s = s.rstrip("AND ")
    matchQuery = "START n=node(*) " + match + " WHERE " + s + " RETURN n;"
    query = "START n=node(*) WHERE " + s + " RETURN n;"
    try:
        data, metadata = cypher.execute(G, query, attr)
    except Exception:
        data, metadata = cypher.execute(G, matchQuery, attr)
        
    # If the query is empty
    if len(data) == 0:
        # create node with the IP
        d, = G.create(attr)
        b = False
    # If the query has something
    else:
        # assign the node to the first returned node
        d = data[0][0]
        b = True

    return d, b


def get_or_create_edge(source, target, relationship, attr = {}):
    """ (py2neo node object, py2neo node obj, hashable_obj, dict) -> py2neo edge object

    Takes a source ID, target ID, and relationship type and returns the first matching relationship
    in the database.  If no matches are found, creates the relationship. returns relationship and if relationship existed

    """
    global G

    # Check for edge
    s = ""
    for key in attr:
        s = s + " AND n.{0}! = {1} ".format(key, "{"+key+"}")
    query = "START n=node(*) MATCH n-[r:{0}]->m WHERE ID(n) = {1} AND ID(m) = {2} {3} RETURN r;".format(
                                                                relationship, "{sourceID}", "{targetID}", s)
    params = attr.copy()
    params["sourceID"] = source._id
    params["targetID"] = target._id

    data, metadata = cypher.execute(G, query, params)        

    # If the relationship doesn't exist
    if len(data) == 0:
        # Create a relationship from the node to the exporter
        r, = G.create((source, relationship ,target))
        # Add attributes
        r.update_properties(attr)
        b = False
    else:
        r = data[0][0]
        b = True

    return r, b


def create_row_anchor(r, *args, **xargs):
    """ list -> py2neo node object

        Takes a list representing the row of a csv file.  Returns an anchor node for the row.

        In the example code, the row number is used as the anchor node.
         
    """
    ### REPLACE WITH YOUR OWN CODE ###
    n, b = get_or_create_node({"row_number":args[0]})
    ### REPLACE WITH YOUR OWN CODE ###

    return n


def get_parent(n, r, *args, **xargs):
    """ py2neo node object, list -> py2neo node object

        Takes a child node and a list representing the row of a csv file.  Returns a node
         to be assigned as the child node's parent.

        In the example code, the parent is based on the csv file.
         
    """
    ### REPLACE WITH YOUR OWN CODE ###
    filename = CSV_FILE.split("/")
    filename = filename[len(filename) - 1]
    filename = CSV_FILE.split("\\")
    filename = filename[len(filename) - 1]
    n, b = get_or_create_node({"file": filename})
    ### REPLACE WITH YOUR OWN CODE ###

    return n
                        

def main():
    global startRow
  
    # open csv
    with open(CSV_FILE, 'rb') as f:
        # if there are column headers, read them
        if columnHeaders:
            f.seek(0)
            linereader = csv.reader(f, delimiter=',', quotechar='\"')
            attributes = linereader.next()
            if startRow is not 0:
                startRow -= 1
            
        # skip to a specific row based on startRow variable
        for i in range(1,startRow):
            linereader.next()


        print "Starting import at {0}.".format(
            datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))

        counter = 0

        # for line in csv
        for row in linereader:
            logging.debug(row)
            
            # get the root node for the row
            hostNode = create_row_anchor(row, linereader.line_num)

            # Import the columns in the csv and link to the host node
            for c in import_list:
                if row[c]:
                    attr = {"Class":"attribute",
                            "attribute":attributes[c],
                            attributes[c]:row[c]}
                    attrNode, b = get_or_create_node(attr)
                    # connect the attribute to the host with an edge
                    # Slight speedup.  If attrNode is new, just create edge)
                    if b:
                        G.create((hostNode, "described_by", attrNode))
                    else:
                        get_or_create_edge(hostNode, attrNode, "described_by")

            # Link the hostNode to a parent in the graph
            parentNode = get_parent(hostNode, row)
            if parentNode:
                get_or_create_edge(parentNode,hostNode,parentRelationshipType)

            # increment counter
            if counter % 10 == 0:
                print linereader.line_num
            counter += 1

            
    print "Done at {0}.".format(datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
   

if __name__ == "__main__":
    main()    
