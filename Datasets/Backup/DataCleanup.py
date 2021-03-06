import csv
from collections import OrderedDict
import re
from neo4j import GraphDatabase

# Base location of dataset
BASE_LOC = 'C:\\Zoshyn\\Classwork\\DM\\Assignment2\\Datasets\\'

# Dataset names
DATASET_ONE_FILENAME = 'Dataset1_DNR_Camping_Parks_Reservation_Data_2016.csv'
DATASET_TWO_FILENAME = 'Dataset2_Parks_in_Canada.csv'
DATASET_THREE_FILENAME = 'Dataset3_Column_Subset_of_Parks_in_Canada.csv'
DATASET_FOUR_FILENAME = 'Dataset4_Refined_Column_Subset_of_Parks_in_Canada.csv'
DATASET_FIVE_FILENAME = 'Dataset5_Parks_in_NS.csv'
SEPERATOR = ','

# Neo4j Configuration
DATABASE_BOLT_URL = 'bolt://localhost:7687'
DB_USERNAME = 'neo4j'
DB_PASSWORD = 'pass'
PARKS_TABLE_NAME = 'NSpark'

# Debug variables
COMPLETED_TEXT = '-----===< Completed >===-----'

park_name_list = []
party_size_list = []
booking_type_list = []
rate_type_list = []
equipment_list = []

# Function to load dataset one
# Input: None
# Return: None
def process_dataset_one():
    csv_list = []
    final_csv = ''
    isFirst = True
    file_loc = BASE_LOC + DATASET_ONE_FILENAME
    with open(file_loc, mode='r') as reader:
        dataset = csv.DictReader(reader)
        for rows in dataset:
            if isFirst:
                isFirst = False
                for column_name in rows.keys():
                    csv_list.append(column_name)
                    csv_list.append(SEPERATOR)
                csv_list.pop(-1)
                csv_list.append('\n')
            if 'CANADA' in rows['Country']:
                for row in rows.values():
                    csv_list.append(row)
                    csv_list.append(SEPERATOR)
            csv_list.pop(-1)
            csv_list.append('\n')
        csv_list.pop()
    csv_content = final_csv.join(csv_list)
    write_to_file(DATASET_TWO_FILENAME, csv_content)

# Function to load dataset two
# Input: None
# Return: None
def process_dataset_two():      
    csv_list = []
    dataset_values = ''
    column_names = ['ParkName', 'State', 'partySize', 'BookingType', 'RateType', 'Equipment']
    file_loc = BASE_LOC + DATASET_TWO_FILENAME
    with open(file_loc, mode='r') as reader:
        dataset = csv.DictReader(reader)
        for rows in dataset:
            for row in rows.items():
                if row[0] in column_names:
                    csv_list.append(row[1])
                    csv_list.append(SEPERATOR)
            csv_list.pop(-1)
            csv_list.append('\n')
        csv_list.pop()
    column_name_string = SEPERATOR.join(column_names)
    column_name_string += '\n'
    dataset_values = dataset_values.join(csv_list)
    csv_content = column_name_string + dataset_values
    write_to_file(DATASET_THREE_FILENAME, csv_content)

# Function to load dataset three
# Input: None
# Return: None
def process_dataset_three():
    csv_list = []
    dataset_values = ''
    final_csv = ''
    isFirst = True
    file_loc = BASE_LOC + DATASET_THREE_FILENAME
    with open(file_loc, mode='r') as reader:
        dataset = csv.DictReader(reader)
        for rows in dataset:
            if isFirst:
                isFirst = False
                for column_name in rows.keys():
                    csv_list.append(column_name)
                    csv_list.append(',')
                csv_list.pop(-1)
                csv_list.append('\n')
            for row in rows.items():
                if row[0] == 'Equipment':
                    if 'Less than' in row[1]:
                        start_index = re.search(r"\d", row[1])
                        csv_list.append('LT'+ row[1][start_index.start():])
                    elif 'Single Tent' in row[1]:
                        csv_list.append('ST')
                    elif 'Tents' in row[1]:
                        csv_list.append('T')
                    csv_list.append(',')
                else:
                    csv_list.append(row[1])
                    csv_list.append(',')
            csv_list.pop(-1)
            csv_list.append('\n')
        csv_list.pop()
    csv_content = final_csv.join(csv_list)
    write_to_file(DATASET_FOUR_FILENAME, csv_content)

# Function to load dataset four
# Input: None
# Return: None
def process_dataset_four():
    csv_list = []
    final_csv = ''
    isFirst = True
    file_loc = BASE_LOC + DATASET_FOUR_FILENAME
    with open(file_loc, mode='r') as reader:
        dataset = csv.DictReader(reader)
        for rows in dataset:
            if isFirst:
                isFirst = False
                for column_name in rows.keys():
                    csv_list.append(column_name)
                    csv_list.append(',')
                csv_list.pop(-1)
                csv_list.append('\n')
            if 'NS' in rows['State']:
                for row in rows.values():
                    csv_list.append(row)
                    csv_list.append(',')
            csv_list.pop(-1)
            csv_list.append('\n')
        csv_list.pop()
    csv_content = final_csv.join(csv_list)
    write_to_file(DATASET_FIVE_FILENAME, csv_content)

#-- Function to load dataset five
# Input: None
# Output: None
def process_dataset_five():
    line_in_csv = ''
    lines_in_csv = []
    file_loc = BASE_LOC + DATASET_FIVE_FILENAME
    with open(file_loc, mode='r') as reader:
        dataset = csv.DictReader(reader)
        for rows in dataset:
            line_in_csv = str(rows)
            if line_in_csv not in lines_in_csv:
                lines_in_csv.append(line_in_csv)
                for row in rows.items():
                    row_refined = row[1].strip()
                    row_refined = row_refined.replace(" ", "_")
                    if row[0] == 'ParkName':
                        park_name_list.append(row_refined)
                    elif row[0] == 'partySize':
                        party_size_list.append(row_refined)
                    elif row[0] == 'BookingType':
                        booking_type_list.append(row_refined)
                    elif row[0] == 'RateType':
                        rate_type_list.append(row_refined)
                    elif row[0] == 'Equipment':
                        equipment_list.append(row_refined)

# Function to write csv content
# Input: Filename, Content to write in csv
# Return: Datasets
def write_to_file(file_name, csv_content):
    file_loc = BASE_LOC + file_name
    with open(file_loc, mode='w') as writer:
        writer.write(csv_content)

# Function to load data onto Neo4j database
# Input: None
# Return: None
def load_data():
    driver = GraphDatabase.driver(DATABASE_BOLT_URL, auth=(DB_USERNAME, DB_PASSWORD))
    with driver.session() as session:
        session.write_transaction(add_ns_node)
        print('\n-----> Created \'NS\' database')
        for i in range(len(park_name_list)):
            session.write_transaction(add_park, park_name_list[i], 'NS', party_size_list[i], booking_type_list[i], rate_type_list[i], equipment_list[i])   
        print('\n-----> Created \''+PARKS_TABLE_NAME+'\' database')
        session.write_transaction(create_relationship)
    driver.close()

# Function to add Nova Scotia node
# Input: None
# Return: None
def add_ns_node(tx):
    country = 'Canada'
    tx.run("CREATE (NS:Nova_Scotia {name: 'Nova_Scotia', country: $country})",country=country)

# Function to add parks from dataset
# Input: Attributes for Parks
# Return: None
def add_park(tx, park_name, state_name, party_size, booking_type, rate_type, equipment):
    #print("park_name: {}, state_name: {}, party_size: {}, booking_type: {}, rate_type: {}, equipment: {}".format(park_name, state_name, party_size, booking_type, rate_type, equipment))
    tx.run("MATCH (a:Nova_Scotia), (b:"+PARKS_TABLE_NAME+") WHERE a.name = 'Nova Scotia' AND b.park_name = '"+park_name+"' and b.party_size="+party_size+" and b.booking_type='"+booking_type+"' and b.rate_type='"+rate_type+"' and b.equipment='"+equipment+"' CREATE (b)-[:located_in]->(a) ")
    tx.run("CREATE ("+PARKS_TABLE_NAME+":"+park_name+" {park_name: '"+park_name+"', state_name: '"+state_name+"', party_size: "+party_size+", booking_type: '"+booking_type+"', rate_type: '"+rate_type+"', equipment: '"+equipment+"'})")

# Function to create 'NeighbourByRate' and 'NeighbourByEquipment' relationship
# Input: None
# Return: None
def create_relationship(tx):
    tx.run("MATCH (a:"+PARKS_TABLE_NAME+"),(b:"+PARKS_TABLE_NAME+") WHERE EXISTS (a.rate_type) AND EXISTS (b.rate_type) AND a.rate_type=b.rate_type CREATE (a)-[:NeighbourByRate]->(b)")
    tx.run("MATCH (a:"+PARKS_TABLE_NAME+"),(b:"+PARKS_TABLE_NAME+") WHERE EXISTS (a.equipment) AND EXISTS (b.equipment) AND a.equipment=b.equipment CREATE (a)-[:NeighbourByEquipment]->(b)")
    print('\n-----> Added \'NeighbourByRate\' and \'NeighbourByEquipment\' relationships')

# Main function
def main():
    print('Processing dataset: ' + DATASET_ONE_FILENAME)
    process_dataset_one()
    print('\n' + COMPLETED_TEXT + '\n')
    print('Processing dataset: ' + DATASET_TWO_FILENAME)
    process_dataset_two()
    print('\n' + COMPLETED_TEXT + '\n')
    print('Processing dataset: ' + DATASET_THREE_FILENAME)
    process_dataset_three()
    print('\n' + COMPLETED_TEXT + '\n')
    print('Processing dataset: ' + DATASET_FOUR_FILENAME)
    process_dataset_four()
    print('\n' + COMPLETED_TEXT + '\n')
    print('Processing dataset: ' + DATASET_FIVE_FILENAME)
    process_dataset_five()
    print('\n' + COMPLETED_TEXT + '\n')
    print('Loading dataset into Neo4j')
    load_data()
    print('\n' + COMPLETED_TEXT + '\n')

if __name__== "__main__":
    main()