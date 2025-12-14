#importing libraries and dependencies to handle the csv, json, xml file manipulations as well as os interactions (file and directory management)

from pony.orm import Database, Required, Optional, PrimaryKey, db_session
import xml.etree.ElementTree as ET
import os
import csv
import json

# functions for type conversion
def convto_int(valuenow):
    if valuenow is None:
        return None
    try:
        return int(valuenow)
    except (ValueError, TypeError):
        return None

def convto_float(valuenow):
    if valuenow is None:
        return None
    try:
        return float(valuenow)
    except (ValueError, TypeError):
        return None

def convto_bool_from_string(valuenow):
    if isinstance(valuenow, bool):
        return valuenow
    if valuenow is None:
        return None
    value_stringed = str(valuenow).strip().lower()
    if value_stringed in ("true", "yes", "1"):
        return True
    if value_stringed in ("false", "no", "0"):
        return False
    return None

# instantiating variable name type to be used later in database
db = Database()

class Customereach(db.Entity):

#instantiating database attributes (to be used as column names)

    id = PrimaryKey(int, auto=True)
    first_name = Required(str)
    last_name = Required(str)

    age = Optional(int)
    sex = Optional(str)
    retired = Optional(bool)
    dependants = Optional(int)
    marital_status = Optional(str)
    salary = Optional(int)
    pension = Optional(int)
    company = Optional(str)
    commute_distance = Optional(float)
    hr_postcode = Optional(str)

    iban = Optional(str)
    credit_card_number = Optional(str)
    credit_card_security_code = Optional(str)
    credit_card_start_date = Optional(str)
    credit_card_end_date = Optional(str)
    billing_address_main = Optional(str)
    billing_address_city = Optional(str)
    billing_address_postcode = Optional(str)

    vehicle_make = Optional(str)
    vehicle_model = Optional(str)
    vehicle_year = Optional(int)
    vehicle_type = Optional(str)

    debt = Optional(float)
    notes = Optional(str)

#To bind PonyORM to MySQL database in USBwebserver
def connect_db():
    
    db.bind(
        provider="mysql",
        host="127.0.0.1",
        port=3307,
        user="root",
        passwd="usbw",
        db="the_laurel_db"         
    )
    db.generate_mapping(create_tables=True)

#intializing a new record using variables corresponding to those above as field names for the database
def create_empty_record(first_name, last_name):
    record_info = {
        "first_name": first_name.strip(),
        "last_name": last_name.strip(),
        "age": None,
        "sex": None,
        "retired": None,
        "dependants": None,
        "marital_status": None,
        "salary": None,
        "pension": None,
        "company": None,
        "commute_distance": None,
        "hr_postcode": None,
        "iban": None,
        "credit_card_number": None,
        "credit_card_security_code": None,
        "credit_card_start_date": None,
        "credit_card_end_date": None,
        "billing_address_main": None,
        "billing_address_city": None,
        "billing_address_postcode": None,
        "vehicle_make": None,
        "vehicle_model": None,
        "vehicle_year": None,
        "vehicle_type": None,
        "debt": None,
        "notes": ""
    }
    return record_info

# To standardise first and last name variables and use them as key 
def get_the_key(first_name, last_name):
    return (first_name.strip().lower(), last_name.strip().lower())

# To create person entity
def to_get_or_create_person(people_record, first_name, last_name):
    key_v = get_the_key(first_name, last_name)
    if key_v not in people_record:
        people_record[key_v] = create_empty_record(first_name, last_name)
    return people_record[key_v]

# To load data from the given csv file
def load_data_from_csv(path, people_record):
    
    with open(path, newline="", encoding="utf-8") as f: #f stands for variable name of my choice for retrieved file object
        reader = csv.DictReader(f)
        for row in reader:
            first = row.get("First Name", "").strip()
            last = row.get("Second Name", "").strip()
            if not first or not last:
                continue
            #This code below would run if first and last names are not empty for the row
            person_v = to_get_or_create_person(people_record, first, last)

            age_strng = row.get("Age (Years)")
            if person_v["age"] is None:
                person_v["age"] = convto_int(age_strng)

            sex_value = row.get("Sex")
            if person_v["sex"] is None and sex_value:
                person_v["sex"] = sex_value

            vehicle_year = row.get("Vehicle Year")
            if person_v["vehicle_year"] is None:
                person_v["vehicle_year"] = convto_int(vehicle_year)

            """checks and updates person object vehicle info if data exists 
            in field and person object's field is not empty""" 
            for field, key_v in [
                    ("Vehicle Make", "vehicle_make"),
                    ("Vehicle Model", "vehicle_model"),
                    ("Vehicle Type", "vehicle_type"),
                ]:
                    value = row.get(field)
                    if value and not person_v.get(key_v):
                        person_v[key_v] = value

#To load data from the given json file
def load_data_from_json(path, people_record):

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    for row in data:
        first = row.get("firstName", "").strip()
        last = row.get("lastName", "").strip()
        if not first or not last:
            continue

        person_v = to_get_or_create_person(people_record, first, last)

        if "age" in row and person_v["age"] is None:
            person_v["age"] = convto_int(row.get("age"))

        mapping = [
            ("iban", "iban"),
            ("credit_card_number", "credit_card_number"),
            ("credit_card_security_code", "credit_card_security_code"),
            ("credit_card_start_date", "credit_card_start_date"),
            ("credit_card_end_date", "credit_card_end_date"),
            ("address_main", "billing_address_main"),
            ("address_city", "billing_address_city"),
            ("address_postcode", "billing_address_postcode"),
        ]

        for source_val, dst in mapping:
            value = row.get(source_val)
            if value and not person_v.get(dst):
                person_v[dst] = value

        if "debt" in row and row["debt"] not in (None, ""):
            debt_val = convto_float(row["debt"])
            if person_v["debt"] is None and debt_val is not None:
                person_v["debt"] = debt_val

# This loads data from the given xml file
def load_data_from_xml(path, people_record):
    
    #This helps us to read and navigate xml files
    tree = ET.parse(path)
    root = tree.getroot()

    for user in root.findall("user"):
        first = user.attrib.get("firstName", "").strip()
        last = user.attrib.get("lastName", "").strip()
        if not first or not last:
            continue

        person_v = to_get_or_create_person(people_record, first, last)

        if person_v["age"] is None:
            person_v["age"] = convto_int(user.attrib.get("age"))
        if person_v["sex"] is None:
            person_v["sex"] = user.attrib.get("sex")
        if person_v["retired"] is None:
            person_v["retired"] = convto_bool_from_string(user.attrib.get("retired"))
        if person_v["dependants"] is None:
            person_v["dependants"] = convto_int(user.attrib.get("dependants"))
        if person_v["marital_status"] is None:
            person_v["marital_status"] = user.attrib.get("marital_status")
        if person_v["salary"] is None:
            person_v["salary"] = convto_int(user.attrib.get("salary"))
        if person_v["pension"] is None:
            person_v["pension"] = convto_int(user.attrib.get("pension"))
        if person_v["company"] is None:
            person_v["company"] = user.attrib.get("company")
        if person_v["commute_distance"] is None:
            person_v["commute_distance"] = convto_float(user.attrib.get("commute_distance"))
        if person_v["hr_postcode"] is None:
            person_v["hr_postcode"] = user.attrib.get("address_postcode")

#This works on the tickets received in user_data.txt
def apply_text_updates(people_record):

    # First, Valerie Ellis'security code updated to 762 (from the 276 in the original record)
    valerie_ellis_key = get_the_key("Valerie", "Ellis")
    if valerie_ellis_key in people_record:
        per = people_record[valerie_ellis_key]
        per["credit_card_security_code"] = "762"
        text = 'Security code was corrected to "762" based on support ticket.'
        per["notes"] = (per["notes"] + " " + text).strip()

    # Second, salary increase of £2100 updated for Charlie West of Williams-Wheeler company
    for record_info in people_record.values():
        if (
            record_info["first_name"] == "Charlie"
            and record_info["last_name"] == "West"
            and record_info.get("company") == "Williams-Wheeler"
            and record_info.get("salary") is not None
        ):
            old_salary_info = record_info["salary"]
            record_info["salary"] = old_salary_info + 2100
            text = "Salary got increased by £2100 based on promotion email."
            record_info["notes"] = (record_info["notes"] + " " + text).strip()

    # Third, update of Charlie Short's new age of 52
    charlie_short_key = get_the_key("Charlie", "Short")
    if charlie_short_key in people_record:
        pcs = people_record[charlie_short_key]
        pcs["age"] = 52
        text = "Age was updated to 52 based on birthday message."
        pcs["notes"] = (pcs["notes"] + " " + text).strip()

    # Fourth, update 0.15% increase on Christian Martin current pension of 22896
    christian_martin_key = get_the_key("Christian", "Martin")
    if christian_martin_key in people_record:
        pcm = people_record[christian_martin_key]
        if pcm.get("pension") is not None:
            former_pension = float(pcm["pension"])
            val_increase = former_pension * 0.0015
            pension_now = int(round(former_pension + val_increase))
            pcm["pension"] = pension_now
            text = "Pension got increased by 0.15% based on policy change email."
            pcm["notes"] = (pcm["notes"] + " " + text).strip()

#Initializing people_record and coordination of path for the document files (csv,json,txt,json) given
def load_all_the_data(base_folder):

    people_record = {}

    csv_path = os.path.join(base_folder, "user_data.csv")
    json_path = os.path.join(base_folder, "user_data.json")
    xml_path = os.path.join(base_folder, "user_data.xml")
    txt_path = os.path.join(base_folder, "user_data.txt")

    if os.path.exists(csv_path):
        load_data_from_csv(csv_path, people_record)
    if os.path.exists(json_path):
        load_data_from_json(json_path, people_record)
    if os.path.exists(xml_path):
        load_data_from_xml(xml_path, people_record)

    if os.path.exists(txt_path):
        apply_text_updates(people_record)

    return people_record


#To Save to the Database
@db_session
def insert_customer(record_info):
    #This creates a record if customer is not on the database and updates the record if existing in the database
    person_v = Customereach.get(
        first_name=record_info["first_name"],
        last_name=record_info["last_name"]
    )
    if person_v is None:
        person_v = Customereach(**record_info)
    else:
        # update non-empty fields
        for key_v, value in record_info.items():
            if key_v in ("first_name", "last_name"):
                continue
            if value not in (None, ""):
                setattr(person_v, key_v, value)

def save_to_database(people_record):
    
    #This goes through the unified records and sends them through PonyROM to MySQL database
    for record_info in people_record.values():
        insert_customer(record_info)

def main():
    #This function contains database connection logic
    connect_db()

    # This loads data from all sources and makes them unified
    base_folder = os.path.dirname(os.path.abspath(__file__))
    people_record = load_all_the_data(base_folder)
    print("Unified records loaded:", len(people_record))

    # This saves to database
    save_to_database(people_record)
    print("All records have been written to database.")

if __name__ == "__main__":
    main()