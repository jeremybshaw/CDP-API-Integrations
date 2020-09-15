# PII data extracted from source table using these field mappings.
# Key : the DS-API field name.
# Value : SQL returning a single field on a PRESTO SQL select statement 


# Valid input field list for UK Demo:


# Demo mappings for UK Data
source_mapping_uk_demo = {
    'id': 'id',                                           # Source data ID, copied to output table    
#    'name': '',                                          # Unparsed name of person    
#    'namePrefix': '',                                    # Person title / refix (Mr, Dr, Prof, etc)
    'firstName': 'FORENAME1',                             # Person given first name
    'middleName': 'FORENAME2',                            # Person given middle name or initial
#    'otherMiddleName': '',                               # Person given other middle names
#    'lastNamePrefix': '',                                # Person family name prefix
    'lastName': 'SURNAME',                                # Person family name
#    'generationalSuffix': '',                            # Person generational suffix (Jr etc)    
#    'businessName': '',                                  # Business /Organisation name
    'streetAddress': """Coalesce(                         
                  Coalesce(ADDRESS_LINE1, '')||
                  Coalesce(' ' || ADDRESS_LINE2, '')||
                  Coalesce(' ' || ADDRESS_LINE3, '')||
                  Coalesce(' ' || ADDRESS_LINE4, '')||
                  Coalesce(' ' || ADDRESS_LINE5, ''), 
                '')""",                                   # Concatenated Address lines (up to 8 lines)
  		'locality': 'TOWN',                                 # Locality / City / Town
  		'administrativeArea': 'COUNTY',                     # State / County / Area
  		'postalCode': 'POSTCODE',                           # Zip or Postcode
      'countryCode': '\'GBR\''                                # ISO3 country code
  		}

 # Demo mappings for US Synthetic data set
 # for each field supply the SQL to extract it from the souce table
source_mapping_us_demo = {
    'id': 'customer_id',
    'firstName': 'first_name',
    'middleName': 'middle_name',
    'lastName': 'last_name',
    'streetAddress': """Coalesce(
                  Coalesce(primarynumber, '')||
                  Coalesce(' ' || predirectional, '')||
                  Coalesce(' ' || street, '')||
                  Coalesce(' ' || streetsuffix, '')||
                  Coalesce(' ' || postdirectional, '')||
                  Coalesce(' ' || unitdesignator, '')||
                  Coalesce(' ' || secondarynumber, ''), 
                '')""",
  		'city': 'city',
  		'state': 'state',
  		'zipCode': 'truncate(zipcode)'
  		}

source_mapping=source_mapping_us_demo