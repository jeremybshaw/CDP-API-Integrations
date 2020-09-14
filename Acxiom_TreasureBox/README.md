# Acxiom Data Services (DS-API) Integration

## Integration Overview

The data services API (DS-API) provides a common interface to access Acxiom's core data products which include:

* Name and address parsing and postal hygiene service based enhancements
* Matching PII to Acxiom's data products and enhancing with data bundles.
* Enhanced First party identity resolution through Acxiom's Identity BuilderTM

The following guide covers the steps to integrate a Treasure Data workflow with these services:

[Example Data Flow](img/Integration_OverviewIntegration_Overview.jpgimg/Integration_OverviewIntegration_Overview.jpg)

# Pre-requisites

1. Login access to Treasure Data console. (e.g.[https://console.treasuredata.com/app/integrations/sources](https://console.treasuredata.com/app/integrations/sources))
2. A Login account on Acxiom Developer DS-API console. Register here[https://developer.myacxiom.com](https://developer.myacxiom.com/)
3. Add an 'Application Name' in the DS-API Developer console to create an API Key (DSAPI.CLIENT_ID) and API Secret (DSAPI.CLIENT_SECRET)
4. A Treasure Data API key with Master Access:

![](https://confluence.acxiom.com/download/attachments/97530437/image2020-5-11_14-0-56.png?version=1&modificationDate=1589202057000&api=v2)

## Source data preparation

1. If required, create a new database via the TD console using the ![](https://confluence.acxiom.com/download/thumbnails/97530437/image2020-5-11_14-39-43.png?version=1&modificationDate=1589204384000&api=v2) menu option and pressing the ![](https://confluence.acxiom.com/download/thumbnails/97530437/image2020-5-11_14-41-10.png?version=1&modificationDate=1589204471000&api=v2) button.
2. Upload PII data to database using the Integration -> Sources screen. Click on the ![](https://confluence.acxiom.com/download/thumbnails/97530437/image2020-5-20_11-8-17.png?version=1&modificationDate=1589969297000&api=v2) button. The data must contain an identity field.
   
   ![](https://confluence.acxiom.com/download/attachments/97530437/image2020-5-20_11-6-56.png?version=1&modificationDate=1589969216000&api=v2)

## Add enrichment workflow

1. Add a workflow template and click the![](https://confluence.acxiom.com/download/thumbnails/97530437/image2020-5-20_11-26-32.png?version=1&modificationDate=1589970392000&api=v2) button.![](https://confluence.acxiom.com/download/thumbnails/97530437/image2020-5-20_11-25-41.png?version=1&modificationDate=1589970341000&api=v2)
2. Under workflow definition, click the edit![](https://confluence.acxiom.com/download/thumbnails/97530437/image2020-5-20_11-29-14.png?version=1&modificationDate=1589970555000&api=v2)  and past in the digdag from GIT repository:![](https://confluence.acxiom.com/download/attachments/97530437/image2020-5-20_11-28-41.png?version=1&modificationDate=1589970521000&api=v2)
3. Next add in the Python scripts and paste the template code from GIT:![](https://confluence.acxiom.com/download/thumbnails/97530437/image2020-5-20_11-30-53.png?version=1&modificationDate=1589970653000&api=v2)
4. Next add in the secrets:![](https://confluence.acxiom.com/download/attachments/97530437/image2020-9-11_17-24-25.png?version=1&modificationDate=1599841466000&api=v2)
   Note: Leave DSAPI.TENANTID=empty for the US Synthetic data.

## Configuring the Enrichment Flow

### Set input mapping rules

The input PII mapping rules are stored in the python script mappings.py.

The script contains mapping rules specified for each DS-API request field. The rules are specified using SQL functions as you would use in a SQL selected statement.

For example:

**mapping.py**
```
source_mapping = {
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
```

The DS-API fields are documented in developer.myacxiom.com.

### Set the runtime parameters in the DigDag



