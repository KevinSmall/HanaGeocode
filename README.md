HanaGeocode
===========

An SAP HANA XS JavaScript utility to reverse geocode data in your HANA tables

Geocoding is the process of taking address information and turning it into geographic coordinates that can be used to view that location on a map.  Reverse geocoding is the opposite process where you start with a point on the globe (perhaps a latitude longitude coordinate) and convert it into a textual address.  This XS JavaScript utility can be used to reverse geocode data in your HANA tables.  This utility currently makes use of the Google Geocoding API but could be extended to use other service providers.

For samples and screenshots see http://scn.sap.com/community/hana-in-memory/blog/2014/02/02/reverse-geocode-your-hana-data-with-this-xs-javascript-utility.

Goals
-----
My goals were to produce something that was production ready, not just a proof of concept or a demo, but an immediately usable XS JavaScript service.  This means supporting exception handling, trace files, simulation mode and giving due consideration to security.  It also means supporting "throttling" so that the API calls are not made to Google's service too quickly (something mentioned in their terms of service).  I also wanted something that would be as easy to install as possible, so that means the very fewest files that could be cut-and-pasted and no need for package imports or configuration.  Finally I wanted something that would be easily extensible, so that more features could be added easily if it proved useful.

Files
-----
The files contained in the utility are:

* **geocodeApiGoogle.xsjslib**: XSJS Library file that wraps the Google Geocode API (and the destination file below) and provides formatted reverse geocode results from a simple JS function call.
* **geocodeApiGoogleDest.xshttpdest**: HTTP destination file, this is required by the XS engine to make calls to external URLs.
* **geodataEnrich.xsjs**: the main XS service, this is what is called to do the work of reading and writing to your tables, making use of the XSJS library above.

You have to make one tiny edit to get everything plumbed in correctly, in the **geocodeApiGoogle.xsjslib** file, go to line 22 and edit this line to contain your project path as its first parameter (so you will replace "geodataenrich.services" with your project name and path):

    var dest = $.net.http.readDestination("geodataenrich.services", "geocodeApiGoogleDest");  

URL Parameters
--------------
maxrecs    = max records to update eg 1000. Read google license about max per day
mindelayms = minimum delay in ms 
log        = omit parameter entirely for no logging, use log=active to see details on screen when url finishes, and use log=hana to write to hana trace file only (as an info level trace record)
simulate   = omit parameter entirely to update records, use simulate=active to not do any update
schema     = source schema name
table      = source table name
fldlat     = source field holding latitude
fldlon     = source field holding longitude
fldcty     = name of field in source table that will receive the Country address information (optional)
fldad1     = name of field in source table that will receive the admin level 1 information, like a region (optional)
fldad1     = name of field in source table that will receive the admin level 2 information, like a sub-region (optional)
fldad1     = name of field in source table that will receive the admin level 3 information, like a city (optional)
fldpost    = name of field in source table that will receive the post code or zip code information (optional)
fldblank   = name of field in source table that is used to identify records you want to write to, this is to prevent the same records being written to over and over again.  So if this field is NULL then this service will attempt to write to all target fields.  If this field is filled, the record will not be selected.
fldstat    = name of field in source table this will receive the status of the geocode API call (optional)

Sample URLs
-----------
All URLs will start as usual, http://<server>:80<instance>/<package path>/

Example 1
Simulate the update of 10 records to table "GEODATA"."testtable01", with 400ms delay between calls, logging to screen, and storing result of geocode API call in the field STATUS.  The field to select on is COUNTRY (ie search for records with COUNTRY=NULL) and the fields to write to are ZIP and COUNTRY. 
geodataEnrich.xsjs?schema=GEODATAENRICH&table=testtable01&maxrecs=10&mindelayms=400&log=active&simulate=active&fldblank=COUNTRY&fldstat=STATUS&fldpost=ZIP&fldcty=COUNTRY

Example 2
Actual update of 2000 records, with 100ms delay between calls, with no logging.  The field to select on is COUNTRY and the fields to write to are POSTCODE, REGION and SUBREGION. 
geodataEnrich.xsjs?schema=GEODATAENRICH&table=testtable01&maxrecs=2000&mindelayms=100&fldblank=COUNTRY&fldpost=POSTCODE&fldad1=REGION&fldad2=SUBREGION
