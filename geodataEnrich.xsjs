//---------------------------------------------------------------------------------------------------------
// geodataEnrich.xsjs
//---------------------------------------------------------------------------------------------------------
// This is an XSJS service that can be used to enrich existing data with geocoding information.  For example, records
// with a latitude and longitude can have additional data populated giving country, region, postcode etc.  The address
// information is retrieved using Google's geocoding API.
//--------------------------------------------------------------------------------------------------------- 
// URL Parameters:
//   maxrecs    = max records to update eg 1000. Read google license about max per day
//   mindelayms = minimum delay in ms 
//   log        = omit parameter entirely for no logging, use log=active to see details on screen when url finishes, and use 
//                log=hana to write to hana trace file only (as an info level trace record)
//   simulate   = omit parameter entirely to update records, use simulate=active to not do any update
//   schema     = source schema name
//   table      = source table name
//   fldlat     = source field holding latitude
//   fldlon     = source field holding longitude
//   fldcty     = name of field in source table that will receive the Country address information (optional)
//   fldad1     = name of field in source table that will receive the admin level 1 information, like a region (optional)
//   fldad1     = name of field in source table that will receive the admin level 2 information, like a sub-region (optional)
//   fldad1     = name of field in source table that will receive the admin level 3 information, like a city (optional)
//   fldpost    = name of field in source table that will receive the post code or zip code information (optional)
//   fldblank   = name of field in source table that is used to identify records you want to write to, this is to prevent the
//                same records being written to over and over again.  So if this field is NULL then this service will attempt
//                to write to all target fields.  If this field is filled, the record will not be selected.
//   fldstat    = name of field in source table this will receive the status of the geocode API call (optional)
//---------------------------------------------------------------------------------------------------------
// Sample URLs:
//   All URLs will start as usual, http://<server>:80<instance>/<package path>/
//
//   Example 1
//   Simulate the update of 10 records to table "GEODATA"."testtable01", with 400ms delay between calls, logging to screen, and storing
//   result of geocode API call in the field STATUS.  The field to select on is COUNTRY (ie search for records with COUNTRY=NULL) and 
//   the fields to write to are ZIP and COUNTRY. 
//   geodataEnrich.xsjs?schema=GEODATAENRICH&table=testtable01&maxrecs=10&mindelayms=400&log=active&simulate=active&fldblank=COUNTRY&fldstat=STATUS&fldpost=ZIP&fldcty=COUNTRY
//
//   Example 2
//   Actual update of 2000 records, with 100ms delay between calls, with no logging.  The field to select on is COUNTRY and the fields to 
//   write to are POSTCODE, REGION and SUBREGION. 
//   geodataEnrich.xsjs?schema=GEODATAENRICH&table=testtable01&maxrecs=2000&mindelayms=100&fldblank=COUNTRY&fldpost=POSTCODE&fldad1=REGION&fldad2=SUBREGION
//
//---------------------------------------------------------------------------------------------------------
$.import("geodataenrich.services","geocodeApiGoogle");
var GEOAPIGOOGLE = $.geodataenrich.services.geocodeApiGoogle;

//---------------------------------------------------------------------------------------------
//GLOBALS
//---------------------------------------------------------------------------------------------
var gConstants = {
	EMPTY : 'EMPTY',
	NOTUSED : 'NOT USED',
};

var gConfig = {
	// Values below are used as defaults if values not specified in URL
	maxRecords : 2500,
	minDelayMS : 500,
	serviceProvider : 'google',
	detailedLogging : 'notactive',
	simulate: 'notactive',
};

var gTable = {
	// Inbound table-related globals (values below areused as defaults of not specified in URL)
	sourceSchema : gConstants.EMPTY,
	sourceTable : gConstants.EMPTY,
	sourceTableKey : gConstants.EMPTY,	// string of all key fields suitable for a select statement field list
	sourceTableKeyFieldList : [],       // list (well, an array) of key fields
	sourceTableKeyFieldCount : 0,       // count of key fields
	sourceFieldLat : 'LATITUDE',
	sourceFieldLon : 'LONGITUDE',
	
	// Processing table-related globals
	sourceTableKeyCount : 0,
	resultSet : null,
	resultSetFieldCount : 0,
	targetProperties : [],  // array of used JSON property names that the geocode xsjslib library call will return, indexed same as targetFieldnames
	targetFieldnames : [],  // array of table field names to write to, indexed same as targetProperties
	targetFieldCount : 0,   // count of fields that will be filled eg just country, or country + zip etc
	
	// Outbound table-related globals
	targetFieldCountry : gConstants.NOTUSED,
	targetFieldAdmin1 : gConstants.NOTUSED,
	targetFieldAdmin2 : gConstants.NOTUSED,
	targetFieldAdmin3 : gConstants.NOTUSED,
	targetFieldPostalCode : gConstants.NOTUSED,
	targetFieldThatIsBlank : gConstants.EMPTY,
	targetFieldStatus : gConstants.NOTUSED,
};

//---------------------------------------------------------------------------------------------
//Entry point
//---------------------------------------------------------------------------------------------
var gLog = '';  // global log
var gRecsProcessed = 0;

try {
	prepareParameters();
	readDataFromTable();
	mainProcessing();	
} catch(e) {
	// on exception, force log to be shown
	gConfig.detailedLogging = 'active';
	if (e.name === 'UrlError' || e.name === 'TableError') {
		// Error already logged, nothing to do
	} else { 
		throw(e);
	}	
} finally {
	finish();
}

//---------------------------------------------------------------------------------------------
// Functions
//---------------------------------------------------------------------------------------------
// global log
function log(s) {
	let i = 1;
	gLog += '\n';
	gLog += s.toString();
	// optionally copy log to hana trace files
	if (gConfig.detailedLogging === 'hana') {
		$.trace.info(s.toString());
	}
}

// Read parameters from URL or use defaults
function prepareParameters() {	
	var i = 0;
	// Override defaults with parameters from the URL
	gConfig.maxRecords = $.request.parameters.get("maxrecs") || gConfig.maxRecords;	
	gConfig.minDelayMS = $.request.parameters.get("mindelayms") || gConfig.minDelayMS;
	gConfig.serviceProvider = $.request.parameters.get("provider") || gConfig.serviceProvider;
	gConfig.detailedLogging = $.request.parameters.get("log") || gConfig.detailedLogging;
	gConfig.simulate = $.request.parameters.get("simulate") || gConfig.simulate;
	gTable.sourceSchema = $.request.parameters.get("schema") || gTable.sourceSchema;
	gTable.sourceTable = $.request.parameters.get("table") || gTable.sourceTable;
	gTable.sourceFieldLat = $.request.parameters.get("fldlat") || gTable.sourceFieldLat;
	gTable.sourceFieldLon = $.request.parameters.get("fldlon") || gTable.sourceFieldLon;
	gTable.targetFieldCountry = $.request.parameters.get("fldcty") || gTable.targetFieldCountry; 
	gTable.targetFieldAdmin1 = $.request.parameters.get("fldad1") || gTable.targetFieldAdmin1; 
	gTable.targetFieldAdmin2 = $.request.parameters.get("fldad2") || gTable.targetFieldAdmin2; 
	gTable.targetFieldAdmin3 = $.request.parameters.get("fldad3") || gTable.targetFieldAdmin3; 
	gTable.targetFieldPostalCode = $.request.parameters.get("fldpost") || gTable.targetFieldPostalCode; 
	gTable.targetFieldThatIsBlank = $.request.parameters.get("fldblank") || gTable.targetFieldThatIsBlank; 
	gTable.targetFieldStatus = $.request.parameters.get("fldstat") || gTable.targetFieldStatus;

	// log
	log('=== Parameters ==========================================================');
	log('Config');
	log('  Maximum API calls to make  : ' + gConfig.maxRecords);
	log('  Min delay between calls    : ' + gConfig.minDelayMS + ' milliseconds');
	log('Source data');
	log('  Source schema              : ' + gTable.sourceSchema);
	log('  Source table               : ' + gTable.sourceTable);
	log('  Source field for latitude  : ' + gTable.sourceFieldLat);
	log('  Source field for longitude : ' + gTable.sourceFieldLon);
	log('  Select where this is NULL  : ' + gTable.targetFieldThatIsBlank);
	log('Target data');
	log('  Target field for country   : ' + gTable.targetFieldCountry);
	log('  Target field for admin1    : ' + gTable.targetFieldAdmin1);
	log('  Target field for admin2    : ' + gTable.targetFieldAdmin2);
	log('  Target field for admin3    : ' + gTable.targetFieldAdmin3);
	log('  Target field for postcode  : ' + gTable.targetFieldPostalCode);
	log('  Target field for status    : ' + gTable.targetFieldStatus);
	log(' ');
	
	// Prepare arrays of target fields asked for
	gTable.targetFieldnames = [];
	gTable.targetProperties = [];
	i = 0;
	// Country
	if (gTable.targetFieldCountry !== gConstants.NOTUSED) {
		gTable.targetFieldnames[i] = gTable.targetFieldCountry;
		gTable.targetProperties[i] = 'country'; // this is property name on object returned by the .xsjslib geocode wrapper call
		i++;
	}
	// Admin 1, Region
	if (gTable.targetFieldAdmin1 !== gConstants.NOTUSED) {
		gTable.targetFieldnames[i] = gTable.targetFieldAdmin1;
		gTable.targetProperties[i] = 'administrative_area_level_1'; 
		i++;
	}
	// Admin 2, Sub-Region
	if (gTable.targetFieldAdmin2 !== gConstants.NOTUSED) {
		gTable.targetFieldnames[i] = gTable.targetFieldAdmin2;
		gTable.targetProperties[i] = 'administrative_area_level_2'; 
		i++;
	}
	// Admin 3, Locality
	if (gTable.targetFieldAdmin3 !== gConstants.NOTUSED) {
		gTable.targetFieldnames[i] = gTable.targetFieldAdmin3;
		gTable.targetProperties[i] = 'administrative_area_level_3'; 
		i++;
	}
	// Post code, zip code
	if (gTable.targetFieldPostalCode !== gConstants.NOTUSED) {
		gTable.targetFieldnames[i] = gTable.targetFieldPostalCode;
		gTable.targetProperties[i] = 'postal_code'; 
		i++;
	}
	// Status field
	if (gTable.targetFieldStatus !== gConstants.NOTUSED) {
		gTable.targetFieldnames[i] = gTable.targetFieldStatus;
		gTable.targetProperties[i] = 'status'; 
		i++;
	}	
	gTable.targetFieldCount = i;
	
	// Perhaps there is nothing to do
	if (gTable.targetFieldCount === 0) {
		log('*** ERROR: No target fields specified in the URL');
		throw {name : "UrlError", };
	};	
	if (gTable.sourceSchema === gConstants.EMPTY) {
		log('*** ERROR: No source schema specified in the URL');
		throw {name : "UrlError", };
	};
	if (gTable.sourceTable === gConstants.EMPTY) {
		log('*** ERROR: No source table specified in the URL');
		throw {name : "UrlError", };
	};	
	if (gTable.targetFieldThatIsBlank === gConstants.EMPTY) {
		log('*** ERROR: You must specify a target field that is blank to allow selection of records');
		throw {name : "UrlError", };
	};	

}

function readDataFromTable() {
	
	//--------------------------------------------------------
	// Read the table's meta data
	//--------------------------------------------------------
	var query = prepareQueryForMetadata();
	var connSelect = $.db.getConnection();
	// query string with ? params
	var pstmtSelect = connSelect.prepareStatement(query);
	// parameter replacement
	pstmtSelect.setString(1, gTable.sourceSchema);
	pstmtSelect.setString(2, gTable.sourceTable);
	var rs = pstmtSelect.executeQuery();
	var fld = '';
	var keyCount = 0;
	
	// Build string representing table key and table key with parameters
	gTable.sourceTableKey = '';	
	gTable.sourceTableKeyFieldList = [];
	while (rs.next()) {
		fld = rs.getString(1);
		gTable.sourceTableKey += ('\"' + fld + '\"' + ' ');		
		gTable.sourceTableKeyFieldList[keyCount] = fld;
		keyCount = keyCount + 1;
	}
	gTable.sourceTableKey = gTable.sourceTableKey.trim();
	gTable.sourceTableKey = gTable.sourceTableKey.replace(/ /g, ', '); // global replace space, with space comma
	
	log('=== Table Information ===================================================');
	log('Table Metadata Query (template): ' + query);
	if (keyCount > 0){
	    log('Table Key: ' + gTable.sourceTableKey);
	    gTable.sourceTableKeyFieldCount = keyCount;
	    // not logging key field list, but could
	} else {
		log('*** ERROR: table ' + gTable.sourceTable + ' does not exist, or does not have a primary key');	
		throw {name : "TableError", };
	}
	
	//--------------------------------------------------------
	// Read source table data proper 
	//--------------------------------------------------------
	query = prepareQueryForMainRead();
	log('Main Select Query: ' + query);	
	connSelect = $.db.getConnection();
	pstmtSelect = connSelect.prepareStatement(query);
	// Store results
	gTable.resultSet = pstmtSelect.executeQuery();
	gTable.sourceTableKeyCount = keyCount;
	gTable.resultSetFieldCount = keyCount + 2;  // number of fields in key plus 2 for lat lon
	log(' ');
}

// Prepare metadata selection query, returns query string (with params as ? to be filled later)
function prepareQueryForMetadata() {
	var select = 'SELECT \"COLUMN_NAME\"';
	var from = ' FROM \"SYS\".\"CONSTRAINTS\"';
	var where = ' WHERE \"SCHEMA_NAME\" = ? AND \"TABLE_NAME\" = ?';
	var orderby = ' ORDER BY \"COLUMN_NAME\"';
	var query = select + from + where + orderby;
	return query;
}

// Prepare main selection query, returns query string  (no params possible here)
function prepareQueryForMainRead() {
	var select = 'SELECT TOP ' + gConfig.maxRecords + ' ' + gTable.sourceTableKey + ', \"' + gTable.sourceFieldLat + '\", \"' + gTable.sourceFieldLon + '\"';
	var from = ' FROM \"' + gTable.sourceSchema + '\".\"' + gTable.sourceTable + '\"';
	var where = ' WHERE \"' + gTable.targetFieldThatIsBlank  + '\" IS NULL';
	var orderby = ' ORDER BY ' + gTable.sourceTableKey;
	var query = select + from + where + orderby;
	return query;
}

// Prepare update statement, returns query string (with params as ? to be filled later)
function prepareQueryForUpdate() {
	//--------------------------------------------------------
	// The UPDATE clause
	//--------------------------------------------------------
	var qupdate = 'UPDATE \"' + gTable.sourceSchema + '\".\"' + gTable.sourceTable + '\"';
	
	//--------------------------------------------------------
	// The SET clause
	//--------------------------------------------------------
	var i = 0;
	var qset = ' SET ';
	for (i = 0; i < gTable.targetFieldCount - 1; i++) {
		qset += ( '\"' + gTable.targetFieldnames[i] + '\" = ?' );
		qset += ', ';
	}
	i = gTable.targetFieldCount - 1; // last entry doesn't get trailing separator
	qset += ( '\"' + gTable.targetFieldnames[i] + '\" = ?' );
	
	//--------------------------------------------------------
	// The WHERE clause
	//--------------------------------------------------------
	var qwhere = ' WHERE ';
	for (i = 0; i < gTable.sourceTableKeyFieldCount - 1; i++) {
		qwhere += ( '\"' + gTable.sourceTableKeyFieldList[i] + '\" = ?' );
		qwhere += ' AND ';
	}
	i = gTable.sourceTableKeyFieldCount - 1; // last entry doesn't get trailing separator
	qwhere += ( '\"' + gTable.sourceTableKeyFieldList[i] + '\" = ?' );
	
	var queryUpdate = qupdate + qset + qwhere;
	return queryUpdate;		
}

// Main processing, this loops over the result set of data, calls the geocode API to get the new data
// and writes it back to the database one record at a time.
function mainProcessing() {
	var rs = gTable.resultSet;
	var i = 0;
	var keyValue = '';
	var remainingTime = 0;
	var overallDuration = 0;
	// record-level vars
	var currentRecord = {
		// Current record-related working vars
		sourceFieldValues : '',  // all field values as string for logging	
		lat : 0,
		lon : 0,
		timeStart : 0,
		timeEnd : 0,
		duration : 0,
		keyValues : [],      // key field values, used in update call
	    addressData : null,  // the object retuned by the geo API call with properties containing address values we want
	};
	
	log('=== Main Processing =====================================================');	
	// iterating a recordset is a one-way ticket, so have to do all processing per record
	while (rs.next()) {
		//--------------------------------------------------------
		// Main process per rs record: call geocode API, write to DB
		//---------------------------------------------------------
		// Clear previous record
		currentRecord.sourceFieldValues = '';
		currentRecord.lat = 0;
		currentRecord.lon = 0;
		currentRecord.timeStart = 0;
		currentRecord.timeEnd = 0,
		currentRecord.duration = 0,
		currentRecord.keyValues = [];
		currentRecord.addressData = null;
		
		// Examine the key values, for logging and later on they used in the Update statement
		for (i = 0; i < gTable.resultSetFieldCount; i++) {
			keyValue = rs.getString(i + 1);
			currentRecord.sourceFieldValues = currentRecord.sourceFieldValues +  ' ' + keyValue;
			currentRecord.keyValues[i] = keyValue;
		}		
		log('Source record (selected fields): ' + currentRecord.sourceFieldValues);
		
		// Get lat lon from source record
		currentRecord.lat = parseFloat(rs.getString(gTable.sourceTableKeyCount + 1));
		currentRecord.lon = parseFloat(rs.getString(gTable.sourceTableKeyCount + 2));
		//log('Current record lat: ' + currentRecord.lat.toString() + ' lon: ' + currentRecord.lon.toString());
	
		// Timer to ensure we don't swamp the google API and get banned
		currentRecord.timeStart = 0;
		currentRecord.timeEnd = 0;	
		currentRecord.timeStart = new Date().getTime();
		
		//--------------------------------------------------------
		// Call our library that wraps the Google service
		// The addressData object that is returned is guaranteed to contain these properties:
		//   country, administrative_area_level_1, _2, _3, postal_code
		//--------------------------------------------------------
		currentRecord.addressData = GEOAPIGOOGLE.reverseGeocode(currentRecord.lat, currentRecord.lon);
		log('Reverse Geocode Results: ' + JSON.stringify(currentRecord.addressData)); 
		
		//--------------------------------------------------------
		// Write back to database
		//--------------------------------------------------------
		// query with ? params
		var queryUpdate = prepareQueryForUpdate();
		log('Record Update Query (template): ' + queryUpdate);	
		var connUpdate = $.db.getConnection();
		var cstmtUpdate = connUpdate.prepareCall(queryUpdate);
		// eg UPDATE "GEODATAENRICH"."testtable01" SET "COUNTRY" = ?, "ZIP" = ? WHERE "KEYREF" = ? AND "KEYYEAR" = ?
		// parameter replacement for SET
		for (i = 0; i < gTable.targetFieldCount; i++) {
			var s = currentRecord.addressData[gTable.targetProperties[i]];
			cstmtUpdate.setString(i + 1, s);
		}
		// parameter replacement for WHERE
		for (i = 0; i < gTable.sourceTableKeyFieldCount; i++) {
			var kfv = currentRecord.keyValues[i]; 
			cstmtUpdate.setString(i + gTable.targetFieldCount + 1, kfv);  // note counter increments from key count
		}		
		if (gConfig.simulate === 'notactive') {
			cstmtUpdate.execute();
			connUpdate.commit();
		} else {
			log('In simulate mode, no table update done.');
		}
		connUpdate.close();
		
		//--------------------------------------------------------
		// Wait until duration reached before allowing loop to continue
		//--------------------------------------------------------
		currentRecord.timeEnd = new Date().getTime();
		currentRecord.duration = currentRecord.timeEnd - currentRecord.timeStart;
		log('Execution Time (ms): ' + currentRecord.duration);
		remainingTime = gConfig.minDelayMS - currentRecord.duration;
		if (remainingTime > 50) {
			log('  sleeping...');
			// This blocks CPU, not ideal at all, but easier to implement than a callback in this case
			sleep(remainingTime);
			overallDuration = ((new Date().getTime()) - currentRecord.timeStart);
			log('  overall duration: ' + overallDuration + ' ms');
		}		
		log(' ');
		gRecsProcessed++;
	};
}

// This blocks CPU, not ideal but works
function sleep(milliseconds) {
	var start = new Date().getTime();
	for (var i = 0; i < 1e7; i++) {
		if ((new Date().getTime() - start) > milliseconds) {
			break;
		};
	};
}

function finish() {
	$.response.contentType = "text/plain";
	if (gConfig.detailedLogging === 'active') {
		$.response.setBody(gLog);
	} else {
		$.response.setBody('Done. Processed ' + gRecsProcessed + ' records.');
	}
};
