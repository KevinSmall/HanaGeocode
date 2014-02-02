//---------------------------------------------------------------------------------------------
// PUBLIC methods
//---------------------------------------------------------------------------------------------

//---------------------------------------------------------------------------------------------
// function reverseGeocode(lat, lon)
// Takes a latitude and longitude value, returns an addressResults object containing these
// properties (see https://developers.google.com/maps/documentation/geocoding): 
//     country
//     administrative_area_level_1
//     administrative_area_level_2
//     administrative_area_level_3
//     postal_code
//     status (see https://developers.google.com/maps/documentation/geocoding/#StatusCodes)
//---------------------------------------------------------------------------------------------
function reverseGeocode(lat, lon) {	

	// Init address data to remove all existing properties 
	gAddressResults = {};
	
	// Call Google reverse geocoding API
	var dest = $.net.http.readDestination("geodataenrich.services", "geocodeApiGoogleDest");
	var client = new $.net.http.Client();
	var req = new $.web.WebRequest($.net.http.GET,"?latlng=" + lat + "," + lon + "&sensor=false");
	client.request(req, dest);
	var response = client.getResponse();

	// Parse results, this adds properties to gAddressResults as it goes
	var geoData = JSON.parse(response.body.asString());	
	var i = 0;
	for (i = 0; i < geoData.results.length; i++) {
		geoTraverse(geoData.results[i].address_components);
	}
	
	// Add any properties that we have been unable to find
	if (!gAddressResults.hasOwnProperty('country')) {
        gAddressResults['country'] = 'Not known';
    }
	if (!gAddressResults.hasOwnProperty('administrative_area_level_1')) {
        gAddressResults['administrative_area_level_1'] = 'Not known';
    }
	if (!gAddressResults.hasOwnProperty('administrative_area_level_2')) {
        gAddressResults['administrative_area_level_2'] = 'Not known';
    }
	if (!gAddressResults.hasOwnProperty('administrative_area_level_3')) {
        gAddressResults['administrative_area_level_3'] = 'Not known';
    }
	if (!gAddressResults.hasOwnProperty('postal_code')) {
        gAddressResults['postal_code'] = 'Not known';
    }
	
	// Status
	var status = geoData.status || "Status unknown";
	gAddressResults.status = status;
	
	return gAddressResults;
}

// Takes an address and returns a latitude longitude pair
function geocode(address) {	
	// not implemented
}

//---------------------------------------------------------------------------------------------
// PRIVATE methods (think of all below as private, even though technically they are not)
//---------------------------------------------------------------------------------------------
// Push a property key value pair to the addressResults object 
function pushAddressResult(addKey, addValue) {
    // keep only longest of the postcodes
    var existingPostcodeLength = 0;
    // adding a postcode when we have already got one
    if (addKey === 'postal_code' && gAddressResults.hasOwnProperty('postal_code')) {
        existingPostcodeLength = gAddressResults['postal_code'].length;
        if (addValue.length <= existingPostcodeLength)
        {            
            return;
        } else {
            gAddressResults[addKey] = addValue;
        }       
    }      
    // only add property if we dont have it already
    if (!gAddressResults.hasOwnProperty(addKey)) {
        gAddressResults[addKey] = addValue;
    }
}

// Recursively traverse the results of a google geocode call, picking out the address parts we need
// and storing them as properties in the addressResults object
function geoTraverse(o) {
	var type = typeof o;            
    var leaf = '';
	gDepth = gDepth + 1;
    if (type === "object") {
        for (var key in o) {        	
            //console.log("depth: %i, object: %O", depth, o, " has key:" + key);
            if (key.toString() === 'long_name'){
                gStoreLastLongname = o[key];
            }            
            geoTraverse(o[key]);
        }
    } else {
        //console.log("depth: %i, leaf: %O", depth, o);         
        leaf = o.toString();
    	if (leaf === 'country' || 
            leaf === 'administrative_area_level_1' ||    		
            leaf === 'administrative_area_level_2' ||    		
            leaf === 'administrative_area_level_3' ||
            leaf === 'postal_code'){    		
            pushAddressResult(leaf, gStoreLastLongname);
    	}                 
    }
    gDepth = gDepth - 1;
}

var gAddressResults = {};
var gDepth = 0;
var gStoreLastLongname = "emptyLongname";