package mil.nga.giat.geowave.adapter.auth;

import java.net.URL;

/**
 * Stores authorization data in a json file. Format: { "authorizationSet" : {
 * "fred" : ["auth1","auth2"], "barney" : ["auth1","auth3"] } }
 * 
 * @author rwgdrummer
 * 
 */
public class JsonFileAuthorizationFactory implements
		AuthorizationFactorySPI
{
	public AuthorizationSPI create(
			URL url ) {
		return new JsonFileAuthorizationProvider(
				url);
	}

	@Override
	public String toString() {
		return "jsonFile";
	}

}
