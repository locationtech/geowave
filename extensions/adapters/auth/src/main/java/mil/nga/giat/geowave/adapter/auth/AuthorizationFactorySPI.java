package mil.nga.giat.geowave.adapter.auth;

import java.net.URL;

/**
 * Creates an authorization provider with the given URL.
 * 
 * 
 */
public interface AuthorizationFactorySPI
{
	/**
	 * 
	 * @param location
	 *            Any connection information to be interpreted by the provider.
	 * 
	 * @return
	 */
	AuthorizationSPI create(
			URL location );
}
