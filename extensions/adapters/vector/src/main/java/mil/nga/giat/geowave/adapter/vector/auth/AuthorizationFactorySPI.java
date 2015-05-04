package mil.nga.giat.geowave.adapter.vector.auth;

import java.net.URL;

/**
 * Creates an authorization provider with the given URL.
 * 
 * @author rwgdrummer
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
