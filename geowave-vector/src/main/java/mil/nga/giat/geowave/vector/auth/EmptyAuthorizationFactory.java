package mil.nga.giat.geowave.vector.auth;

import java.net.URL;

public class EmptyAuthorizationFactory implements
		AuthorizationFactorySPI
{
	public AuthorizationSPI create(
			URL url ) {
		return new EmptyAuthorizationProvider();
	}

	@Override
	public String toString() {
		return "empty";
	}

}
