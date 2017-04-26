package mil.nga.giat.geowave.adapter.auth;

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
