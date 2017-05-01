package mil.nga.giat.geowave.adapter.auth;

/**
 * No authorization provided.
 * 
 * @author rwgdrummer
 * 
 */

public class EmptyAuthorizationProvider implements
		AuthorizationSPI
{

	public EmptyAuthorizationProvider() {}

	@Override
	public String[] getAuthorizations() {
		return new String[0];
	}

}
