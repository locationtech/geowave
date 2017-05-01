package mil.nga.giat.geowave.adapter.auth;

/**
 * A provider that looks up authorizations given a user name.
 * 
 * Created by {@link AuthorizationFactorySPI#create(java.net.URL)}
 * 
 * @author rwgdrummer
 * 
 */
public interface AuthorizationSPI
{
	public String[] getAuthorizations();
}
