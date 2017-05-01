package mil.nga.giat.geowave.adapter.auth;

import java.util.List;

/**
 * Used for Json based authorization data sets.
 * 
 * @author rwgdrummer
 * 
 */
public class AuthorizationEntry
{
	String userName;
	List<String> authorizations;

	protected String getUserName() {
		return userName;
	}

	protected void setUserName(
			String userName ) {
		this.userName = userName;
	}

	protected List<String> getAuthorizations() {
		return authorizations;
	}

	protected void setAuthorizations(
			List<String> authorizations ) {
		this.authorizations = authorizations;
	}

}
