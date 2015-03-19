package mil.nga.giat.geowave.vector.auth;

import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;

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
		Authentication auth = SecurityContextHolder.getContext().getAuthentication();
		return new String[0];
	}

}
