package mil.nga.giat.geowave.adapter.auth;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AuthorizationSet
{
	Map<String, List<String>> authorizationSet = new HashMap<String, List<String>>();

	protected Map<String, List<String>> getAuthorizationSet() {
		return authorizationSet;
	}

	protected void setAuthorizationSet(
			Map<String, List<String>> authorizationSet ) {
		this.authorizationSet = authorizationSet;
	}

	public List<String> findAuthorizationsFor(
			String name ) {
		List<String> r = this.authorizationSet.get(name);
		return r == null ? new LinkedList<String>() : r;
	}

}
