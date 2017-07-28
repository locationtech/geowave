package mil.nga.giat.geowave.service.rest.security.oauth2;

import static org.apache.commons.lang3.StringUtils.EMPTY;

import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.OAuth2Request;
import org.springframework.security.oauth2.provider.token.DefaultAccessTokenConverter;
import org.springframework.security.oauth2.provider.token.UserAuthenticationConverter;
import org.springframework.util.StringUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * Copied the DefaultAccessTokenConverter and modified for Facebook token
 * details.
 */
public class FacebookAccessTokenConverter extends
		DefaultAccessTokenConverter
{

	// private UserAuthenticationConverter userTokenConverter;
	private Collection<? extends GrantedAuthority> defaultAuthorities;

	public FacebookAccessTokenConverter() {

	}

	/**
	 * Converter for the part of the data in the token representing a user.
	 *
	 * @param userTokenConverter
	 *            the userTokenConverter to set
	 */
	public final void setUserTokenConverter(
			UserAuthenticationConverter userTokenConverter ) {}

	public void setDefaultAuthorities(
			String[] defaultAuthorities ) {
		this.defaultAuthorities = AuthorityUtils.commaSeparatedStringToAuthorityList(StringUtils
				.arrayToCommaDelimitedString(defaultAuthorities));
	}

	public OAuth2Authentication extractAuthentication(
			Map<String, ?> map ) {
		Map<String, String> parameters = new HashMap<>();
		Set<String> scope = parseScopes(map);
		Object principal = map.get("name");
		Authentication user = new UsernamePasswordAuthenticationToken(
				principal,
				"N/A",
				defaultAuthorities);
		String clientId = (String) map.get(CLIENT_ID);
		parameters.put(
				CLIENT_ID,
				clientId);
		Set<String> resourceIds = new LinkedHashSet<>(
				map.containsKey(AUD) ? (Collection<String>) map.get(AUD) : Collections.<String> emptySet());
		OAuth2Request request = new OAuth2Request(
				parameters,
				clientId,
				null,
				true,
				scope,
				resourceIds,
				null,
				null,
				null);
		return new OAuth2Authentication(
				request,
				user);
	}

	private Set<String> parseScopes(
			Map<String, ?> map ) {
		// Parse scopes by comma
		Object scopeAsObject = map.containsKey(SCOPE) ? map.get(SCOPE) : EMPTY;
		Set<String> scope = new LinkedHashSet<>();
		if (String.class.isAssignableFrom(scopeAsObject.getClass())) {
			String scopeAsString = (String) scopeAsObject;
			Collections.addAll(
					scope,
					scopeAsString.split(","));
		}
		else if (Collection.class.isAssignableFrom(scopeAsObject.getClass())) {
			Collection<String> scopes = (Collection<String>) scopeAsObject;
			scope.addAll(scopes);
		}
		return scope;
	}
}