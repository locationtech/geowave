/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.security.oauth2;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.OAuth2Request;
import org.springframework.security.oauth2.provider.token.DefaultAccessTokenConverter;
import org.springframework.security.oauth2.provider.token.UserAuthenticationConverter;
import org.springframework.util.StringUtils;

/** Copied the DefaultAccessTokenConverter and modified for Facebook token details. */
public class FacebookAccessTokenConverter extends DefaultAccessTokenConverter {

  // private UserAuthenticationConverter userTokenConverter;
  private Collection<? extends GrantedAuthority> defaultAuthorities;

  public FacebookAccessTokenConverter() {}

  /**
   * Converter for the part of the data in the token representing a user.
   *
   * @param userTokenConverter the userTokenConverter to set
   */
  @Override
  public final void setUserTokenConverter(final UserAuthenticationConverter userTokenConverter) {}

  public void setDefaultAuthorities(final String[] defaultAuthorities) {
    this.defaultAuthorities =
        AuthorityUtils.commaSeparatedStringToAuthorityList(
            StringUtils.arrayToCommaDelimitedString(defaultAuthorities));
  }

  @Override
  public OAuth2Authentication extractAuthentication(final Map<String, ?> map) {
    final Map<String, String> parameters = new HashMap<>();
    final Set<String> scope = parseScopes(map);
    final Object principal = map.get("name");
    final Authentication user =
        new UsernamePasswordAuthenticationToken(principal, "N/A", defaultAuthorities);
    final String clientId = (String) map.get(CLIENT_ID);
    parameters.put(CLIENT_ID, clientId);
    final Set<String> resourceIds =
        new LinkedHashSet<>(
            map.containsKey(AUD) ? (Collection<String>) map.get(AUD)
                : Collections.<String>emptySet());
    final OAuth2Request request =
        new OAuth2Request(parameters, clientId, null, true, scope, resourceIds, null, null, null);
    return new OAuth2Authentication(request, user);
  }

  private Set<String> parseScopes(final Map<String, ?> map) {
    // Parse scopes by comma
    final Object scopeAsObject = map.containsKey(SCOPE) ? map.get(SCOPE) : EMPTY;
    final Set<String> scope = new LinkedHashSet<>();
    if (String.class.isAssignableFrom(scopeAsObject.getClass())) {
      final String scopeAsString = (String) scopeAsObject;
      Collections.addAll(scope, scopeAsString.split(","));
    } else if (Collection.class.isAssignableFrom(scopeAsObject.getClass())) {
      final Collection<String> scopes = (Collection<String>) scopeAsObject;
      scope.addAll(scopes);
    }
    return scope;
  }
}
