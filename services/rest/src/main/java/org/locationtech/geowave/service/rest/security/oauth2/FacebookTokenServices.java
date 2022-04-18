/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.security.oauth2;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.security.oauth2.common.exceptions.InvalidTokenException;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.token.AccessTokenConverter;
import org.springframework.security.oauth2.provider.token.RemoteTokenServices;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.DefaultResponseErrorHandler;
import org.springframework.web.client.RestOperations;
import org.springframework.web.client.RestTemplate;

public class FacebookTokenServices extends RemoteTokenServices {
  protected final Log logger = LogFactory.getLog(getClass());

  private RestOperations restTemplate;

  private String checkTokenEndpointUrl;

  private String tokenName = "token";

  private AccessTokenConverter tokenConverter = new FacebookAccessTokenConverter();

  public FacebookTokenServices() {
    restTemplate = new RestTemplate();
    ((RestTemplate) restTemplate).setErrorHandler(new DefaultResponseErrorHandler() {
      @Override
      // Ignore 400
      public void handleError(final ClientHttpResponse response) throws IOException {
        if (response.getRawStatusCode() != 400) {
          super.handleError(response);
        }
      }
    });
  }

  @Override
  public void setRestTemplate(final RestOperations restTemplate) {
    this.restTemplate = restTemplate;
  }

  @Override
  public void setCheckTokenEndpointUrl(final String checkTokenEndpointUrl) {
    this.checkTokenEndpointUrl = checkTokenEndpointUrl;
  }

  @Override
  public void setAccessTokenConverter(final AccessTokenConverter accessTokenConverter) {
    tokenConverter = accessTokenConverter;
  }

  @Override
  public void setTokenName(final String tokenName) {
    this.tokenName = tokenName;
  }

  @Override
  public OAuth2Authentication loadAuthentication(final String accessToken)
      throws AuthenticationException, InvalidTokenException {

    final MultiValueMap<String, String> formData = new LinkedMultiValueMap<>();
    formData.add(tokenName, accessToken);

    final HttpHeaders headers = new HttpHeaders();
    String req = "";
    try {
      req = checkTokenEndpointUrl + "?access_token=" + URLEncoder.encode(accessToken, "UTF-8");
    } catch (final UnsupportedEncodingException e) {
      logger.error("Unsupported encoding", e);
    }

    final Map<String, Object> map = getForMap(req, formData, headers);

    if (map.containsKey("error")) {
      logger.debug("check_token returned error: " + map.get("error"));
      throw new InvalidTokenException(accessToken);
    }

    return tokenConverter.extractAuthentication(map);
  }

  @Override
  public OAuth2AccessToken readAccessToken(final String accessToken) {
    throw new UnsupportedOperationException("Not supported: read access token");
  }

  private Map<String, Object> getForMap(
      final String path,
      final MultiValueMap<String, String> formData,
      final HttpHeaders headers) {
    if (headers.getContentType() == null) {
      headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
    }
    @SuppressWarnings("rawtypes")
    final Map map =
        restTemplate.exchange(
            path,
            HttpMethod.GET,
            new HttpEntity<>(formData, headers),
            Map.class).getBody();
    @SuppressWarnings("unchecked")
    final Map<String, Object> result = map;
    return result;
  }
}
