/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.security;

import java.io.IOException;
import javax.servlet.FilterChain;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.filter.GenericFilterBean;

public class GeoWaveApiKeySetterFilter extends GenericFilterBean {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveApiKeySetterFilter.class);

  /**
   * This class is only responsible for setting two servlet context attributes: "userName" and
   * "apiKey"
   */
  @Override
  public void doFilter(
      final ServletRequest request,
      final ServletResponse response,
      final FilterChain chain) throws IOException, ServletException {

    try {
      final ServletContext servletContext = getServletContext();
      final ApplicationContext ac =
          WebApplicationContextUtils.getWebApplicationContext(servletContext);
      final GeoWaveBaseApiKeyDB apiKeyDB = (GeoWaveBaseApiKeyDB) ac.getBean("apiKeyDB");
      final String userAndKey = apiKeyDB.getCurrentUserAndKey();

      if (!userAndKey.equals("")) {
        final String[] userAndKeyToks = userAndKey.split(":");
        servletContext.setAttribute("userName", userAndKeyToks[0]);
        servletContext.setAttribute("apiKey", userAndKeyToks[1]);
      }
    } catch (final Exception e) {
      return;
    } finally {
      chain.doFilter(request, response);
    }
  }
}
