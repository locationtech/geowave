/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.security;

import java.io.Serializable;
import javax.servlet.ServletContext;
import org.springframework.web.context.ServletContextAware;

public abstract class GeoWaveBaseApiKeyDB implements Serializable, ServletContextAware {
  /** Base class for implementing ApiKey databases */
  static final long serialVersionUID = 1L;

  private transient ServletContext servletContext;

  public GeoWaveBaseApiKeyDB() {}

  public abstract void initApiKeyDatabase();

  public abstract boolean hasKey(String apiKey);

  /** Returns the username and associated key value. Must be in the form "name:key" */
  public abstract String getCurrentUserAndKey();

  @Override
  public void setServletContext(final ServletContext servletContext) {
    this.servletContext = servletContext;
  }
}
