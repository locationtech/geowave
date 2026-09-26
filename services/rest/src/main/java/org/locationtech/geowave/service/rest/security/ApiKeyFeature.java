/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.security;

import jakarta.ws.rs.core.Feature;
import jakarta.ws.rs.core.FeatureContext;
import org.locationtech.geowave.service.rest.GeoWaveRestApplication;

/**
 * Turns API keys on when the api_key_db property names a SQLite database file. The application
 * fails to start if that database cannot be used, rather than run unprotected.
 */
public class ApiKeyFeature implements Feature {
  @Override
  public boolean configure(final FeatureContext context) {
    final Object dbFile =
        context.getConfiguration().getProperty(GeoWaveRestApplication.API_KEY_DB_PROPERTY);
    if ((dbFile == null) || dbFile.toString().trim().isEmpty()) {
      return false;
    }
    context.register(new ApiKeyFilter(new SQLiteApiKeyStore(dbFile.toString().trim())));
    return true;
  }
}
