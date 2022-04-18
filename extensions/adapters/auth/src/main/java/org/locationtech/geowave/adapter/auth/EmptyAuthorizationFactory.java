/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.auth;

import java.net.URL;

public class EmptyAuthorizationFactory implements AuthorizationFactorySPI {
  @Override
  public AuthorizationSPI create(final URL url) {
    return new EmptyAuthorizationProvider();
  }

  @Override
  public String toString() {
    return "empty";
  }
}
