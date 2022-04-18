/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.auth;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class AuthorizationSet {
  Map<String, List<String>> authorizationSet = new HashMap<>();

  protected Map<String, List<String>> getAuthorizationSet() {
    return authorizationSet;
  }

  protected void setAuthorizationSet(final Map<String, List<String>> authorizationSet) {
    this.authorizationSet = authorizationSet;
  }

  public List<String> findAuthorizationsFor(final String name) {
    final List<String> r = authorizationSet.get(name);
    return r == null ? new LinkedList<>() : r;
  }
}
