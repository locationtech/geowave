/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.auth;

import java.util.List;

/** Used for Json based authorization data sets. */
public class AuthorizationEntry {
  String userName;
  List<String> authorizations;

  protected String getUserName() {
    return userName;
  }

  protected void setUserName(final String userName) {
    this.userName = userName;
  }

  protected List<String> getAuthorizations() {
    return authorizations;
  }

  protected void setAuthorizations(final List<String> authorizations) {
    this.authorizations = authorizations;
  }
}
