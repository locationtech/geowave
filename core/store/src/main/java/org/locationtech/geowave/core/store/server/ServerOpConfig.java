/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.server;

import java.util.EnumSet;
import java.util.Map;

public class ServerOpConfig {
  private final EnumSet<ServerOpScope> scopes;
  private final int serverOpPriority;
  private final String serverOpName;
  private final String serverOpClass;
  private final OptionProvider optionProvider;

  public ServerOpConfig(
      final EnumSet<ServerOpScope> scopes,
      final int serverOpPriority,
      final String serverOpName,
      final String serverOpClass,
      final OptionProvider optionProvider) {
    this.scopes = scopes;
    this.serverOpPriority = serverOpPriority;
    this.serverOpName = serverOpName;
    this.serverOpClass = serverOpClass;
    this.optionProvider = optionProvider;
  }

  public EnumSet<ServerOpScope> getScopes() {
    return scopes;
  }

  public int getServerOpPriority() {
    return serverOpPriority;
  }

  public String getServerOpName() {
    return serverOpName;
  }

  public String getServerOpClass() {
    return serverOpClass;
  }

  public Map<String, String> getOptions(final Map<String, String> existingOptions) {
    return optionProvider.getOptions(existingOptions);
  }

  public static interface OptionProvider {
    public Map<String, String> getOptions(Map<String, String> existingOptions);
  }

  public static enum ServerOpScope {
    MAJOR_COMPACTION, MINOR_COMPACTION, SCAN
  }
}
