/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.server;

import java.util.Map;
import org.locationtech.geowave.core.store.server.ServerOpConfig.OptionProvider;

public class BasicOptionProvider implements OptionProvider {
  private final Map<String, String> options;

  public BasicOptionProvider(final Map<String, String> options) {
    this.options = options;
  }

  @Override
  public Map<String, String> getOptions(final Map<String, String> existingOptions) {
    return options;
  }
}
