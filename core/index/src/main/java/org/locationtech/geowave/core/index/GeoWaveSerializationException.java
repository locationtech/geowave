/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

public class GeoWaveSerializationException extends RuntimeException {
  private static final long serialVersionUID = 7302723488358974170L;

  public GeoWaveSerializationException(final String message) {
    super(message);
  }

  public GeoWaveSerializationException(final Throwable cause) {
    super(cause);
  }

  public GeoWaveSerializationException(final String message, final Throwable cause) {
    super(message, cause);
  }

}
