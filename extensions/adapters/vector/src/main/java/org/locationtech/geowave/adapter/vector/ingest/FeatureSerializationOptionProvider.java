/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.ingest;

import org.locationtech.geowave.core.index.persist.Persistable;
import com.beust.jcommander.Parameter;

public class FeatureSerializationOptionProvider implements Persistable {
  @Parameter(
      names = "--avro",
      description = "A flag to indicate whether avro feature serialization should be used")
  private boolean avro = false;

  public boolean isAvro() {
    return avro;
  }

  @Override
  public byte[] toBinary() {
    return new byte[] {avro ? (byte) 1 : (byte) 0};
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    if ((bytes != null) && (bytes.length > 0)) {
      if (bytes[0] == 1) {
        avro = true;
      }
    }
  }
}
