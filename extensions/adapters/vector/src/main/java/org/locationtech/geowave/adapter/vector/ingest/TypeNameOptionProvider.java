/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.ingest;

import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;
import com.beust.jcommander.Parameter;

public class TypeNameOptionProvider implements Persistable {
  @Parameter(
      names = "--typename",
      description = "A comma-delimitted set of typenames to ingest, feature types matching the specified typenames will be ingested (optional, by default all types will be ingested)")
  private String typename = null;

  private String[] typenames = null;

  public String getTypeName() {
    return typename;
  }

  public boolean typeNameMatches(final String typeName) {
    String[] internalTypenames;
    synchronized (this) {
      if (typenames == null) {
        typenames = typename.split(",");
      }
      internalTypenames = typenames;
    }
    for (final String t : internalTypenames) {
      if (t.equalsIgnoreCase(typeName)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public byte[] toBinary() {
    if (typename == null) {
      return new byte[] {};
    }
    return StringUtils.stringToBinary(typename);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    if (bytes.length > 0) {
      typename = StringUtils.stringFromBinary(bytes);
    } else {
      typename = null;
    }
  }
}
