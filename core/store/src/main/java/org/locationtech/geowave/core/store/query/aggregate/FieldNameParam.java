/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.query.aggregate;

import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.Persistable;

public class FieldNameParam implements Persistable {
  // TODO we can also include a requested CRS in case we want to reproject
  // (although it seemingly can just as easily be done on the resulting
  // envelope rather than per feature)
  private String fieldName;

  public FieldNameParam() {
    this(null);
  }

  public FieldNameParam(final String fieldName) {
    this.fieldName = fieldName;
  }

  @Override
  public byte[] toBinary() {
    if ((fieldName == null) || fieldName.isEmpty()) {
      return new byte[0];
    }
    return StringUtils.stringToBinary(fieldName);
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    if (bytes.length > 0) {
      fieldName = StringUtils.stringFromBinary(bytes);
    } else {
      fieldName = null;
    }
  }

  public boolean isEmpty() {
    return (fieldName == null) || fieldName.isEmpty();
  }

  public String getFieldName() {
    return fieldName;
  }
}
