/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.visibility;

import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;

/**
 * Basic implementation of a visibility handler to allow all access
 *
 * @param <RowType>
 * @param <FieldType>
 */
public class UnconstrainedVisibilityHandler<RowType, FieldType>
    implements FieldVisibilityHandler<RowType, FieldType> {

  @Override
  public byte[] getVisibility(
      final RowType rowValue,
      final String fieldName,
      final FieldType fieldValue) {
    return new byte[0];
  }
}
