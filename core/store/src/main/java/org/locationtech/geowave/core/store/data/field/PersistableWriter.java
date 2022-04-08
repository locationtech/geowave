/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field;

import org.locationtech.geowave.core.index.persist.Persistable;

public class PersistableWriter<F extends Persistable> implements FieldWriter<F> {

  @Override
  public byte[] writeField(final F fieldValue) {
    if (fieldValue == null) {
      return new byte[0];
    }
    return fieldValue.toBinary();
  }

}
