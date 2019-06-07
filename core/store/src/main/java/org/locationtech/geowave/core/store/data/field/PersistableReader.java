/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.field;

import org.locationtech.geowave.core.index.persist.Persistable;
import org.locationtech.geowave.core.index.persist.PersistableFactory;

public class PersistableReader<F extends Persistable> implements FieldReader<F> {
  private final short classId;

  public PersistableReader(final short classId) {
    super();
    this.classId = classId;
  }

  @Override
  public F readField(final byte[] fieldData) {
    final F newInstance = (F) PersistableFactory.getInstance().newInstance(classId);
    newInstance.fromBinary(fieldData);
    return newInstance;
  }

}
