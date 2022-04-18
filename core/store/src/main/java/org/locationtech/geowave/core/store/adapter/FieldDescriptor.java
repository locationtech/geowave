/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.adapter;

import java.util.Set;
import org.locationtech.geowave.core.index.IndexDimensionHint;
import org.locationtech.geowave.core.index.persist.Persistable;

/**
 * Describes an adapter field, including the field name, the class of the field, and any index
 * hints. Each field may have one or more index hints that can be used to help GeoWave determine how
 * the adapter should be mapped to any arbitrary index.
 *
 * @param <T> the adapter field type
 */
public interface FieldDescriptor<T> extends Persistable {

  /**
   * @return the class of the data represented by this field
   */
  Class<T> bindingClass();

  /**
   * @return the name of the field
   */
  String fieldName();

  /**
   * @return the set of index hints that this field contains
   */
  Set<IndexDimensionHint> indexHints();
}
