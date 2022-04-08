/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.api;

/**
 * An index on a single field of a data type.
 */
public interface AttributeIndex extends Index {

  /**
   * @return the attribute that is being indexed
   */
  String getAttributeName();

  /**
   * Provides a default name for an attribute index.
   *
   * @param typeName the data type that the attribute belongs to
   * @param attributeName the attribute that is being indexed
   * @return the default index name
   */
  public static String defaultAttributeIndexName(
      final String typeName,
      final String attributeName) {
    return typeName + "_" + attributeName + "_idx";
  }

}
