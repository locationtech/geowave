/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
/** */
package org.locationtech.geowave.core.cli.utils;

import org.apache.commons.beanutils.ConvertUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

/** Used for general purpose value conversion via appache commons ConvertUtils */
public class ValueConverter {
  private static Logger LOGGER = LoggerFactory.getLogger(ValueConverter.class);

  /** Private constructor to prevent accidental instantiation */
  private ValueConverter() {}

  /**
   * Convert value into the specified type
   *
   * @param <X> Class to convert to
   * @param value Value to convert from
   * @param targetType Type to convert into
   * @return The converted value
   */
  @SuppressWarnings("unchecked")
  public static <X> X convert(final Object value, final Class<X> targetType) {
    // HP Fortify "Improper Output Neutralization" false positive
    // What Fortify considers "user input" comes only
    // from users with OS-level access anyway
    LOGGER.trace("Attempting to convert " + value + " to class type " + targetType);
    if (value != null) {
      // if object is already in intended target type, no need to convert
      // it, just return as it is
      if (value.getClass() == targetType) {
        return (X) value;
      }

      if ((value.getClass() == JSONObject.class) || (value.getClass() == JSONArray.class)) {
        return (X) value;
      }
    }

    final String strValue = String.valueOf(value);
    final Object retval = ConvertUtils.convert(strValue, targetType);
    return (X) retval;
  }
}
