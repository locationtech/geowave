/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.data.visibility;

import java.io.IOException;
import java.util.Iterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Determines the visibility of a field by looking it up in a JSON object that's parsed from a
 * specified visibility field.
 *
 * <p> Example: { "geometry" : "S", "eventName": "TS"}
 *
 * <p> Json attributes can also be regular expressions, matching more than one field name.
 *
 * <p> Example: { "geo.*" : "S", ".*" : "TS"}.
 *
 * <p> The order of the expression must be considered if one expression is more general than
 * another, as shown in the example. The expression ".*" matches all attributes. The more specific
 * expression "geo.*" must be ordered first.
 */
public class JsonFieldLevelVisibilityHandler extends FieldLevelVisibilityHandler {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(JsonFieldLevelVisibilityHandler.class);
  private final ObjectMapper mapper = new ObjectMapper();

  public JsonFieldLevelVisibilityHandler() {}

  public JsonFieldLevelVisibilityHandler(final String visibilityAttribute) {
    super(visibilityAttribute);
  }

  @Override
  public String translateVisibility(final Object visibilityObject, final String fieldName) {
    if (visibilityObject == null) {
      return null;
    }
    try {
      final JsonNode attributeMap = mapper.readTree(visibilityObject.toString());
      final JsonNode field = attributeMap.get(fieldName);
      if ((field != null) && field.isValueNode()) {
        return field.textValue();
      }
      final Iterator<String> attNameIt = attributeMap.fieldNames();
      while (attNameIt.hasNext()) {
        final String attName = attNameIt.next();
        if (fieldName.matches(attName)) {
          final JsonNode attNode = attributeMap.get(attName);
          if (attNode == null) {
            LOGGER.error(
                "Cannot parse visibility expression, JsonNode for attribute "
                    + attName
                    + " was null");
            return null;
          }
          return attNode.textValue();
        }
      }
    } catch (IOException | NullPointerException e) {
      LOGGER.error("Cannot parse visibility expression " + visibilityObject.toString(), e);
    }
    return null;
  }
}
