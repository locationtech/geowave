/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import java.io.IOException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * JSON handling shared by the GeoServer REST client and the commands that print its responses.
 */
public class GeoServerJson {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Two spaces for objects and for arrays. Jackson's default printer puts array elements on one
   * line, which is not what this CLI has always printed.
   */
  private static final ObjectWriter PRETTY =
      MAPPER.writer(
          new DefaultPrettyPrinter().withArrayIndenter(DefaultIndenter.SYSTEM_LINEFEED_INSTANCE));

  private GeoServerJson() {}

  public static ObjectNode object() {
    return MAPPER.createObjectNode();
  }

  public static ArrayNode array() {
    return MAPPER.createArrayNode();
  }

  /**
   * Parse a REST response entity. By the time an entity reaches here it is always the JSON string
   * the client read off the wire.
   */
  public static ObjectNode parse(final Object entity) {
    try {
      final JsonNode node = MAPPER.readTree(String.valueOf(entity));
      return node.isObject() ? (ObjectNode) node : object();
    } catch (final IOException e) {
      throw new IllegalArgumentException("GeoServer response is not JSON: " + entity, e);
    }
  }

  public static String pretty(final JsonNode node) {
    try {
      return PRETTY.writeValueAsString(node);
    } catch (final JsonProcessingException e) {
      return String.valueOf(node);
    }
  }

  /** The text of a field, or null when it is absent. */
  public static String text(final JsonNode node, final String field) {
    final JsonNode value = (node == null) ? null : node.get(field);
    return ((value == null) || value.isNull()) ? null : value.asText();
  }
}
