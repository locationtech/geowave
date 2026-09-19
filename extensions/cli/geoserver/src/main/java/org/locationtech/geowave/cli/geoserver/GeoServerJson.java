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
import com.fasterxml.jackson.core.JsonGenerator;
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
   * Two spaces for objects and for arrays, and <code>": "</code> between a field and its value.
   * Jackson's default printer puts array elements on one line and writes <code>" : "</code>, either
   * of which would change output that people have been grepping since this CLI was written.
   */
  private static class GeoServerPrettyPrinter extends DefaultPrettyPrinter {
    private static final long serialVersionUID = 1L;

    GeoServerPrettyPrinter() {
      _arrayIndenter = DefaultIndenter.SYSTEM_LINEFEED_INSTANCE;
    }

    private GeoServerPrettyPrinter(final GeoServerPrettyPrinter base) {
      super(base);
    }

    @Override
    public DefaultPrettyPrinter createInstance() {
      return new GeoServerPrettyPrinter(this);
    }

    @Override
    public void writeObjectFieldValueSeparator(final JsonGenerator g) throws IOException {
      g.writeRaw(": ");
    }
  }

  private static final ObjectWriter PRETTY = MAPPER.writer(new GeoServerPrettyPrinter());

  private GeoServerJson() {}

  public static ObjectNode object() {
    return MAPPER.createObjectNode();
  }

  public static ArrayNode array() {
    return MAPPER.createArrayNode();
  }

  /**
   * Parse a REST response body. Anything that is not a JSON object is an error rather than an empty
   * result: json-lib threw on an empty or truncated body, and letting one through as {} would turn
   * a failed lookup into a command that prints nothing and exits successfully.
   */
  public static ObjectNode parse(final Object entity) {
    final JsonNode node;
    try {
      node = MAPPER.readTree(String.valueOf(entity));
    } catch (final IOException e) {
      throw new IllegalArgumentException("GeoServer response is not JSON: " + entity, e);
    }
    if ((node == null) || !node.isObject()) {
      throw new IllegalArgumentException("GeoServer response is not a JSON object: " + entity);
    }
    return (ObjectNode) node;
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
