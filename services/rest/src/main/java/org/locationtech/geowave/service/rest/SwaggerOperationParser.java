/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import java.util.List;
import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.locationtech.geowave.service.rest.field.RestField;
import org.locationtech.geowave.service.rest.field.RestFieldFactory;
import com.beust.jcommander.Parameters;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Reads a GeoWave CLI operation's fields for JCommander's @Parameter and @ParametersDelegate
 * annotations, and describes the operation as a Swagger 2.0 operation object for
 * {@link SwaggerApiParser}.
 */
public class SwaggerOperationParser<T> {
  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;

  private final ServiceEnabledCommand<T> operation;
  private final ObjectNode json;

  public SwaggerOperationParser(final ServiceEnabledCommand<T> op) {
    operation = op;
    json = parseParameters();
  }

  public ObjectNode getJsonObject() {
    return json;
  }

  public ObjectNode processField(
      final String name,
      final Class<?> type,
      final String description,
      final boolean required) {
    final ObjectNode param = NODES.objectNode();
    // every parameter is documented as a query parameter
    param.put("in", "query");

    final String swaggerType = getSwaggerType(type);
    if ("array".equals(swaggerType)) {
      param.put("type", swaggerType);
      param.putObject("items").put("type", getSwaggerType(type.getComponentType()));
    } else if ("enum".equals(swaggerType)) {
      // the descriptions of most enum fields already list the permitted values
      param.put("type", "string");
    } else {
      param.put("type", swaggerType);
    }

    if (!description.isEmpty()) {
      param.put("description", description);
    }
    param.put("name", name);
    param.put("required", required);
    return param;
  }

  private ObjectNode parseParameters() {
    final ObjectNode op = NODES.objectNode();
    op.put("operationId", operation.getId());

    final Parameters commandAnnotation = operation.getClass().getAnnotation(Parameters.class);
    op.put("description", commandAnnotation.commandDescription());

    final ArrayNode parameters = op.putArray("parameters");
    final List<RestField<?>> fields = RestFieldFactory.createRestFields(operation.getClass());
    for (final RestField<?> field : fields) {
      parameters.add(
          processField(
              field.getName(),
              field.getType(),
              field.getDescription(),
              field.isRequired()));
    }

    final ObjectNode responses = op.putObject("responses");
    responses.putObject("200").put("description", "success");
    responses.putObject("404").put("description", "route not found");
    responses.putObject("500").put("description", "invalid or null parameter");
    return op;
  }

  private static String getSwaggerType(final Class<?> type) {
    // array and enum types need more than a name, which processField adds
    if (type == String.class) {
      return "string";
    } else if ((type == Integer.class) || (type == int.class)) {
      return "integer";
    } else if ((type == long.class) || (type == Long.class)) {
      return "long";
    } else if ((type == Float.class) || (type == float.class)) {
      return "number";
    } else if ((type == Boolean.class) || (type == boolean.class)) {
      return "boolean";
    } else if ((type != null) && type.isEnum()) {
      return "enum";
    } else if ((type == List.class) || ((type != null) && type.isArray())) {
      return "array";
    }
    return "string";
  }
}
