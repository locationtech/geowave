/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import org.locationtech.geowave.core.cli.api.ServiceEnabledCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Builds the Swagger 2.0 description of the API: one path per {@link RestRoute}, described by
 * {@link SwaggerOperationParser}, plus the file upload service.
 */
public class SwaggerApiParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(SwaggerApiParser.class);
  private static final JsonNodeFactory NODES = JsonNodeFactory.instance;
  static final String FILE_UPLOAD_PATH = "/v0/fileupload";

  private final String apiVersion;
  private final String apiTitle;
  private final String apiDescription;
  private final ObjectNode paths = NODES.objectNode();

  public SwaggerApiParser(
      final String apiVersion,
      final String apiTitle,
      final String apiDescription) {
    this.apiVersion = apiVersion;
    this.apiTitle = apiTitle;
    this.apiDescription = apiDescription;
  }

  public void addRoute(final RestRoute route) {
    final ServiceEnabledCommand<?> instance = route.getOperation();
    LOGGER.info("OPERATION: " + route.getPath() + " : " + instance.getClass().getName());
    final ObjectNode op = new SwaggerOperationParser<>(instance).getJsonObject();
    op.putArray("tags").add(route.getPath().split("/")[1]);
    paths.putObject("/" + route.getPath()).set(instance.getMethod().toString().toLowerCase(), op);
  }

  /**
   * @param host the host and port the API is served from
   * @param basePath the path the API is served under
   * @param scheme the scheme the API is served with
   * @return the Swagger document
   */
  public ObjectNode getSwagger(final String host, final String basePath, final String scheme) {
    final ObjectNode swagger = NODES.objectNode();
    swagger.put("swagger", "2.0");
    final ObjectNode info = swagger.putObject("info");
    info.put("version", apiVersion);
    info.put("title", apiTitle);
    info.put("description", apiDescription);
    info.put("termsOfService", "https://github.com/locationtech/geowave");
    info.putObject("contact").put("name", "GeoWave Team");
    info.putObject("license").put("name", "Apache2");
    swagger.put("host", host);
    swagger.put("basePath", basePath);
    swagger.putArray("schemes").add(scheme);
    swagger.putArray("consumes").add("application/json").add("multipart/form-data");
    swagger.putArray("produces").add("application/json");
    final ObjectNode allPaths = swagger.putObject("paths");
    allPaths.setAll(paths);
    allPaths.set(FILE_UPLOAD_PATH, fileUpload());
    return swagger;
  }

  private static ObjectNode fileUpload() {
    final ObjectNode path = NODES.objectNode();
    final ObjectNode post = path.putObject("post");
    post.put("operationId", "fileupload");
    post.putArray("consumes").add("multipart/form-data");
    post.put("description", "Upload a file to the server's temporary directory");
    final ObjectNode parameter = post.putArray("parameters").addObject();
    parameter.put("name", "file");
    parameter.put("description", "the file to upload");
    parameter.put("required", true);
    parameter.put("type", "file");
    parameter.put("in", "formData");
    final ObjectNode responses = post.putObject("responses");
    responses.putObject("201").put("description", "success");
    responses.putObject("400").put("description", "not exactly one file");
    responses.putObject("415").put("description", "not multipart/form-data");
    responses.putObject("500").put("description", "the file could not be stored");
    post.putArray("tags").add("fileupload");
    return path;
  }
}
