/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Set;
import java.util.TreeSet;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geowave.service.rest.InMemoryRequests.Result;
import com.fasterxml.jackson.databind.JsonNode;

public class GeoWaveRestApplicationTest {
  @ClassRule
  public static final TemporaryFolder TEMP = new TemporaryFolder();

  private static RestRoutes routes;
  private static InMemoryRequests app;

  @BeforeClass
  public static void setup() throws Exception {
    routes = RestRoutes.find();
    app =
        new InMemoryRequests(
            new GeoWaveRestApplication(routes).property(
                GeoWaveRestApplication.CONFIG_FILE_PROPERTY,
                new File(TEMP.getRoot(), "config.properties").getAbsolutePath()));
  }

  private static JsonNode json(final Result result) throws Exception {
    return JsonResponses.MAPPER.readTree(result.body);
  }

  private static JsonNode configList() throws Exception {
    final Result list = app.request("GET", "v0/config/list").send();
    assertEquals(200, list.status());
    return json(list).get("data");
  }

  @Test
  public void swaggerDescribesEveryRouteAndFileUpload() throws Exception {
    final Result result = app.request("GET", "api").send();
    assertEquals(200, result.status());
    assertTrue(result.header("Content-Type").startsWith("application/json"));

    final JsonNode swagger = json(result);
    assertEquals("2.0", swagger.get("swagger").asText());
    assertEquals("localhost:9012", swagger.get("host").asText());
    assertEquals("/restservices", swagger.get("basePath").asText());
    assertEquals("http", swagger.get("schemes").get(0).asText());

    final Set<String> routePaths = new TreeSet<>();
    for (final RestRoute route : routes.list()) {
      routePaths.add("/" + route.getPath());
      final JsonNode path = swagger.get("paths").get("/" + route.getPath());
      assertNotNull(route.getPath(), path);
      assertNotNull(
          route.getPath(),
          path.get(route.getOperation().getMethod().toString().toLowerCase()));
    }
    assertTrue(routePaths.toString(), routePaths.contains("/v0/config/list"));
    assertTrue(routePaths.toString(), routePaths.contains("/v0/store/add/filesystem"));
    assertEquals(routePaths.size() + 1, swagger.get("paths").size());
    assertNotNull(swagger.get("paths").get("/v0/fileupload").get("post"));
  }

  @Test
  public void mainPageListsRoutes() throws Exception {
    final Result result = app.request("GET", "").send();
    assertEquals(200, result.status());
    assertTrue(result.header("Content-Type").startsWith("text/html"));
    assertTrue(result.body.startsWith("Available Routes:<br>"));
    assertTrue(result.body.contains("v0/config/list --> "));
  }

  @Test
  public void routeTakesQueryParameters() throws Exception {
    final Result set = app.request("POST", "v0/config/set?name=query-key&value=query-value").send();
    assertEquals(200, set.status());
    assertEquals("COMPLETE", json(set).get("status").asText());
    assertEquals("query-value", configList().get("query-key").asText());
  }

  @Test
  public void routeTakesJsonBody() throws Exception {
    final Result set =
        app.request("POST", "v0/config/set").body(
            "application/json",
            "{\"name\": \"json-key\", \"value\": \"json-value\"}").send();
    assertEquals(200, set.status());
    assertEquals("json-value", configList().get("json-key").asText());
  }

  @Test
  public void routeTakesFormBody() throws Exception {
    final Result set =
        app.request("POST", "v0/config/set").body(
            "application/x-www-form-urlencoded",
            "name=form-key&value=form+value").send();
    assertEquals(200, set.status());
    assertEquals("form value", configList().get("form-key").asText());
  }

  @Test
  public void answersWhateverTheRequestAccepts() throws Exception {
    // services/client's StatService asks for text/plain, and Restlet answered JSON regardless
    final Result list = app.request("GET", "v0/config/list").header("Accept", "text/plain").send();
    assertEquals(200, list.status());
    assertTrue(list.header("Content-Type").startsWith("application/json"));
    assertEquals(
        200,
        app.request("GET", "v0/operation_status?id=x").header(
            "Accept",
            "text/plain").send().status());

    final Result api = app.request("GET", "api").header("Accept", "text/html").send();
    assertEquals(200, api.status());
    assertTrue(api.header("Content-Type").startsWith("application/json"));

    final Result page = app.request("GET", "").header("Accept", "application/json").send();
    assertEquals(200, page.status());
    assertTrue(page.header("Content-Type").startsWith("text/html"));
  }

  @Test
  public void missingArgumentIsBadRequest() throws Exception {
    final Result set = app.request("POST", "v0/config/set?name=only-a-name").send();
    assertEquals(400, set.status());
    assertEquals("ERROR", json(set).get("status").asText());
  }

  @Test
  public void unknownRouteIsNotFound() throws Exception {
    assertEquals(404, app.request("GET", "v0/no/such/route").send().status());
    assertEquals(404, app.request("POST", "v0/no/such/route").send().status());
  }

  @Test
  public void wrongMethodIsNotAllowed() throws Exception {
    final Result result = app.request("GET", "v0/config/set").send();
    assertEquals(405, result.status());
    assertEquals("POST", result.header("Allow"));
  }

  @Test
  public void unknownOperationStatusIsReported() throws Exception {
    final Result result = app.request("GET", "v0/operation_status?id=no-such-id").send();
    assertEquals(200, result.status());
    assertEquals("ERROR", json(result).get("status").asText());
    assertEquals("no operation found for ID: no-such-id", json(result).get("message").asText());
  }

  @Test
  public void corsEchoesTheOrigin() throws Exception {
    final Result result =
        app.request("GET", "v0/config/list").header("Origin", "http://example.com").send();
    assertEquals(200, result.status());
    assertEquals("http://example.com", result.header("Access-Control-Allow-Origin"));
    assertEquals("true", result.header("Access-Control-Allow-Credentials"));

    final Result noOrigin = app.request("GET", "v0/config/list").send();
    assertNull(noOrigin.header("Access-Control-Allow-Origin"));
  }

  @Test
  public void corsPreflightIsAnswered() throws Exception {
    final Result result =
        app.request("OPTIONS", "v0/config/set").header("Origin", "http://example.com").header(
            "Access-Control-Request-Method",
            "POST").header("Access-Control-Request-Headers", "content-type").send();
    assertEquals(200, result.status());
    assertEquals("http://example.com", result.header("Access-Control-Allow-Origin"));
    assertTrue(result.header("Access-Control-Allow-Methods").contains("POST"));
    assertEquals("content-type", result.header("Access-Control-Allow-Headers"));
  }

  private static String multipart(final String... fileNames) {
    final StringBuilder body = new StringBuilder();
    for (final String fileName : fileNames) {
      body.append("--BOUNDARY\r\n").append(
          "Content-Disposition: form-data; name=\"file\"; filename=\"").append(fileName).append(
              "\"\r\n").append("Content-Type: text/plain\r\n\r\n").append("contents of ").append(
                  fileName).append("\r\n");
    }
    return body.append("--BOUNDARY--\r\n").toString();
  }

  @Test
  public void fileUploadStoresTheFileUnderASafeName() throws Exception {
    final Result result =
        app.request("POST", "v0/fileupload").body(
            "multipart/form-data; boundary=BOUNDARY",
            multipart("../../a dir/up load.gpx")).send();
    assertEquals(201, result.status());
    final String message = json(result).get("message").asText();
    assertTrue(message, message.startsWith("File uploaded to: "));
    final File file = new File(message.substring("File uploaded to: ".length()));
    try {
      assertEquals(
          new File(System.getProperty("java.io.tmpdir")).getCanonicalFile(),
          file.getParentFile().getCanonicalFile());
      assertTrue(file.getName(), file.getName().endsWith("-up_load.gpx"));
      assertEquals(
          "contents of ../../a dir/up load.gpx",
          new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8));
    } finally {
      assertTrue(file.delete());
    }
  }

  @Test
  public void fileUploadNeedsExactlyOneFile() throws Exception {
    final Result result =
        app.request("POST", "v0/fileupload").body(
            "multipart/form-data; boundary=BOUNDARY",
            multipart("a.txt", "b.txt")).send();
    assertEquals(400, result.status());
    assertEquals("Operation requires exactly one file.", json(result).get("message").asText());
  }

  @Test
  public void fileUploadNeedsMultipart() throws Exception {
    assertEquals(
        415,
        app.request("POST", "v0/fileupload").body("text/plain", "text").send().status());
  }

  @Test
  public void safeFileNames() {
    assertEquals("upload", FileUploadResource.safeFileName(null));
    assertEquals("upload", FileUploadResource.safeFileName("dir/"));
    assertEquals("passwd", FileUploadResource.safeFileName("..\\..\\etc\\passwd"));
    assertEquals("a_b_.txt", FileUploadResource.safeFileName("a b;.txt"));
  }
}
