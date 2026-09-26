/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.service.rest.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geowave.service.rest.GeoWaveRestApplication;
import org.locationtech.geowave.service.rest.InMemoryRequests;
import org.locationtech.geowave.service.rest.InMemoryRequests.Result;
import org.locationtech.geowave.service.rest.RestRoutes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ApiKeyFilterTest {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  private static class MemoryApiKeyStore implements ApiKeyStore {
    private final Map<String, String> keysByUser = new ConcurrentHashMap<>();

    @Override
    public boolean hasKey(final String apiKey) {
      return keysByUser.containsValue(apiKey);
    }

    @Override
    public String getOrCreateKey(final String userName) {
      return keysByUser.computeIfAbsent(userName, user -> user + "-key");
    }
  }

  private static GeoWaveRestApplication resources() {
    return new GeoWaveRestApplication(new RestRoutes(Collections.emptyList()));
  }

  private static InMemoryRequests withStore(final ApiKeyStore store) {
    return new InMemoryRequests(resources().register(new ApiKeyFilter(store)));
  }

  private static void assertUnauthorized(final Result result, final String message)
      throws Exception {
    // the filter used to end the chain without a status, so a rejected call looked like a 200
    assertEquals(401, result.status());
    final JsonNode json = MAPPER.readTree(result.body);
    assertEquals("ERROR", json.get("status").asText());
    assertEquals(message, json.get("message").asText());
  }

  @Test
  public void apiRequestWithoutKeyIsUnauthorized() throws Exception {
    assertUnauthorized(
        withStore(new MemoryApiKeyStore()).request("GET", "v0/operation_status?id=x").send(),
        "apiKey is required");
  }

  @Test
  public void apiRequestWithUnknownKeyIsUnauthorized() throws Exception {
    assertUnauthorized(
        withStore(new MemoryApiKeyStore()).request(
            "GET",
            "v0/operation_status?id=x&apiKey=guess").send(),
        "apiKey is invalid");
  }

  @Test
  public void apiRequestWithKeyIsServed() throws Exception {
    final MemoryApiKeyStore store = new MemoryApiKeyStore();
    final String key = store.getOrCreateKey("alice");
    final Result result =
        withStore(store).request("GET", "v0/operation_status?id=x&apiKey=" + key).send();
    assertEquals(200, result.status());
    assertEquals(
        "no operation found for ID: x",
        MAPPER.readTree(result.body).get("message").asText());
  }

  @Test
  public void mainPageNeedsNoKey() throws Exception {
    final Result result = withStore(new MemoryApiKeyStore()).request("GET", "").send();
    assertEquals(200, result.status());
    assertFalse(result.body.contains("Welcome"));
  }

  @Test
  public void eachUserSeesOnlyTheirOwnKey() throws Exception {
    final InMemoryRequests app = withStore(new MemoryApiKeyStore());

    final String alice = app.request("GET", "").user("alice").send().body;
    assertTrue(alice, alice.contains("Welcome alice!"));
    assertTrue(alice, alice.contains("alice-key"));

    // the keys used to be servlet context attributes, so the last user's showed for everyone
    final String bob = app.request("GET", "").user("bob").send().body;
    assertTrue(bob, bob.contains("Welcome bob!"));
    assertTrue(bob, bob.contains("bob-key"));
    assertFalse(bob, bob.contains("alice"));

    final String anonymous = app.request("GET", "").send().body;
    assertFalse(anonymous, anonymous.contains("Welcome"));
    assertFalse(anonymous, anonymous.contains("-key"));
  }

  @Test
  public void featureTurnsKeysOnWithADatabase() throws Exception {
    final InMemoryRequests app =
        new InMemoryRequests(
            resources().property(
                GeoWaveRestApplication.API_KEY_DB_PROPERTY,
                new File(temp.getRoot(), "ApiKeys.db").getAbsolutePath()));
    assertUnauthorized(app.request("GET", "v0/operation_status?id=x").send(), "apiKey is required");

    final String page = app.request("GET", "").user("carol").send().body;
    final String key =
        page.substring(page.indexOf("<b>API key:</b> ") + 16, page.indexOf("<br><br>"));
    assertEquals(200, app.request("GET", "v0/operation_status?id=x&apiKey=" + key).send().status());
  }

  @Test
  public void featureIsOffWithoutADatabase() throws Exception {
    final InMemoryRequests app = new InMemoryRequests(resources());
    assertEquals(200, app.request("GET", "v0/operation_status?id=x").send().status());
  }

  @Test(expected = IllegalStateException.class)
  public void featureFailsRatherThanRunUnprotected() throws Exception {
    new InMemoryRequests(
        resources().property(
            GeoWaveRestApplication.API_KEY_DB_PROPERTY,
            new File(temp.getRoot(), "missing/dir/ApiKeys.db").getAbsolutePath()));
  }
}
