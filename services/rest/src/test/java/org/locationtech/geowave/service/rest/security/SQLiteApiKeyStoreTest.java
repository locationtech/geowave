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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.io.File;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class SQLiteApiKeyStoreTest {
  @Rule
  public final TemporaryFolder temp = new TemporaryFolder();

  @Test
  public void keysAreKeptPerUser() throws Exception {
    final String dbFile = new File(temp.getRoot(), "ApiKeys.db").getAbsolutePath();
    final SQLiteApiKeyStore store = new SQLiteApiKeyStore(dbFile);

    final String alice = store.getOrCreateKey("alice");
    assertNotNull(alice);
    assertEquals(alice, store.getOrCreateKey("alice"));
    final String bob = store.getOrCreateKey("bob");
    assertNotEquals(alice, bob);

    assertTrue(store.hasKey(alice));
    assertTrue(store.hasKey(bob));
    assertFalse(store.hasKey("guess"));

    final SQLiteApiKeyStore reopened = new SQLiteApiKeyStore(dbFile);
    assertTrue(reopened.hasKey(alice));
    assertEquals(bob, reopened.getOrCreateKey("bob"));
  }
}
