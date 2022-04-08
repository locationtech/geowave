/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.memory;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class MemoryStoreUtilsTest {
  @Test
  public void testVisibility() {
    assertTrue(
        MemoryStoreUtils.isAuthorized("aaa&ccc".getBytes(), new String[] {"aaa", "bbb", "ccc"}));

    assertFalse(MemoryStoreUtils.isAuthorized("aaa&ccc".getBytes(), new String[] {"aaa", "bbb"}));

    assertTrue(
        MemoryStoreUtils.isAuthorized(
            "aaa&(ccc|eee)".getBytes(),
            new String[] {"aaa", "eee", "xxx"}));

    assertTrue(
        MemoryStoreUtils.isAuthorized(
            "aaa|(ccc&eee)".getBytes(),
            new String[] {"bbb", "eee", "ccc"}));

    assertFalse(
        MemoryStoreUtils.isAuthorized(
            "aaa|(ccc&eee)".getBytes(),
            new String[] {"bbb", "dddd", "ccc"}));

    assertTrue(
        MemoryStoreUtils.isAuthorized(
            "aaa|(ccc&eee)".getBytes(),
            new String[] {"aaa", "dddd", "ccc"}));

    assertTrue(
        MemoryStoreUtils.isAuthorized("aaa".getBytes(), new String[] {"aaa", "dddd", "ccc"}));

    assertFalse(
        MemoryStoreUtils.isAuthorized("xxx".getBytes(), new String[] {"aaa", "dddd", "ccc"}));
  }
}
