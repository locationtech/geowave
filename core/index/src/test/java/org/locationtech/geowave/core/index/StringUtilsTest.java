/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.index;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class StringUtilsTest {
  @Test
  public void testFull() {
    final String[] result =
        StringUtils.stringsFromBinary(StringUtils.stringsToBinary(new String[] {"12", "34"}));
    assertEquals(2, result.length);
    assertEquals("12", result[0]);
    assertEquals("34", result[1]);
  }

  @Test
  public void testEmpty() {
    final String[] result =
        StringUtils.stringsFromBinary(StringUtils.stringsToBinary(new String[] {}));
    assertEquals(0, result.length);
  }
}
