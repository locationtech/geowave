/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.geoserver;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * These pin the two things that differ between json-lib and Jackson and that this CLI's output
 * contract depends on: a body that is not a JSON object is an error, and the printed shape.
 */
public class GeoServerJsonTest {

  @Test(expected = IllegalArgumentException.class)
  public void emptyBodyIsAnError() {
    GeoServerJson.parse("");
  }

  @Test(expected = IllegalArgumentException.class)
  public void truncatedBodyIsAnError() {
    GeoServerJson.parse("{\"dataStore\":{\"na");
  }

  @Test(expected = IllegalArgumentException.class)
  public void topLevelArrayIsAnError() {
    GeoServerJson.parse("[1,2]");
  }

  @Test(expected = IllegalArgumentException.class)
  public void nonJsonBodyIsAnError() {
    GeoServerJson.parse("<html><body>502</body></html>");
  }

  @Test(expected = IllegalArgumentException.class)
  public void nonStringEntityIsAnError() {
    GeoServerJson.parse(new Object());
  }

  @Test
  public void fieldSeparatorHasNoSpaceBeforeTheColon() {
    final ObjectNode node = GeoServerJson.object();
    node.put("name", "myworkspace");
    assertEquals("{\n  \"name\": \"myworkspace\"\n}", GeoServerJson.pretty(node));
  }

  @Test
  public void arrayElementsEachGetTheirOwnLine() {
    final ArrayNode layers = GeoServerJson.array();
    layers.add(GeoServerJson.object().put("name", "a"));
    layers.add(GeoServerJson.object().put("name", "b"));
    final ObjectNode node = GeoServerJson.object();
    node.set("layers", layers);
    assertEquals(
        "{\n"
            + "  \"layers\": [\n"
            + "    {\n"
            + "      \"name\": \"a\"\n"
            + "    },\n"
            + "    {\n"
            + "      \"name\": \"b\"\n"
            + "    }\n"
            + "  ]\n"
            + "}",
        GeoServerJson.pretty(node));
  }

  @Test
  public void textReturnsNullForAnAbsentField() {
    assertEquals(null, GeoServerJson.text(GeoServerJson.object(), "name"));
    assertEquals(null, GeoServerJson.text(null, "name"));
  }
}
