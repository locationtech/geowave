/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.format.geotools.vector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.io.File;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.geotools.api.feature.simple.SimpleFeature;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.ingest.GeoWaveData;

public class GeoToolsVectorDataStoreIngestPluginTest {
  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testIngestGeoJson() throws Exception {
    final File file = folder.newFile("points.geojson");
    Files.write(
        file.toPath(),
        ("{\"type\":\"FeatureCollection\",\"features\":["
            + "{\"type\":\"Feature\",\"properties\":{\"name\":\"a\"},"
            + "\"geometry\":{\"type\":\"Point\",\"coordinates\":[-77.03,38.89]}},"
            + "{\"type\":\"Feature\",\"properties\":{\"name\":\"b\"},"
            + "\"geometry\":{\"type\":\"Point\",\"coordinates\":[2.35,48.86]}}]}").getBytes(
                StandardCharsets.UTF_8));
    final URL url = file.toURI().toURL();
    final GeoToolsVectorDataStoreIngestPlugin plugin = new GeoToolsVectorDataStoreIngestPlugin();

    assertTrue(plugin.supportsFile(url));
    assertEquals(1, plugin.getDataAdapters(url).length);
    final List<String> features = new ArrayList<>();
    try (CloseableIterator<GeoWaveData<SimpleFeature>> it =
        plugin.toGeoWaveData(url, new String[] {"index"})) {
      while (it.hasNext()) {
        final SimpleFeature feature = it.next().getValue();
        features.add(feature.getAttribute("name") + " " + feature.getDefaultGeometry());
      }
    }
    assertEquals(List.of("a POINT (-77.03 38.89)", "b POINT (2.35 48.86)"), features);
  }
}
