/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.cli.osm.osmfeature.types.features;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.cli.osm.osmfeature.FeatureConfigParser;
import org.locationtech.geowave.cli.osm.osmfeature.types.attributes.AttributeDefinition;
import org.locationtech.geowave.cli.osm.osmfeature.types.attributes.AttributeType;
import org.locationtech.geowave.cli.osm.osmfeature.types.attributes.AttributeTypes;
import org.locationtech.geowave.core.index.StringUtils;
import org.opengis.feature.simple.SimpleFeatureType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FeatureDefinitionSet {
  public static final List<String> GeneralizedFeatures =
      Collections.unmodifiableList(new ArrayList<String>());
  public static final List<FeatureDefinition> Features = (new ArrayList<>());
  public static final Map<String, FeatureDataAdapter> featureAdapters =
      new HashMap(new HashMap<String, FeatureDataAdapter>());
  public static final Map<String, SimpleFeatureType> featureTypes =
      new HashMap(new HashMap<String, SimpleFeatureType>());
  private static final Object MUTEX = new Object();
  private static boolean initialized = false;
  private static final Logger LOGGER = LoggerFactory.getLogger(FeatureDefinitionSet.class);

  public static void initialize(final String configFile) {
    synchronized (MUTEX) {
      if (!initialized) {
        final FeatureConfigParser fcp = new FeatureConfigParser();
        final ByteArrayInputStream bais =
            new ByteArrayInputStream(configFile.getBytes(StringUtils.getGeoWaveCharset()));
        try {
          fcp.parseConfig(bais);
        } catch (final IOException e) {
          LOGGER.error("Unable to parse config file string", e);
        } finally {
          IOUtils.closeQuietly(bais);
        }

        for (final FeatureDefinition fd : Features) {
          parseFeatureDefinition(fd);
        }

        initialized = true;
      }
    }
  }

  private static void parseFeatureDefinition(final FeatureDefinition fd) {
    final SimpleFeatureTypeBuilder sftb = new SimpleFeatureTypeBuilder();
    sftb.setName(fd.name);
    final AttributeTypeBuilder atb = new AttributeTypeBuilder();
    // Class geomClass = null;
    // switch (fd.Type) {
    // case Geometry: {
    // geomClass = Geometry.class;
    // break;
    // }
    // case Point: {
    // geomClass = Point.class;
    // break;
    // }
    // case LineString: {
    // geomClass = LineString.class;
    // break;
    // }
    // case Polygon: {
    // geomClass = Polygon.class;
    // }
    // }
    // sftb.add(atb.binding(geomClass).nillable(false).buildDescriptor("geometry"));
    for (final AttributeDefinition ad : fd.attributes) {
      final AttributeType at = AttributeTypes.getAttributeType(ad.type);
      if (ad.name == null) {
        System.out.println("yo");
      }
      if (at != null) {
        sftb.add(
            atb.binding(at.getClassType()).nillable(true).buildDescriptor(
                normalizeOsmNames(ad.name)));
      }
    }
    final SimpleFeatureType sft = sftb.buildFeatureType();
    featureTypes.put(fd.name, sft);
    featureAdapters.put(fd.name, new FeatureDataAdapter(sft));
  }

  public static String normalizeOsmNames(final String name) {
    if (name == null) {
      return null;
    }

    return name.trim().toLowerCase(Locale.ENGLISH).replace(":", "_");
  }
}
