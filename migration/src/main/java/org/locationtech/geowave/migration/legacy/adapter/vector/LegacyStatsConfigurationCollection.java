/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.migration.legacy.adapter.vector;

import java.util.HashMap;
import java.util.Map;
import org.locationtech.geowave.core.geotime.util.SimpleFeatureUserDataConfiguration;
import org.locationtech.geowave.core.index.persist.Persistable;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 * A collection of statistics configurations targeted to a specific attribute. Each configuration
 * describes how to construct a statistic for an attribute.
 */
public class LegacyStatsConfigurationCollection implements java.io.Serializable, Persistable {

  private static final long serialVersionUID = -4983543525776889248L;

  public LegacyStatsConfigurationCollection() {}

  @Override
  public byte[] toBinary() {
    return new byte[0];
  }

  @Override
  public void fromBinary(final byte[] bytes) {
    return;
  }

  public static class LegacySimpleFeatureStatsConfigurationCollection implements
      SimpleFeatureUserDataConfiguration {

    private static final long serialVersionUID = -9149753182284018327L;
    private Map<String, LegacyStatsConfigurationCollection> attConfig = new HashMap<>();

    public LegacySimpleFeatureStatsConfigurationCollection() {}

    public LegacySimpleFeatureStatsConfigurationCollection(final SimpleFeatureType type) {
      super();
      configureFromType(type);
    }

    public Map<String, LegacyStatsConfigurationCollection> getAttConfig() {
      return attConfig;
    }

    public void setAttConfig(final Map<String, LegacyStatsConfigurationCollection> attConfig) {
      this.attConfig = attConfig;
    }

    @Override
    public void updateType(final SimpleFeatureType type) {}

    @Override
    public void configureFromType(final SimpleFeatureType type) {}

    @Override
    public byte[] toBinary() {
      return new byte[0];
    }

    @Override
    public void fromBinary(final byte[] bytes) {}
  }
}
