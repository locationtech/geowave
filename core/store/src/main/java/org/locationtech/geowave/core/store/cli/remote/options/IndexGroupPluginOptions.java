/**
 * Copyright (c) 2013-2019 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.store.cli.remote.options;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.geowave.core.cli.api.DefaultPluginOptions;
import org.locationtech.geowave.core.cli.api.PluginOptions;
import com.beust.jcommander.ParametersDelegate;

/** Index group contains a list of indexes that are created from existing index configurations. */
public class IndexGroupPluginOptions extends DefaultPluginOptions implements PluginOptions {

  public static final String INDEXGROUP_PROPERTY_NAMESPACE = "indexgroup";

  /** This is indexed by index name, instead of what you'd expect, which would be index type. */
  @ParametersDelegate
  private Map<String, IndexPluginOptions> dimensionalityPlugins = new HashMap<>();

  public IndexGroupPluginOptions() {}

  /** The name of this index group. */
  @Override
  public void selectPlugin(final String qualifier) {
    // This is specified as so: name=type,name=type,...
    if ((qualifier != null) && (qualifier.length() > 0)) {
      for (final String name : qualifier.split(",")) {
        final String[] parts = name.split("=");
        addIndex(parts[0].trim(), parts[1].trim());
      }
    }
  }

  @Override
  public String getType() {
    final List<String> typeString = new ArrayList<>();
    for (final Entry<String, IndexPluginOptions> entry : getDimensionalityPlugins().entrySet()) {
      typeString.add(String.format("%s=%s", entry.getKey(), entry.getValue().getType()));
    }
    if (typeString.isEmpty()) {
      return null;
    }
    return StringUtils.join(typeString, ",");
  }

  public void addIndex(final String name, final String type) {
    if ((name != null) && (type != null)) {
      final IndexPluginOptions indexOptions = new IndexPluginOptions();
      indexOptions.selectPlugin(type);
      getDimensionalityPlugins().put(name, indexOptions);
    }
  }

  public Map<String, IndexPluginOptions> getDimensionalityPlugins() {
    return dimensionalityPlugins;
  }

  public static String getIndexGroupNamespace(final String groupName) {
    return String.format("%s.%s", INDEXGROUP_PROPERTY_NAMESPACE, groupName);
  }
}
