/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.adapter;

import java.util.List;
import java.util.Set;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.jts.geom.Geometry;
import com.google.common.collect.Sets;

/**
 * Maps a `Geometry` adapter field to a `Geometry` index field.
 */
public class GeometryFieldMapper extends SpatialFieldMapper<Geometry> {

  @Override
  protected Geometry getNativeGeometry(List<Geometry> nativeFieldValues) {
    return nativeFieldValues.get(0);
  }

  @Override
  public void toAdapter(final Geometry indexFieldValue, final RowBuilder<?> rowBuilder) {
    rowBuilder.setField(adapterFields[0], indexFieldValue);
  }

  @Override
  public short adapterFieldCount() {
    return 1;
  }

  @Override
  public Class<Geometry> adapterFieldType() {
    return Geometry.class;
  }

  @Override
  public Set<String> getLowerCaseSuggestedFieldNames() {
    return Sets.newHashSet("geom", "geometry", "the_geom");
  }
}
