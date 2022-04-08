/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.spark;

import org.apache.spark.serializer.KryoRegistrator;
import org.geotools.feature.simple.SimpleFeatureImpl;
import org.locationtech.geowave.adapter.raster.adapter.GridCoverageWritable;
import org.locationtech.geowave.analytic.kryo.FeatureSerializer;
import org.locationtech.geowave.analytic.kryo.GridCoverageWritableSerializer;
import org.locationtech.geowave.analytic.kryo.PersistableSerializer;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.persist.PersistableFactory;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import com.esotericsoftware.kryo.Kryo;

public class GeoWaveRegistrator implements KryoRegistrator {
  @Override
  public void registerClasses(final Kryo kryo) {
    // Use existing FeatureSerializer code to serialize SimpleFeature
    // classes
    final FeatureSerializer simpleFeatureSerializer = new FeatureSerializer();
    final GridCoverageWritableSerializer gcwSerializer = new GridCoverageWritableSerializer();
    final PersistableSerializer persistSerializer = new PersistableSerializer();

    PersistableFactory.getInstance().getClassIdMapping().entrySet().forEach(
        e -> kryo.register(e.getKey(), persistSerializer));

    kryo.register(GeoWaveRDD.class);
    kryo.register(GeoWaveIndexedRDD.class);
    kryo.register(Geometry.class);
    kryo.register(PreparedGeometry.class);
    kryo.register(ByteArray.class);
    kryo.register(GeoWaveInputKey.class);
    kryo.register(SimpleFeatureImpl.class, simpleFeatureSerializer);
    kryo.register(GridCoverageWritable.class, gcwSerializer);
  }
}
