/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import java.util.UUID;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.binning.SpatialBinningType;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.geotime.store.dimension.SpatialField;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.SingleFieldPersistentDataset;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import com.github.davidmoten.geo.GeoHash;

public class GeohashBinningStrategyTest {

  private static final GeometryFactory geoFactory = new GeometryFactory();
  private static final SimpleFeatureType schema;

  static {
    try {
      schema = DataUtilities.createType("testGeo", "location:Point:srid=4326,name:String");
    } catch (final SchemaException e) {
      throw new RuntimeException(e);
    }
  }

  private static SimpleFeature createSimpleFeature(final Coordinate c) {
    final String name = UUID.randomUUID().toString();
    return SimpleFeatureBuilder.build(
        GeohashBinningStrategyTest.schema,
        new Object[] {geoFactory.createPoint(c), name},
        name);
  }

  private static CommonIndexedPersistenceEncoding createCommonIndexData(
      final Coordinate coordinate) {
    final PersistentDataset<Object> commonData = new SingleFieldPersistentDataset<>();

    commonData.addValue(
        SpatialField.DEFAULT_GEOMETRY_FIELD_NAME,
        geoFactory.createPoint(coordinate));

    return new CommonIndexedPersistenceEncoding(
        (short) 1,
        StringUtils.stringToBinary("1"),
        StringUtils.stringToBinary("1"),
        StringUtils.stringToBinary("1"),
        1,
        commonData,
        new MultiFieldPersistentDataset<>());
  }

  @Test
  public void testPrecisionConstructor() {
    for (int i = 0; i < 100; i++) {
      assertThat(
          new SpatialSimpleFeatureBinningStrategy(
              SpatialBinningType.GEOHASH,
              i,
              true).getPrecision(),
          is(i));
      assertThat(
          new SpatialCommonIndexedBinningStrategy(
              SpatialBinningType.GEOHASH,
              i,
              true).getPrecision(),
          is(i));
    }
  }

  @Test
  public void testNoGeometry() throws SchemaException {
    final SimpleFeatureType noGeoType = DataUtilities.createType("testNoGeo", "name:String");
    final SimpleFeature noGeoFeature =
        SimpleFeatureBuilder.build(noGeoType, new Object[] {"NAME!"}, "NAME!");
    final SpatialBinningStrategy<SimpleFeature> sfStrat =
        new SpatialSimpleFeatureBinningStrategy(SpatialBinningType.GEOHASH, 4, true);

    // If the feature does not have a geometry, null is returned by binEntry.
    ByteArray[] bin = sfStrat.getBins(null, noGeoFeature);
    assertNull(bin);

    final SpatialBinningStrategy<CommonIndexedPersistenceEncoding> ciStrat =
        new SpatialCommonIndexedBinningStrategy(
            SpatialBinningType.GEOHASH,
            4,
            true,
            "NotTheGeoField");

    // we are looking in the wrong field for the geometry type here, so therefore no Geometry will
    // be found.
    bin = ciStrat.getBins(null, createCommonIndexData(new Coordinate(1, 1, 1)));
    assertNull(bin);
  }

  @Test
  public void testEncodeToGeohash() {
    final Coordinate coord = new Coordinate(49.619, -5.821);
    final Point point = geoFactory.createPoint(coord);
    // calculated this beforehand.
    final String hash = "mngqch76nwb";
    for (int i = 1; i < hash.length(); i++) {
      assertThat(hash.substring(0, i), is(GeoHash.encodeHash(point.getY(), point.getX(), i)));
    }
  }

  @Test
  public void testBinSimpleFeature() {
    SimpleFeature feature1 = createSimpleFeature(new Coordinate(0, 0));

    // same coord, but different name, make sure it still works in this simple case
    SimpleFeature feature2 = createSimpleFeature(new Coordinate(40, 40));
    final SimpleFeature feature3 = createSimpleFeature(new Coordinate(40, 40));

    SpatialBinningStrategy<SimpleFeature> strat =
        new SpatialSimpleFeatureBinningStrategy(SpatialBinningType.GEOHASH, 4, true);

    ByteArray bin1 = strat.getBins(null, feature1)[0];
    ByteArray bin2 = strat.getBins(null, feature2)[0];
    final ByteArray bin3 = strat.getBins(null, feature3)[0];

    assertThat(bin1, is(not(bin2)));
    assertThat(bin2, is(bin3));

    strat = new SpatialSimpleFeatureBinningStrategy(SpatialBinningType.GEOHASH, 1, true);

    feature1 = createSimpleFeature(new Coordinate(0, 0));
    feature2 = createSimpleFeature(new Coordinate(0.01, 0.01));

    bin1 = strat.getBins(null, feature1)[0];
    bin2 = strat.getBins(null, feature2)[0];
    // even though they are different coords, they are binned together due to precision.
    assertThat(bin1, is(bin2));
  }

  @Test
  public void testBinCommonIndexModel() {
    SpatialBinningStrategy<CommonIndexedPersistenceEncoding> strat =
        new SpatialCommonIndexedBinningStrategy(SpatialBinningType.GEOHASH, 4, true);

    CommonIndexedPersistenceEncoding data1 = createCommonIndexData(new Coordinate(0, 0));
    CommonIndexedPersistenceEncoding data2 = createCommonIndexData(new Coordinate(40, 40));

    ByteArray bin1 = strat.getBins(null, data1)[0];
    ByteArray bin2 = strat.getBins(null, data2)[0];
    assertThat(bin1, is(not(bin2)));

    strat = new SpatialCommonIndexedBinningStrategy(SpatialBinningType.GEOHASH, 1, true);

    data1 = createCommonIndexData(new Coordinate(0, 0));
    data2 = createCommonIndexData(new Coordinate(0.01, 0.01));

    bin1 = strat.getBins(null, data1)[0];
    bin2 = strat.getBins(null, data2)[0];
    // even though they are different coords, they are binned together.
    assertThat(bin1, is(bin2));
  }

  @Test
  public void testSerialize() {
    SpatialBinningStrategy<?> strat = new SpatialSimpleFeatureBinningStrategy();
    byte[] stratBytes = PersistenceUtils.toBinary(strat);
    SpatialBinningStrategy<?> roundtrip =
        (SpatialSimpleFeatureBinningStrategy) PersistenceUtils.fromBinary(stratBytes);
    assertThat(strat.getPrecision(), is(roundtrip.getPrecision()));

    strat = new SpatialCommonIndexedBinningStrategy();
    stratBytes = PersistenceUtils.toBinary(strat);
    roundtrip = (SpatialCommonIndexedBinningStrategy) PersistenceUtils.fromBinary(stratBytes);
    assertThat(strat.getPrecision(), is(roundtrip.getPrecision()));
  }
}
