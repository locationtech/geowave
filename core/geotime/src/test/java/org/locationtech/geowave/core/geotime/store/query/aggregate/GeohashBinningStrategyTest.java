/**
 * Copyright (c) 2013-2020 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.core.geotime.store.query.aggregate;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.junit.Test;
import org.locationtech.geowave.core.geotime.store.dimension.GeometryWrapper;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.persist.PersistenceUtils;
import org.locationtech.geowave.core.store.data.CommonIndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.MultiFieldPersistentDataset;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.SingleFieldPersistentDataset;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import java.util.UUID;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class GeohashBinningStrategyTest {

  private static final GeometryFactory geoFactory = new GeometryFactory();
  private static final SimpleFeatureType schema;

  static {
    try {
      schema = DataUtilities.createType("testGeo", "location:Point:srid=4326,name:String");
    } catch (SchemaException e) {
      throw new RuntimeException(e);
    }
  }

  private static SimpleFeature createSimpleFeature(Coordinate c) {
    String name = UUID.randomUUID().toString();
    return SimpleFeatureBuilder.build(
        GeohashBinningStrategyTest.schema,
        new Object[] {geoFactory.createPoint(c), name},
        name);
  }

  private static CommonIndexedPersistenceEncoding createCommonIndexData(
      final Coordinate coordinate) {
    final PersistentDataset<CommonIndexValue> commonData = new SingleFieldPersistentDataset<>();

    commonData.addValue(
        GeometryWrapper.DEFAULT_GEOMETRY_FIELD_NAME,
        new GeometryWrapper(geoFactory.createPoint(coordinate)));

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
      assertThat(new GeohashSimpleFeatureBinningStrategy(i).getPrecision(), is(i));
      assertThat(new GeohashCommonIndexedBinningStrategy(i).getPrecision(), is(i));
    }
  }

  @Test
  public void testNoGeometry() throws SchemaException {
    SimpleFeatureType noGeoType = DataUtilities.createType("testNoGeo", "name:String");
    SimpleFeature noGeoFeature =
        SimpleFeatureBuilder.build(noGeoType, new Object[] {"NAME!"}, "NAME!");
    GeohashBinningStrategy<SimpleFeature> sfStrat = new GeohashSimpleFeatureBinningStrategy(4);

    // If the feature does not have a geometry, null is returned by binEntry.
    String[] bin = sfStrat.binEntry(noGeoFeature);
    assertNull(bin);

    GeohashBinningStrategy<CommonIndexedPersistenceEncoding> ciStrat =
        new GeohashCommonIndexedBinningStrategy(4, "NotTheGeoField");

    // we are looking in the wrong field for the geometry type here, so therefore no Geometry will
    // be found.
    bin = ciStrat.binEntry(createCommonIndexData(new Coordinate(1, 1, 1)));
    assertNull(bin);
  }

  @Test
  public void testEncodeToGeohash() {
    Coordinate coord = new Coordinate(49.619, -5.821);
    Point point = geoFactory.createPoint(coord);
    // calculated this beforehand.
    String hash = "gbgf78c78u5b";
    for (int i = 1; i < hash.length(); i++) {
      assertThat(hash.substring(0, i), is(GeohashBinningStrategy.encodeToGeohash(point, i)));
    }
  }

  @Test
  public void testBinSimpleFeature() {
    SimpleFeature feature1 = createSimpleFeature(new Coordinate(0, 0));

    // same coord, but different name, make sure it still works in this simple case
    SimpleFeature feature2 = createSimpleFeature(new Coordinate(40, 40));
    SimpleFeature feature3 = createSimpleFeature(new Coordinate(40, 40));

    GeohashBinningStrategy<SimpleFeature> strat = new GeohashSimpleFeatureBinningStrategy(4);

    String bin1 = strat.binEntry(feature1)[0];
    String bin2 = strat.binEntry(feature2)[0];
    String bin3 = strat.binEntry(feature3)[0];

    assertThat(bin1, is(not(bin2)));
    assertThat(bin2, is(bin3));

    strat = new GeohashSimpleFeatureBinningStrategy(1);

    feature1 = createSimpleFeature(new Coordinate(0, 0));
    feature2 = createSimpleFeature(new Coordinate(0.01, 0.01));

    bin1 = strat.binEntry(feature1)[0];
    bin2 = strat.binEntry(feature2)[0];
    // even though they are different coords, they are binned together due to precision.
    assertThat(bin1, is(bin2));
  }

  @Test
  public void testBinCommonIndexModel() {
    GeohashBinningStrategy<CommonIndexedPersistenceEncoding> strat =
        new GeohashCommonIndexedBinningStrategy(4);

    CommonIndexedPersistenceEncoding data1 = createCommonIndexData(new Coordinate(0, 0));
    CommonIndexedPersistenceEncoding data2 = createCommonIndexData(new Coordinate(40, 40));

    String bin1 = strat.binEntry(data1)[0];
    String bin2 = strat.binEntry(data2)[0];
    assertThat(bin1, is(not(bin2)));

    strat = new GeohashCommonIndexedBinningStrategy(1);

    data1 = createCommonIndexData(new Coordinate(0, 0));
    data2 = createCommonIndexData(new Coordinate(0.01, 0.01));

    bin1 = strat.binEntry(data1)[0];
    bin2 = strat.binEntry(data2)[0];
    // even though they are different coords, they are binned together.
    assertThat(bin1, is(bin2));
  }

  @Test
  public void testSerialize() {
    GeohashBinningStrategy<?> strat = new GeohashSimpleFeatureBinningStrategy();
    byte[] stratBytes = PersistenceUtils.toBinary(strat);
    GeohashBinningStrategy<?> roundtrip =
        (GeohashSimpleFeatureBinningStrategy) PersistenceUtils.fromBinary(stratBytes);
    assertThat(strat.getPrecision(), is(roundtrip.getPrecision()));

    strat = new GeohashCommonIndexedBinningStrategy();
    stratBytes = PersistenceUtils.toBinary(strat);
    roundtrip = (GeohashCommonIndexedBinningStrategy) PersistenceUtils.fromBinary(stratBytes);
    assertThat(strat.getPrecision(), is(roundtrip.getPrecision()));
  }
}
