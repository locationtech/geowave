/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.adapter.vector.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.store.statistics.field.FixedBinNumericHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.field.FixedBinNumericHistogramStatistic.FixedBinNumericHistogramValue;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

public class FixedBinNumericHistogramStatisticTest {

  private SimpleFeatureType schema;
  FeatureDataAdapter dataAdapter;
  GeometryFactory factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FIXED));

  @Before
  public void setup() throws SchemaException, CQLException, ParseException {
    schema =
        DataUtilities.createType(
            "sp.geostuff",
            "geometry:Geometry:srid=4326,pop:java.lang.Double,when:Date,whennot:Date,somewhere:Polygon,pid:String");
    dataAdapter = new FeatureDataAdapter(schema);
  }

  private SimpleFeature create(final Double val) {
    final List<AttributeDescriptor> descriptors = schema.getAttributeDescriptors();
    final Object[] defaults = new Object[descriptors.size()];
    int p = 0;
    for (final AttributeDescriptor descriptor : descriptors) {
      defaults[p++] = descriptor.getDefaultValue();
    }

    final SimpleFeature newFeature =
        SimpleFeatureBuilder.build(schema, defaults, UUID.randomUUID().toString());

    newFeature.setAttribute("pop", val);
    newFeature.setAttribute("pid", UUID.randomUUID().toString());
    newFeature.setAttribute("when", new Date());
    newFeature.setAttribute("whennot", new Date());
    newFeature.setAttribute("geometry", factory.createPoint(new Coordinate(27.25, 41.25)));
    return newFeature;
  }

  @Test
  public void testPositive() {

    final FixedBinNumericHistogramStatistic stat = new FixedBinNumericHistogramStatistic("", "pop");
    final FixedBinNumericHistogramValue statValue = stat.createEmpty();

    final Random rand = new Random(7777);

    statValue.entryIngested(dataAdapter, create(100.0));
    statValue.entryIngested(dataAdapter, create(101.0));
    statValue.entryIngested(dataAdapter, create(2.0));

    double next = 1;
    for (int i = 0; i < 10000; i++) {
      next = next + (Math.round(rand.nextDouble()));
      statValue.entryIngested(dataAdapter, create(next));
    }

    final FixedBinNumericHistogramValue statValue2 = stat.createEmpty();

    next += 1000;
    final double skewvalue = next + (1000 * rand.nextDouble());
    final SimpleFeature skewedFeature = create(skewvalue);
    for (int i = 0; i < 10000; i++) {
      statValue2.entryIngested(dataAdapter, skewedFeature);
    }

    next += 1000;
    double max = 0;
    for (long i = 0; i < 10000; i++) {
      final double val = next + (1000 * rand.nextDouble());
      statValue2.entryIngested(dataAdapter, create(val));
      max = Math.max(val, max);
    }

    final byte[] b = statValue2.toBinary();
    statValue2.fromBinary(b);
    assertEquals(1.0, statValue2.cdf(max + 1), 0.00001);

    statValue.merge(statValue2);

    assertEquals(1.0, statValue.cdf(max + 1), 0.00001);

    assertEquals(.33, statValue.cdf(skewvalue - 1000), 0.01);
    assertEquals(30003, sum(statValue.count(10)));

    final double r = statValue.percentPopulationOverRange(skewvalue - 1000, skewvalue + 1000);
    assertTrue((r > 0.45) && (r < 0.55));
  }

  @Test
  public void testRapidIncreaseInRange() {

    final FixedBinNumericHistogramStatistic stat = new FixedBinNumericHistogramStatistic("", "pop");
    final FixedBinNumericHistogramValue statValue = stat.createEmpty();

    final Random rand = new Random(7777);
    double next = 1;
    for (int i = 0; i < 10000; i++) {
      next = next + (rand.nextDouble() * 100.0);
      statValue.entryIngested(dataAdapter, create(next));
    }

    FixedBinNumericHistogramValue statValue2 = stat.createEmpty();

    next = 4839434.547854578;
    for (long i = 0; i < 10000; i++) {
      final double val = next + (1000.0 * rand.nextDouble());
      statValue2.entryIngested(dataAdapter, create(val));
    }

    byte[] b = statValue2.toBinary();
    statValue2.fromBinary(b);

    b = statValue.toBinary();
    statValue.fromBinary(b);

    statValue.merge(statValue2);

    statValue2 = stat.createEmpty();

    for (int i = 0; i < 40000; i++) {
      next = (Math.round(rand.nextDouble()));
      statValue2.entryIngested(dataAdapter, create(next));
    }

    final FixedBinNumericHistogramValue statValue3 = stat.createEmpty();

    next = 54589058545734.049454545458;
    for (long i = 0; i < 10000; i++) {
      final double val = next + (rand.nextDouble());
      statValue3.entryIngested(dataAdapter, create(val));
    }

    b = statValue2.toBinary();
    statValue2.fromBinary(b);

    b = statValue3.toBinary();
    statValue3.fromBinary(b);

    statValue.merge(statValue3);
    statValue.merge(statValue2);

    b = statValue.toBinary();
    statValue.fromBinary(b);
  }

  @Test
  public void testMix() {

    final FixedBinNumericHistogramStatistic stat = new FixedBinNumericHistogramStatistic("", "pop");
    final FixedBinNumericHistogramValue statValue = stat.createEmpty();

    final Random rand = new Random(7777);

    double min = 0;
    double max = 0;

    double next = 0;
    for (int i = 0; i < 10000; i++) {
      next = next + (100 * rand.nextDouble());
      statValue.entryIngested(dataAdapter, create(next));
      max = Math.max(next, max);
    }

    next = 0;
    for (int i = 0; i < 10000; i++) {
      next = next - (100 * rand.nextDouble());
      statValue.entryIngested(dataAdapter, create(next));
      min = Math.min(next, min);
    }

    assertEquals(0.0, statValue.cdf(min), 0.00001);

    assertEquals(1.0, statValue.cdf(max), 0.00001);

    assertEquals(0.5, statValue.cdf(0), 0.05);

    assertEquals(20000, sum(statValue.count(10)));

    final double r = statValue.percentPopulationOverRange(min / 2, max / 2);

    assertEquals(0.5, r, 0.05);
  }

  @Test
  public void testMix2() {

    final FixedBinNumericHistogramStatistic stat = new FixedBinNumericHistogramStatistic("", "pop");
    final FixedBinNumericHistogramValue statValue = stat.createEmpty();

    final Random rand = new Random(7777);

    final double min = 0;
    double max = 0;

    double next = 0;
    for (int i = 0; i < 100000; i++) {
      next = 1000 * rand.nextGaussian();
      statValue.entryIngested(dataAdapter, create(next));
      max = Math.max(next, max);
    }

    assertEquals(1.0, statValue.cdf(max), 0.00001);

    assertEquals(0.5, statValue.cdf(0), 0.05);

    assertEquals(100000, sum(statValue.count(10)));

    final double r = statValue.percentPopulationOverRange(min / 2, max / 2);

    assertEquals(0.5, r, 0.05);

    System.out.println(stat.toString());
  }

  private long sum(final long[] list) {
    long result = 0;
    for (final long v : list) {
      result += v;
    }
    return result;
  }
}
