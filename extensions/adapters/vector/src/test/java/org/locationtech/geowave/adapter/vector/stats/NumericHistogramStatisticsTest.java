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
import org.apache.commons.math.util.MathUtils;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.core.store.statistics.field.NumericHistogramStatistic;
import org.locationtech.geowave.core.store.statistics.field.NumericHistogramStatistic.NumericHistogramValue;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.PrecisionModel;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

public class NumericHistogramStatisticsTest {

  private SimpleFeatureType schema;
  FeatureDataAdapter dataAdapter;
  GeometryFactory factory = new GeometryFactory(new PrecisionModel(PrecisionModel.FIXED));

  @Before
  public void setup() throws SchemaException, CQLException, ParseException {
    schema =
        DataUtilities.createType(
            "sp.geostuff",
            "geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,somewhere:Polygon,pid:String");
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

    final NumericHistogramStatistic stat = new NumericHistogramStatistic("", "pop");
    final NumericHistogramValue statValue = stat.createEmpty();

    final Random rand = new Random(7777);

    statValue.entryIngested(dataAdapter, create(100.0));
    statValue.entryIngested(dataAdapter, create(101.0));
    statValue.entryIngested(dataAdapter, create(2.0));

    double next = 1;
    for (int i = 0; i < 10000; i++) {
      next = next + (Math.round(rand.nextDouble()));
      statValue.entryIngested(dataAdapter, create(next));
    }

    final NumericHistogramValue statValue2 = stat.createEmpty();

    final double start2 = next;

    double max = 0;
    for (long i = 0; i < 10000; i++) {
      final double val = next + (1000 * rand.nextDouble());
      statValue2.entryIngested(dataAdapter, create(val));
      max = Math.max(val, max);
    }
    final double skewvalue = next + (1000 * rand.nextDouble());
    final SimpleFeature skewedFeature = create(skewvalue);
    for (int i = 0; i < 10000; i++) {
      statValue2.entryIngested(dataAdapter, skewedFeature);
      // skewedFeature.setAttribute("pop", Long.valueOf(next + (long)
      // (1000 * rand.nextDouble())));
    }

    final byte[] b = statValue2.toBinary();
    statValue2.fromBinary(b);
    assertEquals(1.0, statValue2.cdf(max + 1), 0.00001);

    statValue.merge(statValue2);

    assertEquals(1.0, statValue.cdf(max + 1), 0.00001);

    assertEquals(0.33, statValue.cdf(start2), 0.01);

    assertEquals(30003, sum(statValue.count(10)));

    final double r = statValue.percentPopulationOverRange(skewvalue - 1, skewvalue + 1);
    assertTrue((r > 0.3) && (r < 0.35));
  }

  @Test
  public void testRapidIncreaseInRange() {

    final NumericHistogramStatistic stat = new NumericHistogramStatistic("", "pop");
    final NumericHistogramValue statValue = stat.createEmpty();

    final Random rand = new Random(7777);
    double next = 1;
    for (int i = 0; i < 100; i++) {
      next = next + (rand.nextDouble() * 100.0);
      statValue.entryIngested(dataAdapter, create(next));
    }

    for (long i = 0; i < 100; i++) {
      final NumericHistogramValue statValue2 = stat.createEmpty();
      for (int j = 0; j < 100; j++) {
        statValue2.entryIngested(
            dataAdapter,
            create(4839000434.547854578 * rand.nextDouble() * rand.nextGaussian()));
      }
      byte[] b = statValue2.toBinary();
      statValue2.fromBinary(b);
      b = statValue.toBinary();
      statValue.fromBinary(b);
      statValue.merge(statValue2);
    }
  }

  @Test
  public void testNegative() {

    final NumericHistogramStatistic stat = new NumericHistogramStatistic("", "pop");
    final NumericHistogramValue statValue = stat.createEmpty();

    final Random rand = new Random(7777);

    statValue.entryIngested(dataAdapter, create(-100.0));
    statValue.entryIngested(dataAdapter, create(-101.0));
    statValue.entryIngested(dataAdapter, create(-2.0));

    double next = -1;
    for (int i = 0; i < 10000; i++) {
      next = next - (Math.round(rand.nextDouble()));
      statValue.entryIngested(dataAdapter, create(next));
    }

    final NumericHistogramValue statValue2 = stat.createEmpty();

    final double start2 = next;

    double min = 0;
    for (long i = 0; i < 10000; i++) {
      final double val = next - (long) (1000 * rand.nextDouble());
      statValue2.entryIngested(dataAdapter, create(val));
      min = Math.min(val, min);
    }
    final double skewvalue = next - (1000 * rand.nextDouble());
    final SimpleFeature skewedFeature = create(skewvalue);
    for (int i = 0; i < 10000; i++) {
      statValue2.entryIngested(dataAdapter, skewedFeature);
    }

    assertEquals(1.0, statValue2.cdf(0), 0.00001);
    final byte[] b = statValue2.toBinary();
    statValue2.fromBinary(b);

    assertEquals(0.0, statValue2.cdf(min), 0.00001);

    statValue.merge(statValue2);

    assertEquals(1.0, statValue.cdf(0), 0.00001);

    assertEquals(0.66, statValue.cdf(start2), 0.01);

    assertEquals(30003, sum(statValue.count(10)));

    final double r = statValue.percentPopulationOverRange(skewvalue - 1, skewvalue + 1);
    assertTrue((r > 0.3) && (r < 0.35));
  }

  @Test
  public void testMix() {

    final NumericHistogramStatistic stat = new NumericHistogramStatistic("", "pop");
    final NumericHistogramValue statValue = stat.createEmpty();

    final Random rand = new Random(7777);

    double min = 0;
    double max = 0;

    double next = 0;
    for (int i = 1; i < 300; i++) {
      final NumericHistogramValue statValue2 = stat.createEmpty();
      final double m = 10000.0 * Math.pow(10.0, ((i / 100) + 1));
      if (i == 50) {
        next = 0.0;
      } else if (i == 100) {
        next = Double.NaN;
      } else if (i == 150) {
        next = Double.MAX_VALUE;
      } else if (i == 200) {
        next = Integer.MAX_VALUE;
      } else if (i == 225) {
        next = Integer.MIN_VALUE;
      } else {
        next = (m * rand.nextDouble() * MathUtils.sign(rand.nextGaussian()));
      }
      statValue2.entryIngested(dataAdapter, create(next));
      if (!Double.isNaN(next)) {
        max = Math.max(next, max);
        min = Math.min(next, min);
        stat.fromBinary(stat.toBinary());
        statValue2.fromBinary(statValue2.toBinary());
        statValue.merge(statValue2);
      }
    }

    assertEquals(0.5, statValue.cdf(0), 0.1);

    assertEquals(0.0, statValue.cdf(min), 0.00001);

    assertEquals(1.0, statValue.cdf(max), 0.00001);

    assertEquals(298, sum(statValue.count(10)));
  }

  private long sum(final long[] list) {
    long result = 0;
    for (final long v : list) {
      result += v;
    }
    return result;
  }
}
