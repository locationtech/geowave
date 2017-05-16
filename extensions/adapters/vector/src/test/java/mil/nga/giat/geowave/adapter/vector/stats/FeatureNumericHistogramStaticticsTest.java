/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.adapter.vector.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.data.visibility.GlobalVisibilityHandler;

import org.apache.commons.math.util.MathUtils;
import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class FeatureNumericHistogramStaticticsTest
{

	private SimpleFeatureType schema;
	FeatureDataAdapter dataAdapter;
	GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	@Before
	public void setup()
			throws SchemaException,
			CQLException,
			ParseException {
		schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pop:java.lang.Long,when:Date,whennot:Date,somewhere:Polygon,pid:String");
		dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
	}

	private SimpleFeature create(
			final Double val ) {
		final List<AttributeDescriptor> descriptors = schema.getAttributeDescriptors();
		final Object[] defaults = new Object[descriptors.size()];
		int p = 0;
		for (final AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

		final SimpleFeature newFeature = SimpleFeatureBuilder.build(
				schema,
				defaults,
				UUID.randomUUID().toString());

		newFeature.setAttribute(
				"pop",
				val);
		newFeature.setAttribute(
				"pid",
				UUID.randomUUID().toString());
		newFeature.setAttribute(
				"when",
				new Date());
		newFeature.setAttribute(
				"whennot",
				new Date());
		newFeature.setAttribute(
				"geometry",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));
		return newFeature;
	}

	@Test
	public void testPositive() {

		final FeatureNumericHistogramStatistics stat = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final Random rand = new Random(
				7777);

		stat.entryIngested(
				null,
				create(100.0));
		stat.entryIngested(
				null,
				create(101.0));
		stat.entryIngested(
				null,
				create(2.0));

		double next = 1;
		for (int i = 0; i < 10000; i++) {
			next = next + (Math.round(rand.nextDouble()));
			stat.entryIngested(
					null,
					create(next));
		}

		final FeatureNumericHistogramStatistics stat2 = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final double start2 = next;

		double max = 0;
		for (long i = 0; i < 10000; i++) {
			final double val = next + 1000 * rand.nextDouble();
			stat2.entryIngested(
					null,
					create(val));
			max = Math.max(
					val,
					max);
		}
		final double skewvalue = next + 1000 * rand.nextDouble();
		final SimpleFeature skewedFeature = create(skewvalue);
		for (int i = 0; i < 10000; i++) {
			stat2.entryIngested(
					null,
					skewedFeature);
			// skewedFeature.setAttribute("pop", Long.valueOf(next + (long)
			// (1000 * rand.nextDouble())));
		}

		final byte[] b = stat2.toBinary();
		stat2.fromBinary(b);
		assertEquals(
				1.0,
				stat2.cdf(max + 1),
				0.00001);

		stat.merge(stat2);

		assertEquals(
				1.0,
				stat.cdf(max + 1),
				0.00001);

		assertEquals(
				0.33,
				stat.cdf(start2),
				0.01);

		assertEquals(
				30003,
				sum(stat.count(10)));

		final double r = stat.percentPopulationOverRange(
				skewvalue - 1,
				skewvalue + 1);
		assertTrue((r > 0.3) && (r < 0.35));

		System.out.println(stat.toString());

	}

	@Test
	public void testRapidIncreaseInRange() {

		final FeatureNumericHistogramStatistics stat1 = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final Random rand = new Random(
				7777);
		double next = 1;
		for (int i = 0; i < 100; i++) {
			next = next + (rand.nextDouble() * 100.0);
			stat1.entryIngested(
					null,
					create(next));
		}

		for (long i = 0; i < 100; i++) {
			final FeatureNumericHistogramStatistics stat2 = new FeatureNumericHistogramStatistics(
					new ByteArrayId(
							"sp.geostuff"),
					"pop");
			for (int j = 0; j < 100; j++) {
				stat2.entryIngested(
						null,
						create(4839000434.547854578 * rand.nextDouble() * rand.nextGaussian()));
			}
			byte[] b = stat2.toBinary();
			stat2.fromBinary(b);
			b = stat1.toBinary();
			stat1.fromBinary(b);
			stat1.merge(stat2);
		}

		System.out.println(stat1);
	}

	@Test
	public void testNegative() {

		final FeatureNumericHistogramStatistics stat = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final Random rand = new Random(
				7777);

		stat.entryIngested(
				null,
				create(-100.0));
		stat.entryIngested(
				null,
				create(-101.0));
		stat.entryIngested(
				null,
				create(-2.0));

		double next = -1;
		for (int i = 0; i < 10000; i++) {
			next = next - (Math.round(rand.nextDouble()));
			stat.entryIngested(
					null,
					create(next));
		}

		final FeatureNumericHistogramStatistics stat2 = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final double start2 = next;

		double min = 0;
		for (long i = 0; i < 10000; i++) {
			final double val = next - (long) (1000 * rand.nextDouble());
			stat2.entryIngested(
					null,
					create(val));
			min = Math.min(
					val,
					min);
		}
		final double skewvalue = next - 1000 * rand.nextDouble();
		final SimpleFeature skewedFeature = create(skewvalue);
		for (int i = 0; i < 10000; i++) {
			stat2.entryIngested(
					null,
					skewedFeature);
		}

		assertEquals(
				1.0,
				stat2.cdf(0),
				0.00001);
		final byte[] b = stat2.toBinary();
		stat2.fromBinary(b);

		assertEquals(
				0.0,
				stat2.cdf(min),
				0.00001);

		stat.merge(stat2);

		assertEquals(
				1.0,
				stat.cdf(0),
				0.00001);

		assertEquals(
				0.66,
				stat.cdf(start2),
				0.01);

		assertEquals(
				30003,
				sum(stat.count(10)));

		final double r = stat.percentPopulationOverRange(
				skewvalue - 1,
				skewvalue + 1);
		assertTrue((r > 0.3) && (r < 0.35));

		System.out.println(stat.toString());

	}

	@Test
	public void testMix() {

		final FeatureNumericHistogramStatistics stat = new FeatureNumericHistogramStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final Random rand = new Random(
				7777);

		double min = 0;
		double max = 0;

		double next = 0;
		for (int i = 1; i < 300; i++) {
			final FeatureNumericHistogramStatistics stat2 = new FeatureNumericHistogramStatistics(
					new ByteArrayId(
							"sp.geostuff"),
					"pop");
			final double m = 10000.0 * Math.pow(
					10.0,
					((i / 100) + 1));
			if (i == 50) {
				System.out.println("1");
				next = 0.0;
			}
			else if (i == 100) {
				System.out.println("2");
				next = Double.NaN;
			}
			else if (i == 150) {
				System.out.println("3");
				next = Double.MAX_VALUE;
			}
			else if (i == 200) {
				System.out.println("4");
				next = Integer.MAX_VALUE;
			}
			else if (i == 225) {
				System.out.println("");
				next = Integer.MIN_VALUE;
			}
			else {
				next = (m * rand.nextDouble() * MathUtils.sign(rand.nextGaussian()));
			}
			stat2.entryIngested(
					null,
					create(next));
			if (!Double.isNaN(next)) {
				max = Math.max(
						next,
						max);
				min = Math.min(
						next,
						min);
				stat.fromBinary(stat.toBinary());
				stat2.fromBinary(stat2.toBinary());
				stat.merge(stat2);
			}

		}

		assertEquals(
				0.5,
				stat.cdf(0),
				0.1);

		assertEquals(
				0.0,
				stat.cdf(min),
				0.00001);

		assertEquals(
				1.0,
				stat.cdf(max),
				0.00001);

		assertEquals(
				297,
				sum(stat.count(10)));

	}

	private long sum(
			final long[] list ) {
		long result = 0;
		for (final long v : list) {
			result += v;
		}
		return result;
	}
}
