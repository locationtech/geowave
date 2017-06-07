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

public class FeatureFixedBinNumericStaticticsTest
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
				"geometry:Geometry:srid=4326,pop:java.lang.Double,when:Date,whennot:Date,somewhere:Polygon,pid:String");
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

		final FeatureFixedBinNumericStatistics stat = new FeatureFixedBinNumericStatistics(
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

		final FeatureFixedBinNumericStatistics stat2 = new FeatureFixedBinNumericStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		next += 1000;
		final double skewvalue = next + (1000 * rand.nextDouble());
		final SimpleFeature skewedFeature = create(skewvalue);
		for (int i = 0; i < 10000; i++) {
			stat2.entryIngested(
					null,
					skewedFeature);
		}

		next += 1000;
		double max = 0;
		for (long i = 0; i < 10000; i++) {
			final double val = next + (1000 * rand.nextDouble());
			stat2.entryIngested(
					null,
					create(val));
			max = Math.max(
					val,
					max);
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
				.33,
				stat.cdf(skewvalue - 1000),
				0.01);
		assertEquals(
				30003,
				sum(stat.count(10)));

		final double r = stat.percentPopulationOverRange(
				skewvalue - 1000,
				skewvalue + 1000);
		assertTrue((r > 0.45) && (r < 0.55));

		System.out.println(stat.toString());

	}

	@Test
	public void testRapidIncreaseInRange() {

		final FeatureFixedBinNumericStatistics stat1 = new FeatureFixedBinNumericStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final Random rand = new Random(
				7777);
		double next = 1;
		for (int i = 0; i < 10000; i++) {
			next = next + (rand.nextDouble() * 100.0);
			stat1.entryIngested(
					null,
					create(next));
		}

		FeatureFixedBinNumericStatistics stat2 = new FeatureFixedBinNumericStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		next = 4839434.547854578;
		for (long i = 0; i < 10000; i++) {
			final double val = next + (1000.0 * rand.nextDouble());
			stat2.entryIngested(
					null,
					create(val));
		}

		byte[] b = stat2.toBinary();
		stat2.fromBinary(b);

		b = stat1.toBinary();
		stat1.fromBinary(b);

		stat1.merge(stat2);

		stat2 = new FeatureFixedBinNumericStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		for (int i = 0; i < 40000; i++) {
			next = (Math.round(rand.nextDouble()));
			stat2.entryIngested(
					null,
					create(next));
		}

		final FeatureFixedBinNumericStatistics stat3 = new FeatureFixedBinNumericStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		next = 54589058545734.049454545458;
		for (long i = 0; i < 10000; i++) {
			final double val = next + (rand.nextDouble());
			stat3.entryIngested(
					null,
					create(val));
		}

		b = stat2.toBinary();
		stat2.fromBinary(b);

		b = stat3.toBinary();
		stat3.fromBinary(b);

		stat1.merge(stat3);
		stat1.merge(stat2);

		b = stat1.toBinary();
		stat1.fromBinary(b);
		System.out.println(stat1);
	}

	@Test
	public void testMix() {

		final FeatureFixedBinNumericStatistics stat = new FeatureFixedBinNumericStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final Random rand = new Random(
				7777);

		double min = 0;
		double max = 0;

		double next = 0;
		for (int i = 0; i < 10000; i++) {
			next = next + (100 * rand.nextDouble());
			stat.entryIngested(
					null,
					create(next));
			max = Math.max(
					next,
					max);
		}

		next = 0;
		for (int i = 0; i < 10000; i++) {
			next = next - (100 * rand.nextDouble());
			stat.entryIngested(
					null,
					create(next));
			min = Math.min(
					next,
					min);
		}

		assertEquals(
				0.0,
				stat.cdf(min),
				0.00001);

		assertEquals(
				1.0,
				stat.cdf(max),
				0.00001);

		assertEquals(
				0.5,
				stat.cdf(0),
				0.05);

		assertEquals(
				20000,
				sum(stat.count(10)));

		final double r = stat.percentPopulationOverRange(
				min / 2,
				max / 2);

		assertEquals(
				0.5,
				r,
				0.05);

		System.out.println(stat.toString());

	}

	@Test
	public void testMix2() {

		final FeatureFixedBinNumericStatistics stat = new FeatureFixedBinNumericStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pop");

		final Random rand = new Random(
				7777);

		final double min = 0;
		double max = 0;

		double next = 0;
		for (int i = 0; i < 100000; i++) {
			next = 1000 * rand.nextGaussian();
			stat.entryIngested(
					null,
					create(next));
			max = Math.max(
					next,
					max);
		}

		assertEquals(
				1.0,
				stat.cdf(max),
				0.00001);

		assertEquals(
				0.5,
				stat.cdf(0),
				0.05);

		assertEquals(
				100000,
				sum(stat.count(10)));

		final double r = stat.percentPopulationOverRange(
				min / 2,
				max / 2);

		assertEquals(
				0.5,
				r,
				0.05);

		System.out.println(stat.toString());

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
