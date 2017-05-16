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
import java.util.List;
import java.util.Locale;
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

import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.PrecisionModel;

public class FeatureCountMinSketchStaticticsTest
{

	private SimpleFeatureType schema;
	FeatureDataAdapter dataAdapter;

	private final String sample = "The construction of global warming [Source: CHE] Climate warming, whatever one concludes about its effect on the earth, is insufficiently understood as a concept that has been constructed by scientists, politicians and others, argues David Demerrit, a lecturer in geography at King's College London, in an exchange with Stephen H. Schneider, a professor of biological sciences at Stanford University. Many observers consider the phenomenon's construction -- as a global-scale environmental problem caused by the universal physical properties of greenhouse gases -- to be reductionist, Mr. Demerrit writes. Yet this reductionist formulation serves a variety of political purposes, including obscuring the role of rich nations in producing the vast majority of the greenhouse gases."
			+ "Mr. Demerrit says his objective is to unmask the ways that scientific judgments "
			+ "have both reinforced and been reinforced by certain political considerations about managing"
			+ "global warming. Scientific uncertainty, he suggests, is emphasized in a way that reinforces dependence on experts. He is skeptical of efforts to increase public technical knowledge of the phenomenon, and instead urges efforts to increase public understanding of and therefore trust in the social process through which the facts are scientifically determined."
			+ "In response, Mr. Schneider agrees that the conclusion that science is at least partially socially constructed, even if still news to some scientists, is clearly established."
			+ "He bluntly states, however, that if scholars in the social studies of science are to be heard by more scientists, they will have to be careful to back up all social theoretical assertions with large numbers of broadly representative empirical examples."
			+ " Mr. Schneider also questions Mr. Demerrit's claim that scientists are motivated by politics to conceive of climate warming as a global problem rather than one created primarily by rich nations: Most scientists are woefully unaware of the social context of the implications of their work and are too naive to be politically conspiratorial He says: What needs to be done is to go beyond platitudes about values embedded in science and to show explicitly, via many detailed and representative empirical examples, precisely how those social factors affected the outcome, and how it might have been otherwise if the process were differently constructed. The exchange is available online to subscribers of the journal at http://www.blackwellpublishers.co.uk/journals/anna";
	final String[] pidSet = sample.toLowerCase(
			Locale.ENGLISH).replaceAll(
			"[,.:\\[\\]']",
			"").split(
			" ");
	final GeometryFactory factory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FIXED));

	@Before
	public void setup()
			throws SchemaException,
			CQLException,
			ParseException {
		schema = DataUtilities.createType(
				"sp.geostuff",
				"geometry:Geometry:srid=4326,pid:String");
		dataAdapter = new FeatureDataAdapter(
				schema,
				new GlobalVisibilityHandler<SimpleFeature, Object>(
						"default"));
	}

	final Random rnd = new Random(
			7733);

	private SimpleFeature create() {
		return create(pidSet[Math.abs(rnd.nextInt()) % pidSet.length]);

	}

	private SimpleFeature create(
			final String pid ) {
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
				"pid",
				pid);

		return newFeature;
	}

	@Test
	public void test() {

		final FeatureCountMinSketchStatistics stat = new FeatureCountMinSketchStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pid");

		for (int i = 0; i < 10000; i++) {
			stat.entryIngested(
					null,
					create());
		}
		stat.entryIngested(
				null,
				create("barney"));

		final FeatureCountMinSketchStatistics stat2 = new FeatureCountMinSketchStatistics(
				new ByteArrayId(
						"sp.geostuff"),
				"pid");

		for (int i = 0; i < 10000; i++) {
			stat2.entryIngested(
					null,
					create());
		}

		stat2.entryIngested(
				null,
				create("global"));
		stat2.entryIngested(
				null,
				create("fred"));

		assertTrue(stat2.count("global") > 0);
		assertTrue(stat2.count("fred") > 0);
		assertTrue(stat.count("fred") == 0);
		assertTrue(stat.count("barney") > 0);
		assertTrue(stat2.count("barney") == 0);

		stat.merge(stat2);
		assertTrue(stat2.count("global") > 0);
		assertTrue(stat2.count("fred") > 0);

		stat2.fromBinary(stat.toBinary());
		assertTrue(stat2.count("barney") > 0);

		assertEquals(
				stat2.toString(),
				stat.toString());
		System.out.println(stat.toString());
	}

}
