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
package mil.nga.giat.geowave.adapter.vector.query.cql;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialOptions;
import mil.nga.giat.geowave.core.store.filter.DistributableFilterList;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.filter.QueryFilter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.util.DataStoreUtils;

import org.geotools.data.DataUtilities;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.junit.Before;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.opengis.filter.Filter;
import org.opengis.filter.expression.Expression;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;

public class CQLQueryFilterTest
{
	SimpleFeatureType type;

	Object[] defaults;

	GeometryFactory factory = new GeometryFactory();

	@Before
	public void setup()
			throws SchemaException,
			CQLException {
		type = DataUtilities.createType(
				"geostuff",
				"geom:Geometry:srid=4326,pop:java.lang.Long,pid:String");

		final List<AttributeDescriptor> descriptors = type.getAttributeDescriptors();
		defaults = new Object[descriptors.size()];
		int p = 0;
		for (final AttributeDescriptor descriptor : descriptors) {
			defaults[p++] = descriptor.getDefaultValue();
		}

	}

	@Test
	public void test() {
		final FilterFactoryImpl factory = new FilterFactoryImpl();
		final Expression exp1 = factory.property("pid");
		final Expression exp2 = factory.literal("a89dhd-123-abc");
		final Filter f = factory.equal(
				exp1,
				exp2,
				false);
		final PrimaryIndex spatialIndex = new SpatialDimensionalityTypeProvider()
				.createPrimaryIndex(new SpatialOptions());

		final FeatureDataAdapter adapter = new FeatureDataAdapter(
				type);
		adapter.init(spatialIndex);
		final CQLQuery cqlQuery = new CQLQuery(
				null,
				f,
				adapter);

		final List<QueryFilter> filters = cqlQuery.createFilters(spatialIndex);
		final List<DistributableQueryFilter> dFilters = new ArrayList<DistributableQueryFilter>();
		for (final QueryFilter filter : filters) {
			dFilters.add((DistributableQueryFilter) filter);
		}

		final DistributableFilterList dFilterList = new DistributableFilterList(
				dFilters);

		assertTrue(dFilterList.accept(
				spatialIndex.getIndexModel(),
				DataStoreUtils.getEncodings(
						spatialIndex,
						adapter.encode(
								createFeature(),
								spatialIndex.getIndexModel())).get(
						0)));
	}

	private SimpleFeature createFeature() {
		final SimpleFeature instance = SimpleFeatureBuilder.build(
				type,
				defaults,
				UUID.randomUUID().toString());
		instance.setAttribute(
				"pop",
				Long.valueOf(100));
		instance.setAttribute(
				"pid",
				"a89dhd-123-abc");
		instance.setAttribute(
				"geom",
				factory.createPoint(new Coordinate(
						27.25,
						41.25)));
		return instance;
	}
}
