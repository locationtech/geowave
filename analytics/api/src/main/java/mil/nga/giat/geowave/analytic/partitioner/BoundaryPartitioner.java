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
package mil.nga.giat.geowave.analytic.partitioner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.measure.quantity.Length;
import javax.measure.unit.Unit;

import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.extract.DimensionExtractor;
import mil.nga.giat.geowave.analytic.extract.EmptyDimensionExtractor;
import mil.nga.giat.geowave.analytic.extract.SimpleFeatureGeometryExtractor;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.referencing.crs.CoordinateReferenceSystem;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Partition on the boundary of polygons (the hull); not on the interior space.
 * 
 */
public class BoundaryPartitioner extends
		OrthodromicDistancePartitioner<Object>
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 461679322447608507L;
	SimpleFeatureGeometryExtractor extractor = new SimpleFeatureGeometryExtractor();

	public BoundaryPartitioner() {
		super();
	}

	public BoundaryPartitioner(
			CoordinateReferenceSystem crs,
			CommonIndexModel indexModel,
			final DimensionExtractor<Object> dimensionExtractor,
			double[] distancePerDimension,
			Unit<Length> geometricDistanceUnit ) {
		super(
				crs,
				indexModel,
				new EchoExtractor(),
				distancePerDimension,
				geometricDistanceUnit);
	}

	private static class EchoExtractor extends
			EmptyDimensionExtractor<Object> implements
			DimensionExtractor<Object>
	{

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Geometry getGeometry(
				Object anObject ) {
			return (Geometry) anObject;
		}

		@Override
		public String getGroupID(
				Object anObject ) {
			return "g";
		}

	}

	@Override
	public List<PartitionData> getCubeIdentifiers(
			final Object entry ) {
		Geometry geom = extractor.getGeometry((SimpleFeature) entry);
		Coordinate[] coords = (geom.getCoordinates());
		System.out.println(geom.toString());
		if (coords.length < 2)
			return super.getCubeIdentifiers(geom);
		else {
			List<PartitionData> r = new ArrayList<PartitionData>();
			for (int i = 0; i < (coords.length - 1); i++) {
				r.addAll(super.getCubeIdentifiers(geom.getFactory().createLineString(
						new Coordinate[] {
							coords[i],
							coords[i + 1]
						})));
			}
			return r;
		}
	}

	@Override
	public void partition(
			final Object entry,
			final PartitionDataCallback callback )
			throws Exception {
		Geometry geom = extractor.getGeometry((SimpleFeature) entry);
		System.out.println(geom.toString());
		Coordinate[] coords = (geom.getCoordinates());
		if (coords.length < 2)
			super.partition(
					geom,
					callback);
		else {
			for (int i = 0; i < (coords.length - 1); i++) {

				super.partition(
						geom.getFactory().createLineString(
								new Coordinate[] {
									coords[i],
									coords[i + 1]
								}),
						callback);
			}
		}
	}

	@Override
	public void initialize(
			ScopedJobConfiguration config )
			throws IOException {
		super.initialize(config);
		super.dimensionExtractor = new EchoExtractor();
	}

}
