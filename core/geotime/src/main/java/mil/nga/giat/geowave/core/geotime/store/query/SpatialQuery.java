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
package mil.nga.giat.geowave.core.geotime.store.query;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.opengis.geometry.MismatchedDimensionException;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKBReader;
import com.vividsolutions.jts.io.WKBWriter;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.dimension.CustomCrsIndexModel;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter;
import mil.nga.giat.geowave.core.geotime.store.filter.SpatialQueryFilter.CompareOperation;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.filter.BasicQueryFilter.BasicQueryCompareOperation;
import mil.nga.giat.geowave.core.store.filter.DistributableQueryFilter;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.FilterableConstraints;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.query.BasicQuery;

/**
 * The Spatial Query class represents a query in two dimensions. The constraint
 * that is applied represents an intersection operation on the query geometry.
 *
 */
public class SpatialQuery extends
		BasicQuery
{
	private final static Logger LOGGER = LoggerFactory.getLogger(SpatialQuery.class);

	private static class CrsCache
	{
		Geometry geometry;
		List<MultiDimensionalNumericData> constraints;

		public CrsCache(
				final Geometry geometry,
				final List<MultiDimensionalNumericData> constraints ) {
			this.geometry = geometry;
			this.constraints = constraints;
		}
	}

	private Geometry queryGeometry;
	private String crsCode;
	private CompareOperation compareOp = CompareOperation.INTERSECTS;
	private BasicQueryCompareOperation nonSpatialCompareOp = BasicQueryCompareOperation.INTERSECTS;
	private final Map<String, CrsCache> crsCodeCache = new HashMap<>();
	private CoordinateReferenceSystem crs;

	/**
	 * Convenience constructor used to construct a SpatialQuery object that has
	 * an X and Y dimension (axis).
	 *
	 * @param queryGeometry
	 *            spatial geometry of the query
	 */
	public SpatialQuery(
			final Geometry queryGeometry ) {
		this(
				GeometryUtils.basicConstraintsFromGeometry(queryGeometry),
				queryGeometry);
	}

	public SpatialQuery(
			final Constraints constraints,
			final Geometry queryGeometry ) {
		this(
				constraints,
				queryGeometry,
				Collections.emptyMap());
	}

	protected SpatialQuery(
			final Constraints constraints,
			final Geometry queryGeometry,
			final String crsCode ) {
		this(
				constraints,
				queryGeometry,
				Collections.emptyMap(),
				crsCode,
				CompareOperation.INTERSECTS,
				BasicQueryCompareOperation.INTERSECTS);
	}

	public SpatialQuery(
			final Geometry queryGeometry,
			final Map<ByteArrayId, FilterableConstraints> additionalConstraints ) {
		this(
				GeometryUtils.basicConstraintsFromGeometry(queryGeometry),
				queryGeometry,
				additionalConstraints);
	}

	public SpatialQuery(
			final Geometry queryGeometry,
			final String crsCode ) {
		this(
				GeometryUtils.basicConstraintsFromGeometry(queryGeometry),
				queryGeometry,
				Collections.emptyMap(),
				crsCode,
				CompareOperation.INTERSECTS,
				BasicQueryCompareOperation.INTERSECTS);
	}

	private SpatialQuery(
			final Constraints constraints,
			final Geometry queryGeometry,
			final Map<ByteArrayId, FilterableConstraints> additionalConstraints ) {
		this(
				constraints,
				queryGeometry,
				additionalConstraints,
				null,
				CompareOperation.INTERSECTS,
				BasicQueryCompareOperation.INTERSECTS);
	}

	/**
	 * Convenience constructor used to construct a SpatialQuery object that has
	 * an X and Y dimension (axis).
	 *
	 * @param queryGeometry
	 *            spatial geometry of the query
	 * @param overlaps
	 *            if false, the only fully contained geometries are requested
	 */
	public SpatialQuery(
			final Geometry queryGeometry,
			final CompareOperation compareOp ) {
		this(
				GeometryUtils.basicConstraintsFromGeometry(queryGeometry),
				queryGeometry,
				compareOp);
	}

	/**
	 * Convenience constructor can be used when you already have linear
	 * constraints for the query. The queryGeometry and compareOp is used for
	 * fine grained post filtering.
	 *
	 * @param constraints
	 *            linear constraints
	 * @param queryGeometry
	 *            spatial geometry of the query
	 * @param compareOp
	 *            predicate associated query geometry
	 */
	public SpatialQuery(
			final Constraints constraints,
			final Geometry queryGeometry,
			final CompareOperation compareOp ) {
		this(
				constraints,
				queryGeometry,
				compareOp,
				BasicQueryCompareOperation.INTERSECTS);
	}

	/**
	 * Convenience constructor can be used when you already have linear
	 * constraints for the query. The queryGeometry and compareOp is used for
	 * fine grained post filtering.
	 *
	 * @param constraints
	 *            linear constraints
	 * @param queryGeometry
	 *            spatial geometry of the query
	 * @param compareOp
	 *            predicate associated query geometry
	 * @param nonSpatialCompareOp
	 *            predicate associated non-spatial fields (i.e Time)
	 */
	public SpatialQuery(
			final Constraints constraints,
			final Geometry queryGeometry,
			final CompareOperation compareOp,
			final BasicQueryCompareOperation nonSpatialCompareOp ) {
		this(
				constraints,
				queryGeometry,
				Collections.emptyMap(),
				null,
				compareOp,
				nonSpatialCompareOp);
	}

	public SpatialQuery(
			final Constraints constraints,
			final Geometry queryGeometry,
			final Map<ByteArrayId, FilterableConstraints> additionalConstraints,
			final String crsCode,
			final CompareOperation compareOp,
			final BasicQueryCompareOperation nonSpatialCompareOp ) {
		super(
				constraints,
				nonSpatialCompareOp,
				additionalConstraints);
		this.crsCode = crsCode;
		this.queryGeometry = queryGeometry;
		this.compareOp = compareOp;
		this.nonSpatialCompareOp = nonSpatialCompareOp;
	}

	public SpatialQuery() {
		super();
	}

	/**
	 *
	 * @return queryGeometry the spatial geometry of the SpatialQuery object
	 */
	public Geometry getQueryGeometry() {
		return queryGeometry;
	}

	@Override
	protected DistributableQueryFilter createQueryFilter(
			final MultiDimensionalNumericData constraints,
			final NumericDimensionField<?>[] orderedConstrainedDimensionFields,
			final NumericDimensionField<?>[] unconstrainedDimensionDefinitions,
			final PrimaryIndex index ) {
		return new SpatialQueryFilter(
				constraints,
				orderedConstrainedDimensionFields,
				unconstrainedDimensionDefinitions,
				internalGetGeometry(index),
				compareOp,
				nonSpatialCompareOp);
	}

	private Geometry internalGetGeometry(
			final PrimaryIndex index ) {
		final String indexCrsStr = getCrs(index.getIndexModel());
		CrsCache cache = crsCodeCache.get(indexCrsStr);
		if (cache != null) {
			return cache.geometry;
		}
		cache = transformToIndex(
				indexCrsStr,
				index);
		crsCodeCache.put(
				indexCrsStr,
				cache);
		return cache.geometry;
	}

	@Override
	public List<MultiDimensionalNumericData> getIndexConstraints(
			final PrimaryIndex index ) {
		final String indexCrsStr = getCrs(index.getIndexModel());
		CrsCache cache = crsCodeCache.get(indexCrsStr);
		if (cache != null) {
			return cache.constraints;
		}
		cache = transformToIndex(
				indexCrsStr,
				index);
		crsCodeCache.put(
				indexCrsStr,
				cache);
		return cache.constraints;
	}

	private CrsCache transformToIndex(
			final String indexCrsStr,
			final PrimaryIndex index ) {
		if (crsMatches(
				crsCode,
				indexCrsStr)) {
			return new CrsCache(
					queryGeometry,
					super.getIndexConstraints(index));
		}
		else {
			if (crs == null) {

				if ((crsCode == null) || crsCode.isEmpty()) {
					crsCode = GeometryUtils.DEFAULT_CRS_STR;
				}

				try {
					crs = CRS.decode(
							crsCode,
							true);
				}
				catch (final FactoryException e) {
					LOGGER.warn(
							"Unable to decode spatial query crs",
							e);
				}
			}
			CoordinateReferenceSystem indexCrs;
			if (isDefaultCrs(indexCrsStr)) {
				indexCrs = GeometryUtils.DEFAULT_CRS;
			}
			else {
				indexCrs = ((CustomCrsIndexModel) index.getIndexModel()).getCrs();
			}
			try {
				final MathTransform transform = CRS.findMathTransform(
						crs,
						indexCrs,
						true);
				// transform geometry
				final Geometry indexCrsQueryGeometry = JTS.transform(
						queryGeometry,
						transform);
				return new CrsCache(
						indexCrsQueryGeometry,
						GeometryUtils.basicConstraintsFromGeometry(
								indexCrsQueryGeometry).getIndexConstraints(
								index.getIndexStrategy()));
			}
			catch (final FactoryException e) {
				LOGGER.warn(
						"Unable to create coordinate reference system transform",
						e);
			}
			catch (MismatchedDimensionException | TransformException e) {
				LOGGER.warn(
						"Unable to transform query geometry into index CRS",
						e);
			}
		}
		return new CrsCache(
				queryGeometry,
				super.getIndexConstraints(index));
	}

	private static String getCrs(
			final CommonIndexModel indexModel ) {
		if (indexModel instanceof CustomCrsIndexModel) {
			if (isDefaultCrs(((CustomCrsIndexModel) indexModel).getCrsCode())) {
				return null;
			}
			return ((CustomCrsIndexModel) indexModel).getCrsCode();
		}
		return null;
	}

	private static boolean crsMatches(
			final String crsCode1,
			final String crsCode2 ) {
		if (isDefaultCrs(crsCode1)) {
			return isDefaultCrs(crsCode2);
		}
		else if (isDefaultCrs(crsCode2)) {
			return isDefaultCrs(crsCode1);
		}
		return crsCode1.equalsIgnoreCase(crsCode2);
	}

	private static boolean isDefaultCrs(
			final String crsCode ) {
		return (crsCode == null) || crsCode.isEmpty() || crsCode.equalsIgnoreCase(GeometryUtils.DEFAULT_CRS_STR);
	}

	@Override
	public byte[] toBinary() {
		final byte[] crsBinary = isDefaultCrs(crsCode) ? new byte[0] : StringUtils.stringToBinary(crsCode);
		final byte[] superBinary = super.toBinary();
		final byte[] geometryBinary = new WKBWriter().write(queryGeometry);
		final ByteBuffer buf = ByteBuffer.allocate(superBinary.length + geometryBinary.length + 16);
		buf.putInt(compareOp.ordinal());
		buf.putInt(nonSpatialCompareOp.ordinal());
		buf.putInt(crsBinary.length);
		buf.putInt(superBinary.length);
		buf.put(crsBinary);
		buf.put(superBinary);
		buf.put(geometryBinary);

		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		compareOp = CompareOperation.values()[buf.getInt()];
		nonSpatialCompareOp = BasicQueryCompareOperation.values()[buf.getInt()];

		final byte[] crsBinary = new byte[buf.getInt()];
		final byte[] superBinary = new byte[buf.getInt()];
		buf.get(crsBinary);
		crsCode = crsBinary.length > 0 ? StringUtils.stringFromBinary(crsBinary) : null;
		buf.get(superBinary);
		super.fromBinary(superBinary);
		final byte[] geometryBinary = new byte[bytes.length - superBinary.length - crsBinary.length - 16];
		buf.get(geometryBinary);
		try {
			queryGeometry = new WKBReader().read(geometryBinary);
		}
		catch (final ParseException e) {
			LOGGER.warn(
					"Unable to read query geometry as well-known binary",
					e);
		}
	}
}