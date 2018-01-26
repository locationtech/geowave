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

import org.geotools.filter.FilterFactoryImpl;
import org.geotools.filter.IllegalFilterException;
import org.geotools.filter.LiteralExpressionImpl;
import org.geotools.filter.spatial.DWithinImpl;
import org.geotools.filter.spatial.IntersectsImpl;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.opengis.filter.Filter;
import org.opengis.filter.MultiValuedFilter.MatchAction;
import org.opengis.filter.expression.Expression;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.DWithin;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import mil.nga.giat.geowave.core.geotime.GeometryUtils;

public class FilterToCQLTool
{
	private static Logger LOGGER = LoggerFactory.getLogger(FilterToCQLTool.class);

	public static Filter fixDWithin(
			final Filter filter ) {
		final HasDWithinFilterVisitor dwithinCheck = new HasDWithinFilterVisitor();
		filter.accept(
				dwithinCheck,
				null);
		if (dwithinCheck.hasDWithin()) {
			try {
				final Filter retVal = (Filter) filter.accept(
						new DWithinFilterVisitor(),
						null);
				// We do not have a way to transform a filter directly from one
				// to another.
				return FilterToCQLTool.toFilter(ECQL.toCQL(retVal));
			}
			catch (final CQLException e) {
				LOGGER.trace(
						"Filter is not a CQL Expression",
						e);
			}
		}
		return filter;
	}

	public static Filter toFilter(
			final String expression )
			throws CQLException {
		return ECQL.toFilter(
				expression,
				new FilterFactoryImpl() {
					@Override
					public DWithin dwithin(
							final Expression geometry1,
							final Expression geometry2,
							final double distance,
							final String units,
							final MatchAction matchAction ) {
						try {
							return matchAction == null ? new FixedDWithinImpl(
									geometry1,
									geometry2,
									units,
									distance) : new FixedDWithinImpl(
									geometry1,
									geometry2,
									units,
									distance,
									matchAction);
						}
						catch (IllegalFilterException | TransformException e) {
							LOGGER.warn(
									"Cannot convert DWithin Expression to work with WSG84",
									e);
						}
						final DWithinImpl impl = matchAction == null ? new DWithinImpl(
								geometry1,
								geometry2) : new DWithinImpl(
								geometry1,
								geometry2,
								matchAction);
						impl.setDistance(distance);
						impl.setUnits(units);
						return impl;
					}

					@Override
					public DWithin dwithin(
							final Expression geometry1,
							final Expression geometry2,
							final double distance,
							final String units ) {
						return dwithin(
								geometry1,
								geometry2,
								distance,
								units,
								(MatchAction) null);
					}
				});
	}

	public static final class FixedDWithinImpl extends
			IntersectsImpl implements
			DWithin
	{

		private final double distance;
		private final String units;

		public FixedDWithinImpl(
				final Expression e1,
				final Expression e2,
				final String units,
				final double distance )
				throws IllegalFilterException,
				TransformException {
			super(
					new LiteralExpressionImpl(
							mil.nga.giat.geowave.adapter.vector.utils.FeatureGeometryUtils.buffer(
									getCRS(
											e1,
											e2),
									e1 instanceof PropertyName ? e2.evaluate(
											null,
											com.vividsolutions.jts.geom.Geometry.class) : e1.evaluate(
											null,
											com.vividsolutions.jts.geom.Geometry.class),
									units,
									distance).getLeft()),
					e1 instanceof PropertyName ? e1 : e2);
			this.units = units;
			this.distance = distance;
		}

		private static CoordinateReferenceSystem getCRS(
				final Expression e1,
				final Expression e2 ) {
			return GeometryUtils.DEFAULT_CRS;
		}

		public FixedDWithinImpl(
				final Expression e1,
				final Expression e2,
				final String units,
				final double distance,
				final MatchAction matchAction )
				throws IllegalFilterException,
				TransformException {
			super(
					new LiteralExpressionImpl(
							mil.nga.giat.geowave.adapter.vector.utils.FeatureGeometryUtils.buffer(
									getCRS(
											e1,
											e2),
									e1.evaluate(
											null,
											com.vividsolutions.jts.geom.Geometry.class),
									units,
									distance).getLeft()),
					e2,
					matchAction);
			this.units = units;
			this.distance = distance;
		}

		@Override
		public double getDistance() {
			return distance;
		}

		@Override
		public String getDistanceUnits() {
			return units;
		}
	}

}
