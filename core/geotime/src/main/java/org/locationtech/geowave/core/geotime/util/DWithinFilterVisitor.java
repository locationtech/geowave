/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.core.geotime.util;

import org.apache.commons.lang3.tuple.Pair;
import org.geotools.filter.LiteralExpressionImpl;
import org.geotools.filter.spatial.IntersectsImpl;
import org.geotools.filter.visitor.DuplicatingFilterVisitor;
import org.opengis.filter.expression.Literal;
import org.opengis.filter.expression.PropertyName;
import org.opengis.filter.spatial.DWithin;
import org.opengis.referencing.operation.TransformException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Geometry;

public class DWithinFilterVisitor extends
		DuplicatingFilterVisitor
{
	private static final Logger LOGGER = LoggerFactory.getLogger(DWithinFilterVisitor.class);

	/**
	 * DWithin spatial operator will find out if a feature in a datalayer is
	 * within X meters of a point, line, or polygon.
	 */
	@Override
	public Object visit(
			final DWithin filter,
			final Object extraData ) {
		IntersectsImpl newWithImpl = null;
		try {
			if ((filter.getExpression1() instanceof PropertyName) && (filter.getExpression2() instanceof Literal)) {
				Pair<Geometry, Double> geometryAndDegrees;

				geometryAndDegrees = GeometryUtils.buffer(
						GeometryUtils.getDefaultCRS(),
						filter.getExpression2().evaluate(
								extraData,
								Geometry.class),
						filter.getDistanceUnits(),
						filter.getDistance());

				newWithImpl = new IntersectsImpl(
						filter.getExpression1(),
						new LiteralExpressionImpl(
								geometryAndDegrees.getLeft()));

			}
			else if ((filter.getExpression2() instanceof PropertyName) && (filter.getExpression1() instanceof Literal)) {
				final Pair<Geometry, Double> geometryAndDegrees = GeometryUtils.buffer(
						GeometryUtils.getDefaultCRS(),
						filter.getExpression1().evaluate(
								extraData,
								Geometry.class),
						filter.getDistanceUnits(),
						filter.getDistance());
				newWithImpl = new IntersectsImpl(
						new LiteralExpressionImpl(
								geometryAndDegrees.getLeft()),
						filter.getExpression2());
			}
		}
		catch (final TransformException e) {
			LOGGER.error(
					"Cannot transform geoemetry to support provide distance",
					e);
			return super.visit(
					filter,
					extraData);
		}
		return newWithImpl;
	}
}
