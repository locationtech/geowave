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
package mil.nga.giat.geowave.format.landsat8;

import org.apache.commons.lang.ArrayUtils;
import org.geotools.data.DataUtilities;
import org.geotools.filter.visitor.DuplicatingFilterVisitor;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.filter.Filter;
import org.opengis.filter.PropertyIsBetween;
import org.opengis.filter.PropertyIsEqualTo;
import org.opengis.filter.PropertyIsGreaterThan;
import org.opengis.filter.PropertyIsGreaterThanOrEqualTo;
import org.opengis.filter.PropertyIsLessThan;
import org.opengis.filter.PropertyIsLessThanOrEqualTo;
import org.opengis.filter.PropertyIsLike;
import org.opengis.filter.PropertyIsNil;
import org.opengis.filter.PropertyIsNotEqualTo;
import org.opengis.filter.PropertyIsNull;
import org.opengis.filter.spatial.BBOX;
import org.opengis.filter.spatial.Beyond;
import org.opengis.filter.spatial.Contains;
import org.opengis.filter.spatial.Crosses;
import org.opengis.filter.spatial.DWithin;
import org.opengis.filter.spatial.Disjoint;
import org.opengis.filter.spatial.Equals;
import org.opengis.filter.spatial.Intersects;
import org.opengis.filter.spatial.Overlaps;
import org.opengis.filter.spatial.Touches;
import org.opengis.filter.spatial.Within;
import org.opengis.filter.temporal.After;
import org.opengis.filter.temporal.AnyInteracts;
import org.opengis.filter.temporal.Before;
import org.opengis.filter.temporal.Begins;
import org.opengis.filter.temporal.BegunBy;
import org.opengis.filter.temporal.During;
import org.opengis.filter.temporal.EndedBy;
import org.opengis.filter.temporal.Ends;
import org.opengis.filter.temporal.Meets;
import org.opengis.filter.temporal.MetBy;
import org.opengis.filter.temporal.OverlappedBy;
import org.opengis.filter.temporal.TContains;
import org.opengis.filter.temporal.TEquals;
import org.opengis.filter.temporal.TOverlaps;

public class PropertyIgnoringFilterVisitor extends
		DuplicatingFilterVisitor
{
	private final String[] validPropertyNames;
	private final SimpleFeatureType type;

	public PropertyIgnoringFilterVisitor(
			final String[] validPropertyNames,
			final SimpleFeatureType type ) {
		this.validPropertyNames = validPropertyNames;
		this.type = type;
	}

	@Override
	public Object visit(
			final PropertyIsBetween filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final PropertyIsEqualTo filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final PropertyIsNotEqualTo filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final PropertyIsGreaterThan filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final PropertyIsGreaterThanOrEqualTo filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final PropertyIsLessThan filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final PropertyIsLessThanOrEqualTo filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final PropertyIsLike filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final PropertyIsNull filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final PropertyIsNil filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final BBOX filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Beyond filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Contains filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Crosses filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Disjoint filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final DWithin filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Equals filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Intersects filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Overlaps filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Touches filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final Within filter,
			final Object extraData ) {
		if (!usesProperty(filter)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				filter,
				extraData);
	}

	@Override
	public Object visit(
			final After after,
			final Object extraData ) {
		if (!usesProperty(after)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				after,
				extraData);
	}

	@Override
	public Object visit(
			final AnyInteracts anyInteracts,
			final Object extraData ) {
		if (!usesProperty(anyInteracts)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				anyInteracts,
				extraData);
	}

	@Override
	public Object visit(
			final Before before,
			final Object extraData ) {
		if (!usesProperty(before)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				before,
				extraData);
	}

	@Override
	public Object visit(
			final Begins begins,
			final Object extraData ) {
		if (!usesProperty(begins)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				begins,
				extraData);
	}

	@Override
	public Object visit(
			final BegunBy begunBy,
			final Object extraData ) {
		if (!usesProperty(begunBy)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				begunBy,
				extraData);
	}

	@Override
	public Object visit(
			final During during,
			final Object extraData ) {
		if (!usesProperty(during)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				during,
				extraData);
	}

	@Override
	public Object visit(
			final EndedBy endedBy,
			final Object extraData ) {
		if (!usesProperty(endedBy)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				endedBy,
				extraData);
	}

	@Override
	public Object visit(
			final Ends ends,
			final Object extraData ) {
		if (!usesProperty(ends)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				ends,
				extraData);
	}

	@Override
	public Object visit(
			final Meets meets,
			final Object extraData ) {
		if (!usesProperty(meets)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				meets,
				extraData);
	}

	@Override
	public Object visit(
			final MetBy metBy,
			final Object extraData ) {
		if (!usesProperty(metBy)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				metBy,
				extraData);
	}

	@Override
	public Object visit(
			final OverlappedBy overlappedBy,
			final Object extraData ) {
		if (!usesProperty(overlappedBy)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				overlappedBy,
				extraData);
	}

	@Override
	public Object visit(
			final TContains contains,
			final Object extraData ) {
		if (!usesProperty(contains)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				contains,
				extraData);
	}

	@Override
	public Object visit(
			final TEquals equals,
			final Object extraData ) {
		if (!usesProperty(equals)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				equals,
				extraData);
	}

	@Override
	public Object visit(
			final TOverlaps contains,
			final Object extraData ) {
		if (!usesProperty(contains)) {
			return Filter.INCLUDE;
		}
		return super.visit(
				contains,
				extraData);
	}

	private boolean usesProperty(
			final Filter filter ) {
		final String[] attributes = DataUtilities.attributeNames(
				filter,
				type);
		// rely on best scene aggregation at a higher level if the filter is
		// using attributes not contained in the scene

		for (final String attr : attributes) {
			if (!ArrayUtils.contains(
					validPropertyNames,
					attr)) {
				return false;
			}
		}
		return true;
	}
}
