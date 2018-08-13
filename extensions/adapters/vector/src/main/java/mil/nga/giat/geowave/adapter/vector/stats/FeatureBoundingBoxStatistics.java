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

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

import mil.nga.giat.geowave.adapter.vector.util.FeatureDataUtils;
import mil.nga.giat.geowave.core.geotime.store.statistics.BoundingBoxDataStatistics;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.InternalAdapterStore;
import mil.nga.giat.geowave.core.store.adapter.statistics.DataStatistics;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class FeatureBoundingBoxStatistics extends
		BoundingBoxDataStatistics<SimpleFeature> implements
		FeatureStatistic
{

	private static final ByteArrayId STATS_TYPE = new ByteArrayId(
			"FEATURE_BBOX");

	private SimpleFeatureType reprojectedType;
	private MathTransform transform;

	public FeatureBoundingBoxStatistics() {
		super();
	}

	public FeatureBoundingBoxStatistics(
			final String fieldName ) {
		this(
				null,
				fieldName);
	}

	public FeatureBoundingBoxStatistics(
			final Short internalDataAdapterId,
			final String fieldName ) {
		this(
				internalDataAdapterId,
				fieldName,
				null,
				null);
	}

	public FeatureBoundingBoxStatistics(
			final String fieldName,
			final SimpleFeatureType reprojectedType,
			final MathTransform transform ) {
		this(
				null,
				fieldName,
				reprojectedType,
				transform);
	}

	public FeatureBoundingBoxStatistics(
			final Short internalDataAdapterId,
			final String fieldName,
			final SimpleFeatureType reprojectedType,
			final MathTransform transform ) {
		super(
				internalDataAdapterId,
				composeId(
						STATS_TYPE.getString(),
						fieldName));
		this.reprojectedType = reprojectedType;
		this.transform = transform;
	}

	public static final ByteArrayId composeId(
			final String fieldName ) {
		return composeId(
				STATS_TYPE.getString(),
				fieldName);
	}

	@Override
	public String getFieldName() {
		return decomposeNameFromId(getStatisticsId());
	}

	public Geometry composeGeometry(
			final CoordinateReferenceSystem system ) {
		final Envelope bounds = new Envelope(
				getMinX(),
				getMaxX(),
				getMinY(),
				getMaxY());

		return new GeometryFactory().toGeometry(bounds);
	}

	@Override
	protected Envelope getEnvelope(
			final SimpleFeature entry ) {
		// incorporate the bounding box of the entry's envelope
		final Object o;
		if ((reprojectedType != null) && (transform != null) && !reprojectedType.getCoordinateReferenceSystem().equals(
				entry.getType().getCoordinateReferenceSystem())) {
			o = FeatureDataUtils.crsTransform(
					entry,
					reprojectedType,
					transform).getAttribute(
					getFieldName());
		}
		else {
			o = entry.getAttribute(getFieldName());
		}
		if ((o != null) && (o instanceof Geometry)) {
			final Geometry geometry = (Geometry) o;
			if (!geometry.isEmpty()) {
				return geometry.getEnvelopeInternal();
			}
		}
		return null;
	}

	@Override
	public DataStatistics<SimpleFeature> duplicate() {
		return new FeatureBoundingBoxStatistics(
				internalDataAdapterId,
				getFieldName(),
				reprojectedType,
				transform);
	}

	@Override
	public String toString() {
		final StringBuffer buffer = new StringBuffer();
		buffer.append(
				"bbox[internalAdapter=").append(
				super.getInternalDataAdapterId());
		buffer.append(
				", field=").append(
				getFieldName());
		if (isSet()) {
			buffer.append(
					", minX=").append(
					minX);
			buffer.append(
					", maxX=").append(
					maxX);
			buffer.append(
					", minY=").append(
					minY);
			buffer.append(
					", maxY=").append(
					maxY);
		}
		else {
			buffer.append(", No Values");
		}
		buffer.append("]");
		return buffer.toString();
	}

	/**
	 * Convert Feature Bounding Box statistics to a JSON object
	 */

	@Override
	public JSONObject toJSONObject(
			final InternalAdapterStore store )
			throws JSONException {
		final JSONObject jo = new JSONObject();
		jo.put(
				"type",
				STATS_TYPE.getString());
		jo.put(
				"dataAdapterID",
				store.getAdapterId(internalDataAdapterId));
		jo.put(
				"statisticsId",
				statisticsId.getString());

		jo.put(
				"field_identifier",
				getFieldName());

		if (isSet()) {
			jo.put(
					"minX",
					minX);
			jo.put(
					"maxX",
					maxX);
			jo.put(
					"minY",
					minY);
			jo.put(
					"maxY",
					maxY);
		}
		else {
			jo.put(
					"boundaries",
					"No Values");
		}
		return jo;
	}
}
