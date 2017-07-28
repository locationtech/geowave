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
package mil.nga.giat.geowave.adapter.vector;

import java.util.ArrayList;
import java.util.List;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.data.visibility.VisibilityManagement;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

/**
 */
public class AvroFeatureDataAdapter extends
		FeatureDataAdapter
{

	protected AvroFeatureDataAdapter() {}

	public AvroFeatureDataAdapter(
			final SimpleFeatureType type ) {
		super(
				type,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>());
	}

	public AvroFeatureDataAdapter(
			final SimpleFeatureType type,
			final VisibilityManagement<SimpleFeature> visibilityManagement ) {
		super(
				type,
				new ArrayList<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>>(),
				null,
				visibilityManagement);
	}

	public AvroFeatureDataAdapter(
			final SimpleFeatureType type,
			final List<PersistentIndexFieldHandler<SimpleFeature, ? extends CommonIndexValue, Object>> customIndexHandlers ) {
		super(
				type,
				customIndexHandlers);
	}

	public AvroFeatureDataAdapter(
			final SimpleFeatureType type,
			final FieldVisibilityHandler<SimpleFeature, Object> fieldVisiblityHandler ) {
		super(
				type,
				fieldVisiblityHandler);
	}

	@Override
	protected List<NativeFieldHandler<SimpleFeature, Object>> getFieldHandlersFromFeatureType(
			final SimpleFeatureType type ) {
		final List<NativeFieldHandler<SimpleFeature, Object>> nativeHandlers = new ArrayList<NativeFieldHandler<SimpleFeature, Object>>(
				1);

		nativeHandlers.add(new AvroFeatureAttributeHandler());
		return nativeHandlers;
	}

	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		if (fieldId.equals(AvroFeatureAttributeHandler.FIELD_ID)) {
			return new AvroFeatureReader();
		}
		return super.getReader(fieldId);
	}

	@Override
	public FieldWriter<SimpleFeature, Object> getWriter(
			final ByteArrayId fieldId ) {
		if (fieldId.equals(AvroFeatureAttributeHandler.FIELD_ID)) {
			return new AvroFeatureWriter();
		}
		return super.getWriter(fieldId);
	}

	@Override
	public int getPositionOfOrderedField(
			final CommonIndexModel model,
			final ByteArrayId fieldId ) {

		if (fieldId.equals(AvroFeatureAttributeHandler.FIELD_ID)) {
			final List<ByteArrayId> dimensionFieldIds = getDimensionFieldIds(model);
			return dimensionFieldIds.size();
		}
		return super.getPositionOfOrderedField(
				model,
				fieldId);
	}

	@Override
	public ByteArrayId getFieldIdForPosition(
			final CommonIndexModel model,
			final int position ) {
		final List<ByteArrayId> dimensionFieldIds = getDimensionFieldIds(model);
		if (position < dimensionFieldIds.size()) {
			return dimensionFieldIds.get(position);
		}
		else if (position == dimensionFieldIds.size()) {
			return AvroFeatureAttributeHandler.FIELD_ID;
		}
		return super.getFieldIdForPosition(
				model,
				position);
	}

	@Override
	protected RowBuilder<SimpleFeature, Object> newBuilder() {
		return new AvroAttributeRowBuilder();
	}
}