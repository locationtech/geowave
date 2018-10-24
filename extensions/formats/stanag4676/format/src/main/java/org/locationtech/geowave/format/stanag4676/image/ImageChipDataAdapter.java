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
package org.locationtech.geowave.format.stanag4676.image;

import java.util.HashMap;
import java.util.Map;

import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;

public class ImageChipDataAdapter implements
		DataTypeAdapter<ImageChip>
{
	public final static String ADAPTER_TYPE_NAME = "image";
	private final static String IMAGE_FIELD_NAME = "image";
	private final FieldVisibilityHandler<ImageChip, Object> imageChipVisibilityHandler;

	public ImageChipDataAdapter() {
		this(
				null);
	}

	public ImageChipDataAdapter(
			final FieldVisibilityHandler<ImageChip, Object> imageChipVisibilityHandler ) {
		this.imageChipVisibilityHandler = imageChipVisibilityHandler;
	}

	@Override
	public String getTypeName() {
		return ADAPTER_TYPE_NAME;
	}

	@Override
	public ByteArray getDataId(
			final ImageChip entry ) {
		return entry.getDataId();
	}

	@Override
	public ImageChip decode(
			final IndexedAdapterPersistenceEncoding data,
			final Index index ) {
		return ImageChipUtils.fromDataIdAndValue(
				data.getDataId(),
				(byte[]) data.getAdapterExtendedData().getValue(
						IMAGE_FIELD_NAME));
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final ImageChip entry,
			final CommonIndexModel indexModel ) {
		final Map<String, Object> fieldIdToValueMap = new HashMap<>();
		fieldIdToValueMap.put(
				IMAGE_FIELD_NAME,
				entry.getImageBinary());
		return new AdapterPersistenceEncoding(
				entry.getDataId(),
				new PersistentDataset<CommonIndexValue>(),
				new PersistentDataset<>(
						fieldIdToValueMap));
	}

	@Override
	public FieldReader<Object> getReader(
			final String fieldId ) {
		if (IMAGE_FIELD_NAME.equals(fieldId)) {
			return (FieldReader) FieldUtils.getDefaultReaderForClass(byte[].class);
		}
		return null;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {}

	@Override
	public FieldWriter<ImageChip, Object> getWriter(
			final String fieldId ) {
		if (IMAGE_FIELD_NAME.equals(fieldId)) {
			if (imageChipVisibilityHandler != null) {
				return (FieldWriter) FieldUtils.getDefaultWriterForClass(
						byte[].class,
						imageChipVisibilityHandler);
			}
			else {
				return (FieldWriter) FieldUtils.getDefaultWriterForClass(byte[].class);
			}
		}
		return null;
	}

	@Override
	public int getPositionOfOrderedField(
			final CommonIndexModel model,
			final String fieldId ) {
		int i = 0;
		for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
			if (fieldId.equals(dimensionField.getFieldName())) {
				return i;
			}
			i++;
		}
		if (fieldId.equals(IMAGE_FIELD_NAME)) {
			return i;
		}
		return -1;
	}

	@Override
	public String getFieldNameForPosition(
			final CommonIndexModel model,
			final int position ) {
		if (position < model.getDimensions().length) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (i == position) {
					return dimensionField.getFieldName();
				}
				i++;
			}
		}
		else {
			final int numDimensions = model.getDimensions().length;
			if (position == numDimensions) {
				return IMAGE_FIELD_NAME;
			}
		}
		return null;
	}
}
