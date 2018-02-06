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

package mil.nga.giat.geowave.format.osm.types;

import java.util.HashMap;
import java.util.Map;

import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.store.adapter.AdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.data.PersistentDataset;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.format.osm.types.generated.Node;

public class NodeDataAdapter implements
		WritableDataAdapter<Node>
{
	public final static ByteArrayId ADAPTER_ID = new ByteArrayId(
			"node");
	private final static ByteArrayId NODE_FIELD_ID = new ByteArrayId(
			"node");
	private final FieldVisibilityHandler<Node, Object> nodeVisibilityHandler;

	public NodeDataAdapter() {
		this(
				null);
	}

	public NodeDataAdapter(
			final FieldVisibilityHandler<Node, Object> imageChipVisibilityHandler ) {
		this.nodeVisibilityHandler = imageChipVisibilityHandler;
	}

	@Override
	public ByteArrayId getAdapterId() {
		return ADAPTER_ID;
	}

	@Override
	public boolean isSupported(
			final Node entry ) {
		return true;
	}

	@Override
	public ByteArrayId getDataId(
			final Node entry ) {
		return null;
	}

	@Override
	public Node decode(
			final IndexedAdapterPersistenceEncoding data,
			final PrimaryIndex index ) {
		return null;
	}

	@Override
	public AdapterPersistenceEncoding encode(
			final Node entry,
			final CommonIndexModel indexModel ) {
				return null;
		/*final Map<ByteArrayId, Object> fieldIdToValueMap = new HashMap<ByteArrayId, Object>();
		fieldIdToValueMap.put(
				NODE_FIELD_ID,
				entry.getImageBinary());
		return new AdapterPersistenceEncoding(
				getAdapterId(),
				entry.getDataId(),
				new PersistentDataset<CommonIndexValue>(),
				new PersistentDataset<Object>(
						fieldIdToValueMap));*/
	}

	@Override
	public FieldReader<Object> getReader(
			final ByteArrayId fieldId ) {
		if (NODE_FIELD_ID.equals(fieldId)) {
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
	public FieldWriter<Node, Object> getWriter(
			final ByteArrayId fieldId ) {
		if (NODE_FIELD_ID.equals(fieldId)) {
			if (nodeVisibilityHandler != null) {
				return (FieldWriter) FieldUtils.getDefaultWriterForClass(
						byte[].class,
						nodeVisibilityHandler);
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
			final ByteArrayId fieldId ) {
		int i = 0;
		for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
			if (fieldId.equals(dimensionField.getFieldId())) {
				return i;
			}
			i++;
		}
		if (fieldId.equals(NODE_FIELD_ID)) {
			return i;
		}
		return -1;
	}

	@Override
	public ByteArrayId getFieldIdForPosition(
			final CommonIndexModel model,
			final int position ) {
		if (position < model.getDimensions().length) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (i == position) {
					return dimensionField.getFieldId();
				}
				i++;
			}
		}
		else {
			final int numDimensions = model.getDimensions().length;
			if (position == numDimensions) {
				return NODE_FIELD_ID;
			}
		}
		return null;
	}
}
