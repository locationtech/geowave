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
package mil.nga.giat.geowave.analytic.mapreduce.kmeans;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.core.geotime.store.dimension.GeometryWrapper;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.persist.Persistable;
import mil.nga.giat.geowave.core.store.adapter.AbstractDataAdapter;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler.RowBuilder;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.PersistentValue;
import mil.nga.giat.geowave.core.store.data.field.FieldReader;
import mil.nga.giat.geowave.core.store.data.field.FieldUtils;
import mil.nga.giat.geowave.core.store.data.field.FieldWriter;
import mil.nga.giat.geowave.core.store.dimension.NumericDimensionField;
import mil.nga.giat.geowave.core.store.index.CommonIndexModel;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.mapreduce.HadoopDataAdapter;
import mil.nga.giat.geowave.mapreduce.HadoopWritableSerializer;

import com.vividsolutions.jts.geom.Geometry;

public class TestObjectDataAdapter extends
		AbstractDataAdapter<TestObject> implements
		HadoopDataAdapter<TestObject, TestObjectWritable>
{
	private static final ByteArrayId GEOM = new ByteArrayId(
			"myGeo");
	private static final ByteArrayId ID = new ByteArrayId(
			"myId");
	private static final ByteArrayId GROUP_ID = new ByteArrayId(
			"myGroupId");

	private static final PersistentIndexFieldHandler<TestObject, ? extends CommonIndexValue, Object> GEOM_FIELD_HANDLER = new PersistentIndexFieldHandler<TestObject, CommonIndexValue, Object>() {

		@Override
		public ByteArrayId[] getNativeFieldIds() {
			return new ByteArrayId[] {
				GEOM
			};
		}

		@Override
		public CommonIndexValue toIndexValue(
				final TestObject row ) {
			return new GeometryWrapper(
					row.geo,
					new byte[0]);
		}

		@Override
		public PersistentValue<Object>[] toNativeValues(
				final CommonIndexValue indexValue ) {
			return new PersistentValue[] {
				new PersistentValue<Object>(
						GEOM,
						((GeometryWrapper) indexValue).getGeometry())
			};
		}

		@Override
		public byte[] toBinary() {
			return new byte[0];
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {

		}
	};

	private static final NativeFieldHandler<TestObject, Object> ID_FIELD_HANDLER = new NativeFieldHandler<TestObject, Object>() {

		@Override
		public ByteArrayId getFieldId() {
			return ID;
		}

		@Override
		public Object getFieldValue(
				final TestObject row ) {
			return row.id;
		}
	};

	private static final NativeFieldHandler<TestObject, Object> GROUP_ID_FIELD_HANDLER = new NativeFieldHandler<TestObject, Object>() {

		@Override
		public ByteArrayId getFieldId() {
			return GROUP_ID;
		}

		@Override
		public Object getFieldValue(
				final TestObject row ) {
			return row.groupID;
		}
	};

	private static final List<NativeFieldHandler<TestObject, Object>> NATIVE_FIELD_HANDLER_LIST = new ArrayList<NativeFieldHandler<TestObject, Object>>();
	private static final List<PersistentIndexFieldHandler<TestObject, ? extends CommonIndexValue, Object>> COMMON_FIELD_HANDLER_LIST = new ArrayList<PersistentIndexFieldHandler<TestObject, ? extends CommonIndexValue, Object>>();
	static {
		COMMON_FIELD_HANDLER_LIST.add(GEOM_FIELD_HANDLER);
		NATIVE_FIELD_HANDLER_LIST.add(ID_FIELD_HANDLER);
		NATIVE_FIELD_HANDLER_LIST.add(GROUP_ID_FIELD_HANDLER);
	}

	public TestObjectDataAdapter() {
		super(
				COMMON_FIELD_HANDLER_LIST,
				NATIVE_FIELD_HANDLER_LIST);
	}

	@Override
	public ByteArrayId getAdapterId() {
		return new ByteArrayId(
				"test");
	}

	@Override
	public boolean isSupported(
			final TestObject entry ) {
		return true;
	}

	@Override
	public ByteArrayId getDataId(
			final TestObject entry ) {
		return new ByteArrayId(
				entry.id);
	}

	@Override
	public FieldReader getReader(
			final ByteArrayId fieldId ) {
		if (fieldId.equals(GEOM)) {
			return FieldUtils.getDefaultReaderForClass(Geometry.class);
		}
		else if (fieldId.equals(ID)) {
			return FieldUtils.getDefaultReaderForClass(String.class);
		}
		else if (fieldId.equals(GROUP_ID)) {
			return FieldUtils.getDefaultReaderForClass(String.class);
		}
		return null;
	}

	@Override
	public FieldWriter getWriter(
			final ByteArrayId fieldId ) {
		if (fieldId.equals(GEOM)) {
			return FieldUtils.getDefaultWriterForClass(Geometry.class);
		}
		else if (fieldId.equals(ID)) {
			return FieldUtils.getDefaultWriterForClass(String.class);
		}
		else if (fieldId.equals(GROUP_ID)) {
			return FieldUtils.getDefaultWriterForClass(String.class);
		}
		return null;
	}

	@Override
	protected RowBuilder newBuilder() {
		return new RowBuilder<TestObject, Object>() {
			private String id;
			private String groupID;
			private Geometry geom;

			@Override
			public void setField(
					final PersistentValue<Object> fieldValue ) {
				if (fieldValue.getId().equals(
						GEOM)) {
					geom = (Geometry) fieldValue.getValue();
				}
				else if (fieldValue.getId().equals(
						ID)) {
					id = (String) fieldValue.getValue();
				}
				else if (fieldValue.getId().equals(
						GROUP_ID)) {
					groupID = (String) fieldValue.getValue();
				}
			}

			@Override
			public TestObject buildRow(
					final ByteArrayId dataId ) {
				return new TestObject(
						geom,
						id,
						groupID);
			}
		};
	}

	@Override
	public HadoopWritableSerializer<TestObject, TestObjectWritable> createWritableSerializer() {
		return new TestObjectHadoopSerializer();
	}

	private class TestObjectHadoopSerializer implements
			HadoopWritableSerializer<TestObject, TestObjectWritable>
	{

		@Override
		public TestObjectWritable toWritable(
				final TestObject entry ) {
			return new TestObjectWritable(
					entry);
		}

		@Override
		public TestObject fromWritable(
				final TestObjectWritable writable ) {
			return writable.getObj();
		}

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
		if (fieldId.equals(GEOM)) {
			return i;
		}
		else if (fieldId.equals(ID)) {
			return i + 1;
		}
		else if (fieldId.equals(GROUP_ID)) {
			return i + 2;
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
			int numDimensions = model.getDimensions().length;
			if (position == numDimensions) {
				return GEOM;
			}
			else if (position == (numDimensions + 1)) {
				return ID;
			}
			else if (position == (numDimensions + 2)) {
				return GROUP_ID;
			}
		}
		return null;
	}

	@Override
	public void init(
			PrimaryIndex... indices ) {
		// TODO Auto-generated method stub

	}
}
