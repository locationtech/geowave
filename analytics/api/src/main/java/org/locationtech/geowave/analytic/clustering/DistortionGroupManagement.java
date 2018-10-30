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
package org.locationtech.geowave.analytic.clustering;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Writable;
import org.locationtech.geowave.analytic.AnalyticItemWrapperFactory;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.sfc.data.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.AdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.IndexedAdapterPersistenceEncoding;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.PersistentDataset;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.data.field.FieldVisibilityHandler;
import org.locationtech.geowave.core.store.data.field.FieldWriter;
import org.locationtech.geowave.core.store.dimension.NumericDimensionField;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.CommonIndexValue;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Find the max change in distortion between some k and k-1, picking the value k
 * associated with that change.
 *
 * In a multi-group setting, each group may have a different optimal k. Thus,
 * the optimal batch may be different for each group. Each batch is associated
 * with a different value k.
 *
 * Choose the appropriate batch for each group. Then change the batch identifier
 * for group centroids to a final provided single batch identifier ( parent
 * batch ).
 *
 */
public class DistortionGroupManagement
{

	final static Logger LOGGER = LoggerFactory.getLogger(DistortionGroupManagement.class);
	public final static Index DISTORTIONS_INDEX = new NullIndex(
			"DISTORTIONS");
	public final static String[] DISTORTIONS_INDEX_ARRAY = new String[] {
		DISTORTIONS_INDEX.getName()
	};

	final DataStore dataStore;
	final IndexStore indexStore;
	final PersistentAdapterStore adapterStore;
	final InternalAdapterStore internalAdapterStore;

	public DistortionGroupManagement(
			final DataStorePluginOptions dataStoreOptions ) {
		dataStore = dataStoreOptions.createDataStore();
		indexStore = dataStoreOptions.createIndexStore();
		adapterStore = dataStoreOptions.createAdapterStore();
		internalAdapterStore = dataStoreOptions.createInternalAdapterStore();

		final DistortionDataAdapter adapter = new DistortionDataAdapter();
		dataStore.addType(
				adapter,
				DISTORTIONS_INDEX);
	}

	public static class BatchIdFilter implements
			QueryFilter
	{
		String batchId;

		public BatchIdFilter() {

		}

		public BatchIdFilter(
				final String batchId ) {
			super();
			this.batchId = batchId;
		}

		@Override
		public boolean accept(
				final CommonIndexModel indexModel,
				final IndexedPersistenceEncoding<?> persistenceEncoding ) {
			return new DistortionEntry(
					persistenceEncoding.getDataId(),
					0.0).batchId.equals(batchId);
		}

		@Override
		public byte[] toBinary() {
			return StringUtils.stringToBinary(batchId);
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			batchId = StringUtils.stringFromBinary(bytes);
		}
	}

	public static class BatchIdQuery implements
			QueryConstraints
	{
		String batchId;

		public BatchIdQuery() {}

		public BatchIdQuery(
				final String batchId ) {
			super();
			this.batchId = batchId;
		}

		@Override
		public List<QueryFilter> createFilters(
				final Index index ) {
			return Collections.<QueryFilter> singletonList(new BatchIdFilter(
					batchId));
		}

		@Override
		public List<MultiDimensionalNumericData> getIndexConstraints(
				final Index index ) {
			return Collections.emptyList();
		}

		@Override
		public byte[] toBinary() {
			return StringUtils.stringToBinary(batchId);
		}

		@Override
		public void fromBinary(
				final byte[] bytes ) {
			batchId = StringUtils.stringFromBinary(bytes);
		}

	}

	/**
	 *
	 * @param ops
	 * @param distortationTableName
	 *            the name of the table holding the distortions
	 * @param parentBatchId
	 *            the batch id to associate with the centroids for each group
	 * @return
	 */
	public <T> int retainBestGroups(
			final AnalyticItemWrapperFactory<T> itemWrapperFactory,
			final String dataTypeId,
			final String indexId,
			final String batchId,
			final int level ) {

		try {
			final Map<String, DistortionGroup> groupDistortions = new HashMap<>();

			// row id is group id
			// colQual is cluster count
			try (CloseableIterator<DistortionEntry> it = (CloseableIterator) dataStore.query(QueryBuilder
					.newBuilder()
					.addTypeName(
							DistortionDataAdapter.ADAPTER_TYPE_NAME)
					.indexName(
							DISTORTIONS_INDEX.getName())
					.constraints(
							new BatchIdQuery(
									batchId))
					.build())) {
				while (it.hasNext()) {
					final DistortionEntry entry = it.next();
					final String groupID = entry.getGroupId();
					final Integer clusterCount = entry.getClusterCount();
					final Double distortion = entry.getDistortionValue();

					DistortionGroup grp = groupDistortions.get(groupID);
					if (grp == null) {
						grp = new DistortionGroup(
								groupID);
						groupDistortions.put(
								groupID,
								grp);
					}
					grp.addPair(
							clusterCount,
							distortion);
				}
			}

			final CentroidManagerGeoWave<T> centroidManager = new CentroidManagerGeoWave<>(
					dataStore,
					indexStore,
					adapterStore,
					itemWrapperFactory,
					dataTypeId,
					internalAdapterStore.getAdapterId(dataTypeId),
					indexId,
					batchId,
					level);

			for (final DistortionGroup grp : groupDistortions.values()) {
				final int optimalK = grp.bestCount();
				final String kbatchId = batchId + "_" + optimalK;
				centroidManager.transferBatch(
						kbatchId,
						grp.getGroupID());
			}
		}
		catch (final RuntimeException ex) {
			throw ex;
		}
		catch (final Exception ex) {
			LOGGER.error(
					"Cannot determine groups for batch",
					ex);
			return 1;
		}
		return 0;
	}

	public static class DistortionEntry implements
			Writable
	{
		private String groupId;
		private String batchId;
		private Integer clusterCount;
		private Double distortionValue;

		public DistortionEntry() {}

		public DistortionEntry(
				final String groupId,
				final String batchId,
				final Integer clusterCount,
				final Double distortionValue ) {
			this.groupId = groupId;
			this.batchId = batchId;
			this.clusterCount = clusterCount;
			this.distortionValue = distortionValue;
		}

		private DistortionEntry(
				final ByteArray dataId,
				final Double distortionValue ) {
			final String dataIdStr = StringUtils.stringFromBinary(dataId.getBytes());
			final String[] split = dataIdStr.split("/");
			batchId = split[0];
			groupId = split[1];
			clusterCount = Integer.parseInt(split[2]);
			this.distortionValue = distortionValue;
		}

		public String getGroupId() {
			return groupId;
		}

		public Integer getClusterCount() {
			return clusterCount;
		}

		public Double getDistortionValue() {
			return distortionValue;
		}

		private ByteArray getDataId() {
			return new ByteArray(
					batchId + "/" + groupId + "/" + clusterCount);
		}

		@Override
		public void write(
				final DataOutput out )
				throws IOException {
			out.writeUTF(groupId);
			out.writeUTF(batchId);
			out.writeInt(clusterCount);
			out.writeDouble(distortionValue);
		}

		@Override
		public void readFields(
				final DataInput in )
				throws IOException {
			groupId = in.readUTF();
			batchId = in.readUTF();
			clusterCount = in.readInt();
			distortionValue = in.readDouble();
		}
	}

	private static class DistortionGroup
	{
		final String groupID;
		final List<Pair<Integer, Double>> clusterCountToDistortion = new ArrayList<>();

		public DistortionGroup(
				final String groupID ) {
			this.groupID = groupID;
		}

		public void addPair(
				final Integer count,
				final Double distortion ) {
			clusterCountToDistortion.add(Pair.of(
					count,
					distortion));
		}

		public String getGroupID() {
			return groupID;
		}

		public int bestCount() {
			Collections.sort(
					clusterCountToDistortion,
					new Comparator<Pair<Integer, Double>>() {

						@Override
						public int compare(
								final Pair<Integer, Double> arg0,
								final Pair<Integer, Double> arg1 ) {
							return arg0.getKey().compareTo(
									arg1.getKey());
						}
					});
			double maxJump = -1.0;
			Integer jumpIdx = -1;
			Double oldD = 0.0; // base case !?
			for (final Pair<Integer, Double> pair : clusterCountToDistortion) {
				final Double jump = pair.getValue() - oldD;
				if (jump > maxJump) {
					maxJump = jump;
					jumpIdx = pair.getKey();
				}
				oldD = pair.getValue();
			}
			return jumpIdx;
		}
	}

	public static class DistortionDataAdapter implements
			DataTypeAdapter<DistortionEntry>
	{
		public final static String ADAPTER_TYPE_NAME = "distortion";
		private final static String DISTORTION_FIELD_NAME = "distortion";
		private final FieldVisibilityHandler<DistortionEntry, Object> distortionVisibilityHandler;

		public DistortionDataAdapter() {
			this(
					null);
		}

		public DistortionDataAdapter(
				final FieldVisibilityHandler<DistortionEntry, Object> distortionVisibilityHandler ) {
			this.distortionVisibilityHandler = distortionVisibilityHandler;
		}

		@Override
		public String getTypeName() {
			return ADAPTER_TYPE_NAME;
		}

		@Override
		public ByteArray getDataId(
				final DistortionEntry entry ) {
			return entry.getDataId();
		}

		@Override
		public DistortionEntry decode(
				final IndexedAdapterPersistenceEncoding data,
				final Index index ) {
			return new DistortionEntry(
					data.getDataId(),
					(Double) data.getAdapterExtendedData().getValue(
							DISTORTION_FIELD_NAME));
		}

		@Override
		public AdapterPersistenceEncoding encode(
				final DistortionEntry entry,
				final CommonIndexModel indexModel ) {
			final Map<String, Object> fieldNameToValueMap = new HashMap<>();
			fieldNameToValueMap.put(
					DISTORTION_FIELD_NAME,
					entry.getDistortionValue());
			return new AdapterPersistenceEncoding(
					entry.getDataId(),
					new PersistentDataset<CommonIndexValue>(),
					new PersistentDataset<>(
							fieldNameToValueMap));
		}

		@Override
		public FieldReader<Object> getReader(
				final String fieldId ) {
			if (DISTORTION_FIELD_NAME.equals(fieldId)) {
				return (FieldReader) FieldUtils.getDefaultReaderForClass(Double.class);
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
		public FieldWriter<DistortionEntry, Object> getWriter(
				final String fieldId ) {
			if (DISTORTION_FIELD_NAME.equals(fieldId)) {
				if (distortionVisibilityHandler != null) {
					return (FieldWriter) FieldUtils.getDefaultWriterForClass(
							Double.class,
							distortionVisibilityHandler);
				}
				else {
					return (FieldWriter) FieldUtils.getDefaultWriterForClass(Double.class);
				}
			}
			return null;
		}

		@Override
		public int getPositionOfOrderedField(
				final CommonIndexModel model,
				final String fieldName ) {
			int i = 0;
			for (final NumericDimensionField<? extends CommonIndexValue> dimensionField : model.getDimensions()) {
				if (fieldName.equals(dimensionField.getFieldName())) {
					return i;
				}
				i++;
			}
			if (fieldName.equals(DISTORTION_FIELD_NAME)) {
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
					return DISTORTION_FIELD_NAME;
				}
			}
			return null;
		}
	}
}
