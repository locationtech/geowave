/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
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
import org.locationtech.geowave.core.index.StringUtils;
import org.locationtech.geowave.core.index.numeric.MultiDimensionalNumericData;
import org.locationtech.geowave.core.store.CloseableIterator;
import org.locationtech.geowave.core.store.adapter.FieldDescriptor;
import org.locationtech.geowave.core.store.adapter.FieldDescriptorBuilder;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.adapter.PersistentAdapterStore;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.RowBuilder;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.api.QueryBuilder;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.data.IndexedPersistenceEncoding;
import org.locationtech.geowave.core.store.data.field.FieldReader;
import org.locationtech.geowave.core.store.data.field.FieldUtils;
import org.locationtech.geowave.core.store.index.CommonIndexModel;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.NullIndex;
import org.locationtech.geowave.core.store.query.constraints.QueryConstraints;
import org.locationtech.geowave.core.store.query.filter.QueryFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Find the max change in distortion between some k and k-1, picking the value k associated with
 * that change.
 *
 * <p> In a multi-group setting, each group may have a different optimal k. Thus, the optimal batch
 * may be different for each group. Each batch is associated with a different value k.
 *
 * <p> Choose the appropriate batch for each group. Then change the batch identifier for group
 * centroids to a final provided single batch identifier ( parent batch ).
 */
public class DistortionGroupManagement {

  static final Logger LOGGER = LoggerFactory.getLogger(DistortionGroupManagement.class);
  public static final Index DISTORTIONS_INDEX = new NullIndex("DISTORTIONS");
  public static final String[] DISTORTIONS_INDEX_ARRAY = new String[] {DISTORTIONS_INDEX.getName()};

  final DataStore dataStore;
  final IndexStore indexStore;
  final PersistentAdapterStore adapterStore;
  final InternalAdapterStore internalAdapterStore;

  public DistortionGroupManagement(final DataStorePluginOptions dataStoreOptions) {
    dataStore = dataStoreOptions.createDataStore();
    indexStore = dataStoreOptions.createIndexStore();
    adapterStore = dataStoreOptions.createAdapterStore();
    internalAdapterStore = dataStoreOptions.createInternalAdapterStore();

    final DistortionDataAdapter adapter = new DistortionDataAdapter();
    dataStore.addType(adapter, DISTORTIONS_INDEX);
  }

  public static class BatchIdFilter implements QueryFilter {
    String batchId;

    public BatchIdFilter() {}

    public BatchIdFilter(final String batchId) {
      super();
      this.batchId = batchId;
    }

    @Override
    public boolean accept(
        final CommonIndexModel indexModel,
        final IndexedPersistenceEncoding<?> persistenceEncoding) {
      return new DistortionEntry(persistenceEncoding.getDataId(), 0.0).batchId.equals(batchId);
    }

    @Override
    public byte[] toBinary() {
      return StringUtils.stringToBinary(batchId);
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      batchId = StringUtils.stringFromBinary(bytes);
    }
  }

  public static class BatchIdQuery implements QueryConstraints {
    String batchId;

    public BatchIdQuery() {}

    public BatchIdQuery(final String batchId) {
      super();
      this.batchId = batchId;
    }

    @Override
    public List<QueryFilter> createFilters(final Index index) {
      return Collections.<QueryFilter>singletonList(new BatchIdFilter(batchId));
    }

    @Override
    public List<MultiDimensionalNumericData> getIndexConstraints(final Index index) {
      return Collections.emptyList();
    }

    @Override
    public byte[] toBinary() {
      return StringUtils.stringToBinary(batchId);
    }

    @Override
    public void fromBinary(final byte[] bytes) {
      batchId = StringUtils.stringFromBinary(bytes);
    }
  }

  public <T> int retainBestGroups(
      final AnalyticItemWrapperFactory<T> itemWrapperFactory,
      final String dataTypeId,
      final String indexId,
      final String batchId,
      final int level) {

    try {
      final Map<String, DistortionGroup> groupDistortions = new HashMap<>();

      // row id is group id
      // colQual is cluster count
      try (CloseableIterator<DistortionEntry> it =
          (CloseableIterator) dataStore.query(
              QueryBuilder.newBuilder().addTypeName(
                  DistortionDataAdapter.ADAPTER_TYPE_NAME).indexName(
                      DISTORTIONS_INDEX.getName()).constraints(
                          new BatchIdQuery(batchId)).build())) {
        while (it.hasNext()) {
          final DistortionEntry entry = it.next();
          final String groupID = entry.getGroupId();
          final Integer clusterCount = entry.getClusterCount();
          final Double distortion = entry.getDistortionValue();

          DistortionGroup grp = groupDistortions.get(groupID);
          if (grp == null) {
            grp = new DistortionGroup(groupID);
            groupDistortions.put(groupID, grp);
          }
          grp.addPair(clusterCount, distortion);
        }
      }

      final CentroidManagerGeoWave<T> centroidManager =
          new CentroidManagerGeoWave<>(
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
        centroidManager.transferBatch(kbatchId, grp.getGroupID());
      }
    } catch (final RuntimeException ex) {
      throw ex;
    } catch (final Exception ex) {
      LOGGER.error("Cannot determine groups for batch", ex);
      return 1;
    }
    return 0;
  }

  public static class DistortionEntry implements Writable {
    private String groupId;
    private String batchId;
    private Integer clusterCount;
    private Double distortionValue;

    public DistortionEntry() {}

    public DistortionEntry(
        final String groupId,
        final String batchId,
        final Integer clusterCount,
        final Double distortionValue) {
      this.groupId = groupId;
      this.batchId = batchId;
      this.clusterCount = clusterCount;
      this.distortionValue = distortionValue;
    }

    private DistortionEntry(final byte[] dataId, final Double distortionValue) {
      final String dataIdStr = StringUtils.stringFromBinary(dataId);
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

    private byte[] getDataId() {
      return StringUtils.stringToBinary(batchId + "/" + groupId + "/" + clusterCount);
    }

    @Override
    public void write(final DataOutput out) throws IOException {
      out.writeUTF(groupId);
      out.writeUTF(batchId);
      out.writeInt(clusterCount);
      out.writeDouble(distortionValue);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
      groupId = in.readUTF();
      batchId = in.readUTF();
      clusterCount = in.readInt();
      distortionValue = in.readDouble();
    }
  }

  private static class DistortionGroup {
    final String groupID;
    final List<Pair<Integer, Double>> clusterCountToDistortion = new ArrayList<>();

    public DistortionGroup(final String groupID) {
      this.groupID = groupID;
    }

    public void addPair(final Integer count, final Double distortion) {
      clusterCountToDistortion.add(Pair.of(count, distortion));
    }

    public String getGroupID() {
      return groupID;
    }

    public int bestCount() {
      Collections.sort(clusterCountToDistortion, new Comparator<Pair<Integer, Double>>() {

        @Override
        public int compare(final Pair<Integer, Double> arg0, final Pair<Integer, Double> arg1) {
          return arg0.getKey().compareTo(arg1.getKey());
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

  public static class DistortionDataAdapter implements DataTypeAdapter<DistortionEntry> {
    public static final String ADAPTER_TYPE_NAME = "distortion";
    private static final String DISTORTION_FIELD_NAME = "distortion";
    private static final FieldDescriptor<Double> DESC =
        new FieldDescriptorBuilder<>(Double.class).fieldName(DISTORTION_FIELD_NAME).build();
    private static final FieldDescriptor<?>[] DESC_ARRAY = new FieldDescriptor[] {DESC};

    public DistortionDataAdapter() {
      super();
    }

    @Override
    public String getTypeName() {
      return ADAPTER_TYPE_NAME;
    }

    @Override
    public byte[] getDataId(final DistortionEntry entry) {
      return entry.getDataId();
    }

    @Override
    public FieldReader<Object> getReader(final String fieldId) {
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
    public void fromBinary(final byte[] bytes) {}

    @Override
    public Object getFieldValue(final DistortionEntry entry, final String fieldName) {
      return entry.getDistortionValue();
    }

    @Override
    public Class<DistortionEntry> getDataClass() {
      return DistortionEntry.class;
    }

    @Override
    public RowBuilder<DistortionEntry> newRowBuilder(
        final FieldDescriptor<?>[] outputFieldDescriptors) {
      return new RowBuilder<DistortionEntry>() {
        Double fieldValue;

        @Override
        public void setField(final String fieldName, final Object fieldValue) {
          if (DISTORTION_FIELD_NAME.equals(fieldName) && (fieldValue instanceof Double)) {
            this.fieldValue = (Double) fieldValue;
          }
        }

        @Override
        public void setFields(final Map<String, Object> values) {
          values.entrySet().forEach((e) -> setField(e.getKey(), e.getValue()));
        }

        @Override
        public DistortionEntry buildRow(final byte[] dataId) {
          return new DistortionEntry(dataId, fieldValue);
        }

      };
    }

    @Override
    public FieldDescriptor<?>[] getFieldDescriptors() {
      return DESC_ARRAY;
    }

    @Override
    public FieldDescriptor<?> getFieldDescriptor(final String fieldName) {
      return DESC;
    }
  }
}
