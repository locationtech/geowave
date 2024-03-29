/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
package org.locationtech.geowave.analytic.mapreduce.nn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.locationtech.geowave.analytic.AdapterWithObjectWritable;
import org.locationtech.geowave.analytic.PropertyManagement;
import org.locationtech.geowave.analytic.ScopedJobConfiguration;
import org.locationtech.geowave.analytic.distance.DistanceFn;
import org.locationtech.geowave.analytic.distance.FeatureGeometryDistanceFn;
import org.locationtech.geowave.analytic.nn.DefaultNeighborList;
import org.locationtech.geowave.analytic.nn.DistanceProfile;
import org.locationtech.geowave.analytic.nn.DistanceProfileGenerateFn;
import org.locationtech.geowave.analytic.nn.NNProcessor;
import org.locationtech.geowave.analytic.nn.NNProcessor.CompleteNotifier;
import org.locationtech.geowave.analytic.nn.NeighborList;
import org.locationtech.geowave.analytic.nn.NeighborListFactory;
import org.locationtech.geowave.analytic.nn.TypeConverter;
import org.locationtech.geowave.analytic.param.CommonParameters;
import org.locationtech.geowave.analytic.param.ParameterEnum;
import org.locationtech.geowave.analytic.param.ParameterHelper;
import org.locationtech.geowave.analytic.param.PartitionParameters;
import org.locationtech.geowave.analytic.param.PartitionParameters.Partition;
import org.locationtech.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import org.locationtech.geowave.analytic.partitioner.Partitioner;
import org.locationtech.geowave.analytic.partitioner.Partitioner.PartitionData;
import org.locationtech.geowave.analytic.partitioner.Partitioner.PartitionDataCallback;
import org.locationtech.geowave.core.index.ByteArray;
import org.locationtech.geowave.mapreduce.HadoopWritableSerializationTool;
import org.locationtech.geowave.mapreduce.input.GeoWaveInputKey;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.primitives.UnsignedBytes;

/**
 * Find the nearest neighbors to a each item.
 *
 * <p>The solution represented here partitions the data using a partitioner. The nearest neighbors
 * are inspected within those partitions. Each partition is processed in memory. If the partitioner
 * is agnostic to density, then the number of nearest neighbors inspected in a partition may exceed
 * memory. Selecting the appropriate partitioning is critical. It may be best to work bottom up,
 * partitioning at a finer grain and iterating through larger partitions.
 *
 * <p>The reducer has four extension points:
 *
 * <!-- @formatter:off -->
 *     <p>(1) createSetForNeighbors() create a set for primary and secondary neighbor lists. The set
 *     implementation can control the amount of memory used. The algorithm loads the primary and
 *     secondary sets before performing the neighbor analysis. An implementer can constrain the set
 *     size, removing items not considered relevant.
 *     <p>(2) createSummary() permits extensions to create an summary object for the entire
 *     partition
 *     <p>(3) processNeighbors() permits extensions to process the neighbor list for each primary
 *     item and update the summary object
 *     <p>(4) processSummary() permits the reducer to produce an output from the summary object
 * <!-- @formatter:on -->
 *     <p>* Properties:
 * <!-- @formatter:off -->"NNMapReduce.Partition.PartitionerClass" -> {@link
 *     org.locationtech.geowave.analytic.partitioner.Partitioner}
 *     <p>"NNMapReduce.Common.DistanceFunctionClass" -> Used to determine distance to between simple
 *     features {@link org.locationtech.geowave.analytic.distance.DistanceFn}
 *     <p>"NNMapReduce.Partition.PartitionerClass" -> {@link
 *     org.locationtech.geowave.analytic.partitioner.Partitioner}
 *     <p>"NNMapReduce.Partition.MaxMemberSelection" -> Maximum number of neighbors (pick the top K
 *     closest, where this variable is K) (integer)
 *     <p>"NNMapReduce.Partition.PartitionDistance" -> Maximum distance between item and its
 *     neighbors. (double)
 * <!-- @formatter:on -->
 */
public class NNMapReduce {
  protected static final Logger LOGGER = LoggerFactory.getLogger(NNMapReduce.class);

  /** Nearest neighbors...take one */
  public static class NNMapper<T> extends
      Mapper<GeoWaveInputKey, Object, PartitionDataWritable, AdapterWithObjectWritable> {
    protected Partitioner<T> partitioner;
    protected HadoopWritableSerializationTool serializationTool;

    protected final AdapterWithObjectWritable outputValue = new AdapterWithObjectWritable();
    protected final PartitionDataWritable partitionDataWritable = new PartitionDataWritable();

    @Override
    protected void map(
        final GeoWaveInputKey key,
        final Object value,
        final Mapper<GeoWaveInputKey, Object, PartitionDataWritable, AdapterWithObjectWritable>.Context context)
        throws IOException, InterruptedException {

      @SuppressWarnings("unchecked")
      final T unwrappedValue =
          (T) ((value instanceof ObjectWritable)
              ? serializationTool.fromWritable(key.getInternalAdapterId(), (ObjectWritable) value)
              : value);
      try {
        partitioner.partition(unwrappedValue, new PartitionDataCallback() {

          @Override
          public void partitionWith(final PartitionData partitionData) throws Exception {
            outputValue.setInternalAdapterId(key.getInternalAdapterId());
            AdapterWithObjectWritable.fillWritableWithAdapter(
                serializationTool,
                outputValue,
                key.getInternalAdapterId(),
                key.getDataId(),
                unwrappedValue);
            partitionDataWritable.setPartitionData(partitionData);
            context.write(partitionDataWritable, outputValue);
          }
        });
      } catch (final IOException e) {
        throw e;
      } catch (final Exception e) {
        throw new IOException(e);
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    protected void setup(
        final Mapper<GeoWaveInputKey, Object, PartitionDataWritable, AdapterWithObjectWritable>.Context context)
        throws IOException, InterruptedException {
      super.setup(context);
      final ScopedJobConfiguration config =
          new ScopedJobConfiguration(context.getConfiguration(), NNMapReduce.class, LOGGER);
      serializationTool = new HadoopWritableSerializationTool(context);
      try {
        partitioner =
            config.getInstance(
                PartitionParameters.Partition.PARTITIONER_CLASS,
                Partitioner.class,
                OrthodromicDistancePartitioner.class);

        partitioner.initialize(context, NNMapReduce.class);
      } catch (final Exception e1) {
        throw new IOException(e1);
      }
    }
  }

  public abstract static class NNReducer<VALUEIN, KEYOUT, VALUEOUT, PARTITION_SUMMARY> extends
      Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT> {
    protected HadoopWritableSerializationTool serializationTool;
    protected DistanceFn<VALUEIN> distanceFn;
    protected double maxDistance = 1.0;
    protected int maxNeighbors = Integer.MAX_VALUE;
    protected Partitioner<Object> partitioner;

    protected TypeConverter<VALUEIN> typeConverter = new TypeConverter<VALUEIN>() {

      @SuppressWarnings("unchecked")
      @Override
      public VALUEIN convert(final ByteArray id, final Object o) {
        return (VALUEIN) o;
      }
    };

    protected DistanceProfileGenerateFn<?, VALUEIN> distanceProfileFn =
        new LocalDistanceProfileGenerateFn();

    @Override
    protected void reduce(
        final PartitionDataWritable key,
        final Iterable<AdapterWithObjectWritable> values,
        final Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context)
        throws IOException, InterruptedException {

      final NNProcessor<Object, VALUEIN> processor =
          new NNProcessor<>(
              partitioner,
              typeConverter,
              distanceProfileFn,
              maxDistance,
              key.partitionData);

      processor.setUpperBoundPerPartition(maxNeighbors);

      final PARTITION_SUMMARY summary = createSummary();

      for (final AdapterWithObjectWritable inputValue : values) {

        final Object value =
            AdapterWithObjectWritable.fromWritableWithAdapter(serializationTool, inputValue);

        processor.add(inputValue.getDataId(), key.partitionData.isPrimary(), value);
      }

      preprocess(context, processor, summary);

      processor.process(this.createNeighborsListFactory(summary), new CompleteNotifier<VALUEIN>() {
        @Override
        public void complete(
            final ByteArray id,
            final VALUEIN value,
            final NeighborList<VALUEIN> primaryList) throws IOException, InterruptedException {
          context.progress();
          processNeighbors(key.partitionData, id, value, primaryList, context, summary);
          processor.remove(id);
        }
      });

      processSummary(key.partitionData, summary, context);
    }

    public NeighborListFactory<VALUEIN> createNeighborsListFactory(
        final PARTITION_SUMMARY summary) {
      return new DefaultNeighborList.DefaultNeighborListFactory<>();
    }

    protected void preprocess(
        final Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context,
        final NNProcessor<Object, VALUEIN> processor,
        final PARTITION_SUMMARY summary) throws IOException, InterruptedException {}

    /** @return an object that represents a summary of the neighbors processed */
    protected abstract PARTITION_SUMMARY createSummary();

    /**
     * Allow extended classes to do some final processing for the partition.
     */
    protected abstract void processSummary(
        PartitionData partitionData,
        PARTITION_SUMMARY summary,
        Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context)
        throws IOException, InterruptedException;

    /** allow the extending classes to return sets with constraints and management algorithms */
    protected Set<VALUEIN> createSetForNeighbors(final boolean isSetForPrimary) {
      return new HashSet<>();
    }

    protected abstract void processNeighbors(
        PartitionData partitionData,
        ByteArray primaryId,
        VALUEIN primary,
        NeighborList<VALUEIN> neighbors,
        Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context,
        PARTITION_SUMMARY summary) throws IOException, InterruptedException;

    @SuppressWarnings("unchecked")
    @Override
    protected void setup(
        final Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context)
        throws IOException, InterruptedException {

      final ScopedJobConfiguration config =
          new ScopedJobConfiguration(
              context.getConfiguration(),
              NNMapReduce.class,
              NNMapReduce.LOGGER);

      serializationTool = new HadoopWritableSerializationTool(context);

      try {
        distanceFn =
            config.getInstance(
                CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
                DistanceFn.class,
                FeatureGeometryDistanceFn.class);
      } catch (InstantiationException | IllegalAccessException e) {
        throw new IOException(e);
      }

      maxDistance = config.getDouble(PartitionParameters.Partition.MAX_DISTANCE, 1.0);

      try {
        LOGGER.info("Using secondary partitioning");
        partitioner =
            config.getInstance(
                PartitionParameters.Partition.SECONDARY_PARTITIONER_CLASS,
                Partitioner.class,
                PassthruPartitioner.class);
        ((ParameterHelper<Double>) Partition.PARTITION_PRECISION.getHelper()).setValue(
            context.getConfiguration(),
            NNMapReduce.class,
            new Double(1.0));
        partitioner.initialize(context, NNMapReduce.class);
      } catch (final Exception e1) {
        throw new IOException(e1);
      }

      maxNeighbors =
          config.getInt(
              PartitionParameters.Partition.MAX_MEMBER_SELECTION,
              NNProcessor.DEFAULT_UPPER_BOUND_PARTIION_SIZE);

      LOGGER.info("Maximum Neighbors = {}", maxNeighbors);
    }

    protected class LocalDistanceProfileGenerateFn implements
        DistanceProfileGenerateFn<Object, VALUEIN> {

      // for GC concerns in the default NN case
      DistanceProfile<Object> singleNotThreadSafeImage = new DistanceProfile<>();

      @Override
      public DistanceProfile<Object> computeProfile(final VALUEIN item1, final VALUEIN item2) {
        singleNotThreadSafeImage.setDistance(distanceFn.measure(item1, item2));
        return singleNotThreadSafeImage;
      }
    }
  }

  public static class NNSimpleFeatureIDOutputReducer extends
      NNReducer<SimpleFeature, Text, Text, Boolean> {

    final Text primaryText = new Text();
    final Text neighborsText = new Text();
    final byte[] sepBytes = new byte[] {0x2c};

    @Override
    protected void processNeighbors(
        final PartitionData partitionData,
        final ByteArray primaryId,
        final SimpleFeature primary,
        final NeighborList<SimpleFeature> neighbors,
        final Reducer<PartitionDataWritable, AdapterWithObjectWritable, Text, Text>.Context context,
        final Boolean summary) throws IOException, InterruptedException {
      if ((neighbors == null) || (neighbors.size() == 0)) {
        return;
      }
      primaryText.clear();
      neighborsText.clear();
      byte[] utfBytes;
      try {

        utfBytes = primary.getID().getBytes("UTF-8");
        primaryText.append(utfBytes, 0, utfBytes.length);
        for (final Map.Entry<ByteArray, SimpleFeature> neighbor : neighbors) {
          if (neighborsText.getLength() > 0) {
            neighborsText.append(sepBytes, 0, sepBytes.length);
          }
          utfBytes = neighbor.getValue().getID().getBytes("UTF-8");
          neighborsText.append(utfBytes, 0, utfBytes.length);
        }

        context.write(primaryText, neighborsText);
      } catch (final UnsupportedEncodingException e) {
        throw new RuntimeException("UTF-8 Encoding invalid for Simople feature ID", e);
      }
    }

    @Override
    protected Boolean createSummary() {
      return Boolean.TRUE;
    }

    @Override
    protected void processSummary(
        final PartitionData partitionData,
        final Boolean summary,
        final org.apache.hadoop.mapreduce.Reducer.Context context) {
      // do nothing
    }
  }

  public static class PartitionDataWritable implements
      Writable,
      WritableComparable<PartitionDataWritable> {

    protected PartitionData partitionData;

    public PartitionDataWritable() {}

    protected void setPartitionData(final PartitionData partitionData) {
      this.partitionData = partitionData;
    }

    public PartitionData getPartitionData() {
      return partitionData;
    }

    public PartitionDataWritable(final PartitionData partitionData) {
      this.partitionData = partitionData;
    }

    @Override
    public void readFields(final DataInput input) throws IOException {
      partitionData = new PartitionData();
      partitionData.readFields(input);
    }

    @Override
    public void write(final DataOutput output) throws IOException {
      partitionData.write(output);
    }

    @Override
    public int compareTo(final PartitionDataWritable o) {
      final int val =
          UnsignedBytes.lexicographicalComparator().compare(
              partitionData.getCompositeKey().getBytes(),
              o.partitionData.getCompositeKey().getBytes());
      if ((val == 0)
          && (o.partitionData.getGroupId() != null)
          && (partitionData.getGroupId() != null)) {
        return UnsignedBytes.lexicographicalComparator().compare(
            partitionData.getGroupId().getBytes(),
            o.partitionData.getGroupId().getBytes());
      }
      return val;
    }

    @Override
    public String toString() {
      return partitionData.toString();
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = (prime * result) + ((partitionData == null) ? 0 : partitionData.hashCode());
      return result;
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      final PartitionDataWritable other = (PartitionDataWritable) obj;
      if (partitionData == null) {
        if (other.partitionData != null) {
          return false;
        }
      } else if (!partitionData.equals(other.partitionData)) {
        return false;
      }
      return true;
    }
  }

  public static class PassthruPartitioner<T> implements Partitioner<T> {

    /** */
    private static final long serialVersionUID = -1022316020113365561L;

    @Override
    public void initialize(final JobContext context, final Class<?> scope) throws IOException {}

    private static final List<PartitionData> FixedPartition =
        Collections.singletonList(
            new PartitionData(new ByteArray(new byte[] {}), new ByteArray("1"), true));

    @Override
    public List<PartitionData> getCubeIdentifiers(final T entry) {
      return FixedPartition;
    }

    @Override
    public void partition(final T entry, final PartitionDataCallback callback) throws Exception {
      callback.partitionWith(FixedPartition.get(0));
    }

    @Override
    public Collection<ParameterEnum<?>> getParameters() {
      return Collections.emptyList();
    }

    @Override
    public void setup(
        final PropertyManagement runTimeProperties,
        final Class<?> scope,
        final Configuration configuration) {}
  }
}
