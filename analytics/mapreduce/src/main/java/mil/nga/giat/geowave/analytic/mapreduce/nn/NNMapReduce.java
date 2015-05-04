package mil.nga.giat.geowave.analytic.mapreduce.nn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;

import mil.nga.giat.geowave.analytic.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidOrthodromicDistanceFn;
import mil.nga.giat.geowave.analytic.mapreduce.JobContextConfigurationWrapper;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.HadoopWritableSerializationTool;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.opengis.feature.simple.SimpleFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.SignedBytes;

/**
 * Find the nearest neighbors to a each item.
 * 
 * The solution represented here partitions the data using a partitioner. The
 * nearest neighbors are inspected within those partitions. Each partition is
 * processed in memory. If the partitioner is agnostic to density, then the
 * number of nearest neighbors inspected in a partition may exceed memory.
 * Selecting the appropriate partitioning is critical. It may be best to work
 * bottom up, partitioning at a finer grain and iterating through larger
 * partitions.
 * 
 * The reducer has four extension points:
 * 
 * @Formatter:off
 * 
 *                (1) createSetForNeighbors() create a set for primary and
 *                secondary neighbor lists. The set implementation can control
 *                the amount of memory used. The algorithm loads the primary and
 *                secondary sets before performing the neighbor analysis. An
 *                implementer can constrain the set size, removing items not
 *                considered relevant.
 * 
 *                (2) createSummary() permits extensions to create an summary
 *                object for the entire partition
 * 
 *                (3) processNeighbors() permits extensions to process the
 *                neighbor list for each primary item and update the summary
 *                object
 * 
 *                (4) processSummary() permits the reducer to produce an output
 *                from the summary object
 * 
 * @Formatter:on
 * 
 * * Properties:
 * 
 * @formatter:off "NNMapReduce.Partition.PartitionerClass" -> {@link mil.nga.giat.geowave.analytic.partitioner.Partitioner}
 *                <p/>
 *               "NNMapReduce.Common.DistanceFunctionClass" -> Used to
 *                determine distance to between simple features {@link mil.nga.giat.geowave.analytic.distance.DistanceFn}
 *                <p/>
 *                "NNMapReduce.Partition.PartitionerClass" -> {@link mil.nga.giat.geowave.analytic.partitioner.Partitioner}
 *                <p/>
 *                "NNMapReduce.Partition.MaxMemberSelection" -> Maximum number of neighbors (pick the top K closest, where this variable is K) (integer)
 *                <p/>
 *                "NNMapReduce.Partition.PartitionDistance" -> Maximum distance between item and its neighbors. (double)
 *                
 *                
 * @Formatter:on
 */
public class NNMapReduce
{
	protected static final Logger LOGGER = LoggerFactory.getLogger(NNMapReduce.class);

	/**
	 * Nearest neighbors...take one
	 * 
	 */
	public static class NNMapper<T> extends
			Mapper<GeoWaveInputKey, T, PartitionDataWritable, AdapterWithObjectWritable>
	{
		protected Partitioner<T> partitioner;
		protected HadoopWritableSerializationTool serializationTool;

		final protected AdapterWithObjectWritable outputValue = new AdapterWithObjectWritable();
		final protected PartitionDataWritable partitionDataWritable = new PartitionDataWritable();

		@Override
		protected void map(
				final GeoWaveInputKey key,
				final T value,
				final Mapper<GeoWaveInputKey, T, PartitionDataWritable, AdapterWithObjectWritable>.Context context )
				throws IOException,
				InterruptedException {

			for (final PartitionData partitionData : partitioner.getCubeIdentifiers(value)) {
				outputValue.setAdapterId(key.getAdapterId());
				AdapterWithObjectWritable.fillWritableWithAdapter(
						serializationTool,
						outputValue,
						key.getAdapterId(),
						partitionData.isPrimary(),
						value);
				partitionDataWritable.setPartitionData(partitionData);
				context.write(
						partitionDataWritable,
						outputValue);
			}
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, T, PartitionDataWritable, AdapterWithObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			final ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context,
					LOGGER);
			try {
				serializationTool = new HadoopWritableSerializationTool(
						new JobContextAdapterStore(
								context,
								GeoWaveInputFormat.getAccumuloOperations(context)));
			}
			catch (AccumuloException | AccumuloSecurityException e) {
				LOGGER.warn(
						"Unable to get GeoWave adapter store from job context",
						e);
			}
			try {
				partitioner = config.getInstance(
						PartitionParameters.Partition.PARTITIONER_CLASS,
						NNMapReduce.class,
						Partitioner.class,
						OrthodromicDistancePartitioner.class);

				partitioner.initialize(config);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}
		}
	}

	public abstract static class NNReducer<VALUEIN, KEYOUT, VALUEOUT, PARTITION_SUMMARY> extends
			Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>
	{
		protected HadoopWritableSerializationTool serializationTool;
		protected DistanceFn<VALUEIN> distanceFn;
		protected double maxDistance = 1.0;
		protected int maxNeighbors = Integer.MAX_VALUE;

		@Override
		protected void reduce(
				final PartitionDataWritable key,
				final Iterable<AdapterWithObjectWritable> values,
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context )
				throws IOException,
				InterruptedException {
			final Set<VALUEIN> primaries = createSetForNeighbors(true);
			final Set<VALUEIN> others = createSetForNeighbors(false);
			final PARTITION_SUMMARY summary = createSummary();

			for (final AdapterWithObjectWritable inputValue : values) {
				@SuppressWarnings("unchecked")
				final VALUEIN unwrappedValue = (VALUEIN) AdapterWithObjectWritable.fromWritableWithAdapter(
						serializationTool,
						inputValue);
				if (inputValue.isPrimary()) {
					primaries.add(unwrappedValue);
				}
				else {
					others.add(unwrappedValue);
				}
			}

			final TreeSet<NNData<VALUEIN>> neighbors = new TreeSet<NNData<VALUEIN>>();
			for (final VALUEIN primary : primaries) {
				for (final VALUEIN anotherPrimary : primaries) {
					if (anotherPrimary.equals(primary)) {
						continue;
					}
					final double distance = distanceFn.measure(
							primary,
							anotherPrimary);
					if (distance <= maxDistance) {
						neighbors.add(new NNData<VALUEIN>(
								anotherPrimary,
								distance));
						if (neighbors.size() > maxNeighbors) {
							neighbors.pollLast();
						}
					}
				}
				for (final VALUEIN anOther : others) {
					if (anOther.equals(primary)) {
						continue;
					}
					final double distance = distanceFn.measure(
							primary,
							anOther);
					if (distance <= maxDistance) {
						neighbors.add(new NNData<VALUEIN>(
								anOther,
								distance));
						if (neighbors.size() > maxNeighbors) {
							neighbors.pollLast();
						}
					}
				}
				if (neighbors.size() > 0) {
					processNeighbors(
							primary,
							neighbors,
							context,
							summary);
				}
				neighbors.clear();
			}

			processSummary(
					summary,
					context);
		}

		/**
		 * 
		 * @Return an object that represents a summary of the neighbors
		 *         processed
		 */
		protected abstract PARTITION_SUMMARY createSummary();

		/**
		 * Allow extended classes to do some final processing for the partition.
		 * 
		 * @param summary
		 * @param context
		 */
		protected abstract void processSummary(
				PARTITION_SUMMARY summary,
				Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context );

		/**
		 * 
		 * allow the extending classes to return sets with constraints and
		 * management algorithms
		 */
		protected Set<VALUEIN> createSetForNeighbors(
				final boolean isSetForPrimary ) {
			return new HashSet<VALUEIN>();
		}

		protected abstract void processNeighbors(
				VALUEIN primary,
				Set<NNData<VALUEIN>> neighbors,
				Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context,
				PARTITION_SUMMARY summary )
				throws IOException,
				InterruptedException;

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context )
				throws IOException,
				InterruptedException {

			final ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context,
					NNMapReduce.LOGGER);

			try {
				serializationTool = new HadoopWritableSerializationTool(
						new JobContextAdapterStore(
								context,
								GeoWaveInputFormat.getAccumuloOperations(context)));
			}
			catch (AccumuloException | AccumuloSecurityException e) {
				LOGGER.warn(
						"Unable to get GeoWave adapter store from job context",
						e);
			}

			try {
				distanceFn = config.getInstance(
						CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
						NNMapReduce.class,
						DistanceFn.class,
						FeatureCentroidOrthodromicDistanceFn.class);
			}
			catch (InstantiationException | IllegalAccessException e) {
				throw new IOException(
						e);
			}

			maxDistance = config.getDouble(
					PartitionParameters.Partition.PARTITION_DISTANCE,
					NNMapReduce.class,
					1.0);

			maxNeighbors = config.getInt(
					PartitionParameters.Partition.MAX_MEMBER_SELECTION,
					NNMapReduce.class,
					Integer.MAX_VALUE);
		}
	}

	public static class NNSimpleFeatureIDOutputReducer extends
			NNReducer<SimpleFeature, Text, Text, Boolean>
	{

		final Text primaryText = new Text();
		final Text neighborsText = new Text();
		final byte[] sepBytes = new byte[] {
			0x2c
		};

		@Override
		protected void processNeighbors(
				final SimpleFeature primary,
				final Set<NNData<SimpleFeature>> neighbors,
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, Text, Text>.Context context,
				final Boolean summary )
				throws IOException,
				InterruptedException {
			primaryText.clear();
			neighborsText.clear();
			byte[] utfBytes;
			try {

				utfBytes = primary.getID().getBytes(
						"UTF-8");
				primaryText.append(
						utfBytes,
						0,
						utfBytes.length);
				for (final NNData<SimpleFeature> neighbor : neighbors) {
					if (neighborsText.getLength() > 0) {
						neighborsText.append(
								sepBytes,
								0,
								sepBytes.length);
					}
					utfBytes = neighbor.getNeighbor().getID().getBytes(
							"UTF-8");
					neighborsText.append(
							utfBytes,
							0,
							utfBytes.length);
				}

				context.write(
						primaryText,
						neighborsText);
			}
			catch (final UnsupportedEncodingException e) {
				throw new RuntimeException(
						"UTF-8 Encoding invalid for Simople feature ID",
						e);
			}

		}

		@Override
		protected Boolean createSummary() {
			return Boolean.TRUE;
		}

		@Override
		protected void processSummary(
				final Boolean summary,
				final org.apache.hadoop.mapreduce.Reducer.Context context ) {
			// do nothing
		}
	}

	public static class PartitionDataWritable implements
			Writable,
			WritableComparable<PartitionDataWritable>
	{

		protected PartitionData partitionData;

		public PartitionDataWritable() {

		}

		protected void setPartitionData(
				final PartitionData partitionData ) {
			this.partitionData = partitionData;
		}

		public PartitionDataWritable(
				final PartitionData partitionData ) {
			this.partitionData = partitionData;
		}

		@Override
		public void readFields(
				final DataInput input )
				throws IOException {
			partitionData = new PartitionData();
			partitionData.readFields(input);

		}

		@Override
		public void write(
				final DataOutput output )
				throws IOException {
			partitionData.write(output);
		}

		@Override
		public int compareTo(
				final PartitionDataWritable o ) {
			return SignedBytes.lexicographicalComparator().compare(
					partitionData.getId().getBytes(),
					o.partitionData.getId().getBytes());
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = (prime * result) + ((partitionData == null) ? 0 : partitionData.hashCode());
			return result;
		}

		@Override
		public boolean equals(
				final Object obj ) {
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
			}
			else if (!partitionData.equals(other.partitionData)) {
				return false;
			}
			return true;
		}
	}
}
