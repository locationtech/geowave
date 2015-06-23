package mil.nga.giat.geowave.analytic.mapreduce.nn;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import mil.nga.giat.geowave.analytic.AdapterWithObjectWritable;
import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureGeometryDistanceFn;
import mil.nga.giat.geowave.analytic.log.LoggingConfigurationWrapper;
import mil.nga.giat.geowave.analytic.mapreduce.JobContextConfigurationWrapper;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.PartitionParameters;
import mil.nga.giat.geowave.analytic.partitioner.OrthodromicDistancePartitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionData;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner.PartitionDataCallback;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.HadoopWritableSerializationTool;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.JobContextAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputFormat;
import mil.nga.giat.geowave.datastore.accumulo.mapreduce.input.GeoWaveInputKey;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.ObjectWritable;
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
 *               * Properties:
 * 
 * @formatter:off "NNMapReduce.Partition.PartitionerClass" ->
 *                {@link mil.nga.giat.geowave.analytics.tools.partitioners.Partitioner}
 *                <p/>
 *                "NNMapReduce.Common.DistanceFunctionClass" -> Used to
 *                determine distance to between simple features
 *                {@link mil.nga.giat.geowave.analytics.distance.DistanceFn}
 *                <p/>
 *                "NNMapReduce.Partition.PartitionerClass" ->
 *                {@link mil.nga.giat.geowave.analytics.tools.partitioners.Partitioner}
 *                <p/>
 *                "NNMapReduce.Partition.MaxMemberSelection" -> Maximum number
 *                of neighbors (pick the top K closest, where this variable is
 *                K) (integer)
 *                <p/>
 *                "NNMapReduce.Partition.PartitionDistance" -> Maximum distance
 *                between item and its neighbors. (double)
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
			Mapper<GeoWaveInputKey, Object, PartitionDataWritable, AdapterWithObjectWritable>
	{
		protected Partitioner<T> partitioner;
		protected HadoopWritableSerializationTool serializationTool;

		final protected AdapterWithObjectWritable outputValue = new AdapterWithObjectWritable();
		final protected PartitionDataWritable partitionDataWritable = new PartitionDataWritable();

		@Override
		protected void map(
				final GeoWaveInputKey key,
				final Object value,
				final Mapper<GeoWaveInputKey, Object, PartitionDataWritable, AdapterWithObjectWritable>.Context context )
				throws IOException,
				InterruptedException {

			@SuppressWarnings("unchecked")
			final T unwrappedValue = (T) ((value instanceof ObjectWritable) ? serializationTool.fromWritable(
					key.getAdapterId(),
					(ObjectWritable) value) : value);
			try {
				partitioner.partition(
						unwrappedValue,
						new PartitionDataCallback() {

							@Override
							public void partitionWith(
									final PartitionData partitionData )
									throws Exception {
								outputValue.setAdapterId(key.getAdapterId());
								AdapterWithObjectWritable.fillWritableWithAdapter(
										serializationTool,
										outputValue,
										key.getAdapterId(),
										key.getDataId(),
										partitionData.isPrimary(),
										unwrappedValue);
								partitionDataWritable.setPartitionData(partitionData);
								context.write(
										partitionDataWritable,
										outputValue);

							}
						});
			}
			catch (final IOException e) {
				throw e;
			}
			catch (final Exception e) {
				throw new IOException(
						e);
			}

			// System.out.println(((SimpleFeature)unwrappedValue).getDefaultGeometry().toString()
			// + " = " + partitionData.toString());

		}

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, Object, PartitionDataWritable, AdapterWithObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			LOGGER.info("Running NNMapper");
			final ConfigurationWrapper config = new LoggingConfigurationWrapper(
					LOGGER,
					new JobContextConfigurationWrapper(
							context,
							LOGGER));
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
		protected TypeConverter<VALUEIN> typeConverter = new TypeConverter<VALUEIN>() {

			@SuppressWarnings("unchecked")
			@Override
			public VALUEIN convert(
					final ByteArrayId id,
					final Object o ) {
				return (VALUEIN) o;
			}

		};

		protected DistanceProfileGenerateFn<?, VALUEIN> distanceProfileFn = new LocalDistanceProfileGenerateFn();

		@Override
		protected void reduce(
				final PartitionDataWritable key,
				final Iterable<AdapterWithObjectWritable> values,
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context )
				throws IOException,
				InterruptedException {
			final Map<ByteArrayId, VALUEIN> primaries = new HashMap<ByteArrayId, VALUEIN>();
			final Map<ByteArrayId, VALUEIN> others = new HashMap<ByteArrayId, VALUEIN>();
			final PARTITION_SUMMARY summary = createSummary();

			for (final AdapterWithObjectWritable inputValue : values) {

				final VALUEIN unwrappedValue = typeConverter.convert(
						inputValue.getDataId(),
						AdapterWithObjectWritable.fromWritableWithAdapter(
								serializationTool,
								inputValue));
				if (inputValue.isPrimary()) {
					primaries.put(
							inputValue.getDataId(),
							unwrappedValue);
				}
				else {
					if (!primaries.containsKey(inputValue.getDataId())) {
						others.put(
								inputValue.getDataId(),
								unwrappedValue);
					}
				}
			}

			LOGGER.warn("Processing " + key.toString() + " with primary = " + primaries.size() + " and other = " + others.size());

			final NeighborIndex<VALUEIN> index = new NeighborIndex<VALUEIN>(
					this.createNeighborsListFactory(summary));

			final Iterator<Map.Entry<ByteArrayId, VALUEIN>> primaryIt = primaries.entrySet().iterator();
			while (primaryIt.hasNext()) {
				final Map.Entry<ByteArrayId, VALUEIN> primary = primaryIt.next();
				final NeighborList<VALUEIN> primaryList = index.init(primary);
				for (final Map.Entry<ByteArrayId, VALUEIN> anotherPrimary : primaries.entrySet()) {
					if (anotherPrimary.getKey().equals(
							primary.getKey())) {
						continue;
					}
					if (!primaryList.contains(anotherPrimary.getKey())) {
						final DistanceProfile<?> distanceProfile = distanceProfileFn.computeProfile(
								primary.getValue(),
								anotherPrimary.getValue());
						if (distanceProfile.getDistance() <= maxDistance) {
							index.add(
									distanceProfile,
									primary,
									anotherPrimary,
									true);
						}
					}
				}
				context.progress();
				for (final Map.Entry<ByteArrayId, VALUEIN> anOther : others.entrySet()) {
					if (anOther.getKey().equals(
							primary.getKey())) {
						continue;
					}
					if (!primaryList.contains(anOther.getKey())) {
						final DistanceProfile<?> distanceProfile = distanceProfileFn.computeProfile(
								primary.getValue(),
								anOther.getValue());
						if (distanceProfile.getDistance() <= maxDistance) {
							index.add(
									distanceProfile,
									primary,
									anOther,
									false);
						}
					}
				}
				context.progress();
				processNeighbors(
						key.partitionData,
						primary.getKey(),
						primary.getValue(),
						primaryList,
						context,
						summary);

				// the list is not needed once the primary has been thoroughly
				// processed.
				// child classes may use there on collections to retain neighbor
				// list, if needed.
				index.empty(primary.getKey());
				primaryIt.remove();
			}

			processSummary(
					key.partitionData,
					summary,
					context);
		}

		public NeighborListFactory<VALUEIN> createNeighborsListFactory(
				PARTITION_SUMMARY summary ) {
			return new DefaultNeighborList.DefaultNeighborListFactory<VALUEIN>();
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
				PartitionData partitionData,
				PARTITION_SUMMARY summary,
				Reducer<PartitionDataWritable, AdapterWithObjectWritable, KEYOUT, VALUEOUT>.Context context )
				throws IOException,
				InterruptedException;

		protected abstract void processNeighbors(
				PartitionData partitionData,
				ByteArrayId primaryId,
				VALUEIN primary,
				NeighborList<VALUEIN> neighbors,
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
						FeatureGeometryDistanceFn.class);
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

			LOGGER.info(
					"Maximum Neighbors = {}",
					maxNeighbors);
		}

		protected class LocalDistanceProfileGenerateFn implements
				DistanceProfileGenerateFn<Object, VALUEIN>
		{

			// for GC concerns in the default NN case
			DistanceProfile<Object> singleNotThreadSafeImage = new DistanceProfile<Object>();

			@Override
			public DistanceProfile<Object> computeProfile(
					VALUEIN item1,
					VALUEIN item2 ) {
				singleNotThreadSafeImage.setDistance(distanceFn.measure(
						item1,
						item2));
				return singleNotThreadSafeImage;
			}

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
				final PartitionData partitionData,
				final ByteArrayId primaryId,
				final SimpleFeature primary,
				final NeighborList<SimpleFeature> neighbors,
				final Reducer<PartitionDataWritable, AdapterWithObjectWritable, Text, Text>.Context context,
				final Boolean summary )
				throws IOException,
				InterruptedException {
			if ((neighbors == null) || (neighbors.size() == 0)) {
				return;
			}
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
				for (final Map.Entry<ByteArrayId, SimpleFeature> neighbor : neighbors) {
					if (neighborsText.getLength() > 0) {
						neighborsText.append(
								sepBytes,
								0,
								sepBytes.length);
					}
					utfBytes = neighbor.getValue().getID().getBytes(
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
				final PartitionData partitionData,
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

		public PartitionData getPartitionData() {
			return partitionData;
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
			final int val = SignedBytes.lexicographicalComparator().compare(
					partitionData.getId().getBytes(),
					o.partitionData.getId().getBytes());
			if ((val == 0) && (o.partitionData.getGroupId() != null) && (partitionData.getGroupId() != null)) {
				return SignedBytes.lexicographicalComparator().compare(
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
