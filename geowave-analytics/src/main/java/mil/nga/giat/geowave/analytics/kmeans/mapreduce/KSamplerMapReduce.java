package mil.nga.giat.geowave.analytics.kmeans.mapreduce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveWritableInputMapper;
import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveWritableInputReducer;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.extract.CentroidExtractor;
import mil.nga.giat.geowave.analytics.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.GlobalParameters;
import mil.nga.giat.geowave.analytics.parameters.SampleParameters;
import mil.nga.giat.geowave.analytics.sample.functions.RandomSamplingRankFunction;
import mil.nga.giat.geowave.analytics.sample.functions.SamplingRankFunction;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.mapreduce.JobContextConfigurationWrapper;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.index.IndexType;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import com.vividsolutions.jts.geom.Point;

/**
 * Samples a random 'k' number of features from a population of geospatial
 * features PER GROUP. Outputs the samples in SimpleFeatures. Sampling is
 * achieved by picking the top ranked input objects. Rank is determined by a
 * sample function implementing {@link SamplingRankFunction}.
 * 
 * The input features should have a groupID set if they intend to be sampled by
 * group.
 * 
 * Keys are partitioned by the group ID in an attempt to process each group in a
 * separate reducer.
 * 
 * Sampled features are written to as a new SimpleFeature to a data store. The
 * SimpleFeature contains attributes:
 * 
 * @formatter:off
 * 
 *                name - data id of the sampled point
 * 
 *                weight - can be anything including the sum of all assigned
 *                feature distances
 * 
 *                geometry - geometry of the sampled features
 * 
 *                count - to hold the number of assigned features
 * 
 *                groupID - the assigned group ID to the input objects
 * @formatter:on
 * 
 *               Properties:
 * @formatter:off
 * 
 *                "KSamplerMapReduce.Sample.SampleSize" - number of input
 *                objects to sample. defaults to 1.
 * 
 *                "KSamplerMapReduce.Sample.DataTypeId" - Id of the data type to
 *                store the k samples - defaults to "centroids"
 * 
 *                "KSamplerMapReduce.Centroid.ExtractorClass" - extracts a
 *                centroid from an item. This parameter allows customization of
 *                determining one or more representative centroids for a
 *                geometry.
 * 
 *                "KSamplerMapReduce.Sample.IndexId" - The Index ID used for
 *                output simple features.
 * 
 *                "KSamplerMapReduce.Sample.SampleRankFunction" - An
 *                implementation of {@link SamplingRankFunction} used to rank
 *                the input object.
 * 
 *                "KSamplerMapReduce.Centroid.ZoomLevel" - Sets an attribute on
 *                the sampled objects recording a zoom level used in the
 *                sampling process. The interpretation of the attribute is not
 *                specified or assumed.
 * 
 *                "KSamplerMapReduce.Global.BatchId" ->the id of the batch;
 *                defaults to current time in millis (for range comparisons)
 * 
 *                "KSamplerMapReduce.Centroid.WrapperFactoryClass" ->
 *                {@link AnalyticItemWrapperFactory} to extract non-geometric
 *                dimensions
 * 
 * @formatter:on
 */
public class KSamplerMapReduce
{
	protected static final Logger LOGGER = Logger.getLogger(KSamplerMapReduce.class);

	public static class SampleMap<T> extends
			GeoWaveWritableInputMapper<GeoWaveInputKey, ObjectWritable>
	{

		protected GeoWaveInputKey outputKey = new GeoWaveInputKey();
		private final KeyManager keyManager = new KeyManager();
		private SamplingRankFunction<T> samplingFunction;
		private ObjectWritable currentValue;
		private AnalyticItemWrapperFactory<Object> itemWrapperFactory;
		private int sampleSize = 1;
		private NestedGroupCentroidAssignment<Object> nestedGroupCentroidAssigner;

		// Override parent since there is not need to decode the value.
		@Override
		protected void mapWritableValue(
				final GeoWaveInputKey key,
				final ObjectWritable value,
				final Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			// cached for efficiency since the output is the input object
			// the de-serialized input object is only used for sampling.
			// For simplicity, allow the de-serialization to occur in all cases,
			// even though some sampling
			// functions do not inspect the input object.
			currentValue = value;
			super.mapWritableValue(
					key,
					value,
					context);
		}

		@Override
		protected void mapNativeValue(
				final GeoWaveInputKey key,
				final Object value,
				final org.apache.hadoop.mapreduce.Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			@SuppressWarnings("unchecked")
			final double rank = samplingFunction.rank(
					sampleSize,
					(T) value);
			if (rank > 0.0000000001) {
				AnalyticItemWrapper<Object> wrapper = itemWrapperFactory.create(value);
				outputKey.setDataId(new ByteArrayId(
						keyManager.putData(
								nestedGroupCentroidAssigner.getGroupForLevel(wrapper),
								1.0 - rank, // sorts in ascending order
								key.getDataId().getBytes())));
				outputKey.setAdapterId(key.getAdapterId());
				outputKey.setAccumuloKey(key.getAccumuloKey());
				context.write(
						outputKey,
						currentValue);
			}
		}

		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);

			ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context,
					KSamplerMapReduce.LOGGER);
			sampleSize = config.getInt(
					SampleParameters.Sample.SAMPLE_SIZE,
					KSamplerMapReduce.class,
					1);

			try {
				nestedGroupCentroidAssigner = new NestedGroupCentroidAssignment<Object>(
						config);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}

			try {
				samplingFunction = config.getInstance(
						SampleParameters.Sample.SAMPLE_RANK_FUNCTION,
						KSamplerMapReduce.class,
						SamplingRankFunction.class,
						RandomSamplingRankFunction.class);

				samplingFunction.initialize(config);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}
			try {
				itemWrapperFactory = config.getInstance(
						CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
						KSamplerMapReduce.class,
						AnalyticItemWrapperFactory.class,
						SimpleFeatureItemWrapperFactory.class);

				itemWrapperFactory.initialize(config);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}

		}
	}

	public static class SampleReducer<T> extends
			GeoWaveWritableInputReducer<GeoWaveOutputKey, T>
	{

		private int maxCount = 1;
		private CentroidExtractor<T> centroidExtractor;
		private AnalyticItemWrapperFactory<T> itemWrapperFactory;
		private ByteArrayId sampleDataTypeId = null;
		private ByteArrayId indexId;
		private int zoomLevel = 1;
		private String batchID;
		private final Map<String, Integer> outputCounts = new HashMap<String, Integer>();

		@Override
		protected void reduceNativeValues(
				final GeoWaveInputKey key,
				final Iterable<Object> values,
				final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, T>.Context context )
				throws IOException,
				InterruptedException {

			String groupID = KeyManager.getGroupAsString(key.getDataId().getBytes());

			for (final Object value : values) {
				final AnalyticItemWrapper<T> sampleItem = itemWrapperFactory.create((T) value);
				Integer outputCount = outputCounts.get(groupID);
				outputCount = outputCount == null ? Integer.valueOf(0) : outputCount;
				if ((outputCount == null) || (outputCount < maxCount)) {

					AnalyticItemWrapper<T> centroid = createCentroid(
							groupID,
							sampleItem);
					if (centroid != null) {
						context.write(
								new GeoWaveOutputKey(
										sampleDataTypeId,
										indexId),
								centroid.getWrappedItem());
						outputCount++;
						outputCounts.put(
								groupID,
								outputCount);
					}
				}
			}
		}

		private AnalyticItemWrapper<T> createCentroid(
				final String groupID,
				final AnalyticItemWrapper<T> item ) {
			final Point point = centroidExtractor.getCentroid(item.getWrappedItem());
			final AnalyticItemWrapper<T> nextCentroid = itemWrapperFactory.createNextItem(
					item.getWrappedItem(),
					groupID,
					point.getCoordinate(),
					item.getExtraDimensions(),
					item.getDimensionValues());

			nextCentroid.setBatchID(batchID);
			nextCentroid.setGroupID(groupID);
			nextCentroid.setZoomLevel(zoomLevel);
			return nextCentroid;
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(
				final Reducer<GeoWaveInputKey, ObjectWritable, GeoWaveOutputKey, T>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);

			ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context,
					KSamplerMapReduce.LOGGER);

			maxCount = config.getInt(
					SampleParameters.Sample.SAMPLE_SIZE,
					KSamplerMapReduce.class,
					1);

			zoomLevel = config.getInt(
					CentroidParameters.Centroid.ZOOM_LEVEL,
					KSamplerMapReduce.class,
					1);

			sampleDataTypeId = new ByteArrayId(
					StringUtils.stringToBinary(config.getString(
							SampleParameters.Sample.DATA_TYPE_ID,
							KSamplerMapReduce.class,
							"sample")));

			batchID = config.getString(
					GlobalParameters.Global.BATCH_ID,
					KSamplerMapReduce.class,
					UUID.randomUUID().toString());

			indexId = new ByteArrayId(
					StringUtils.stringToBinary(config.getString(
							SampleParameters.Sample.INDEX_ID,
							KSamplerMapReduce.class,
							IndexType.SPATIAL_VECTOR.getDefaultId())));

			try {
				centroidExtractor = config.getInstance(
						CentroidParameters.Centroid.EXTRACTOR_CLASS,
						KSamplerMapReduce.class,
						CentroidExtractor.class,
						SimpleFeatureCentroidExtractor.class);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}

			try {
				itemWrapperFactory = config.getInstance(
						CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
						KSamplerMapReduce.class,
						AnalyticItemWrapperFactory.class,
						SimpleFeatureItemWrapperFactory.class);

				itemWrapperFactory.initialize(config);
			}
			catch (final Exception e1) {

				throw new IOException(
						e1);
			}
		}
	}

	public static class SampleKeyPartitioner extends
			Partitioner<GeoWaveInputKey, ObjectWritable>
	{
		@Override
		public int getPartition(
				GeoWaveInputKey key,
				ObjectWritable val,
				int numPartitions ) {
			final byte[] grpIDInBytes = KeyManager.getGroup(key.getDataId().getBytes());
			int partition = hash(grpIDInBytes) % numPartitions;
			return partition;
		}

		private int hash(
				byte[] data ) {
			int code = 1;
			int i = 0;
			for (byte b : data) {
				code += b * Math.pow(
						31,
						data.length - 1 - (i++));
			}
			return code;
		}
	}

	private static class KeyManager
	{
		private ByteBuffer keyBuffer = ByteBuffer.allocate(64);

		private static String getGroupAsString(
				final byte[] data ) {
			return new String(
					getGroup(data),
					StringUtils.UTF8_CHAR_SET);
		}

		private static byte[] getGroup(
				final byte[] data ) {
			final ByteBuffer buffer = ByteBuffer.wrap(data);
			buffer.getDouble();
			final int len = buffer.getInt();
			return Arrays.copyOfRange(
					data,
					buffer.position(),
					(buffer.position() + len));
		}

		private byte[] putData(
				final String groupID,
				final double weight,
				final byte[] dataIdBytes ) {
			keyBuffer.rewind();
			final byte[] groupIDBytes = groupID.getBytes(StringUtils.UTF8_CHAR_SET);
			// try to reuse
			final int size = dataIdBytes.length + 16 + groupIDBytes.length;
			if (keyBuffer.capacity() < size) {
				keyBuffer = ByteBuffer.allocate(size);
			}
			keyBuffer.putDouble(weight);
			keyBuffer.putInt(groupIDBytes.length);
			keyBuffer.put(groupIDBytes);
			keyBuffer.putInt(dataIdBytes.length);
			keyBuffer.put(dataIdBytes);
			return keyBuffer.array();
		}
	}
}
