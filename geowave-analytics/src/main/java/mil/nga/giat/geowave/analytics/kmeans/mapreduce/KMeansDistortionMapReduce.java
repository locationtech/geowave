package mil.nga.giat.geowave.analytics.kmeans.mapreduce;

import com.vividsolutions.jts.geom.Point;
import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveWritableInputMapper;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.clustering.CentroidPairing;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.extract.CentroidExtractor;
import mil.nga.giat.geowave.analytics.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytics.kmeans.AssociationNotification;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.parameters.JumpParameters;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.mapreduce.CountofDoubleWritable;
import mil.nga.giat.geowave.analytics.tools.mapreduce.JobContextConfigurationWrapper;
import mil.nga.giat.geowave.index.StringUtils;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

/**
 * Calculate the distortation.
 * <p/>
 * See Catherine A. Sugar and Gareth M. James (2003).
 * "Finding the number of clusters in a data set: An information theoretic approach"
 * Journal of the American Statistical Association 98 (January): 750–763
 * 
 * @formatter:off Context configuration parameters include:
 *                <p/>
 *                "KMeansDistortionMapReduce.Common.DistanceFunctionClass" ->
 *                {@link mil.nga.giat.geowave.analytics.distance.DistanceFn}
 *                used to determine distance to centroid
 *                <p/>
 *                "KMeansDistortionMapReduce.Centroid.WrapperFactoryClass" ->
 *                {@link AnalyticItemWrapperFactory} to extract wrap spatial
 *                objects with Centroid management functions
 *                <p/>
 *                "KMeansDistortionMapReduce.Centroid.ExtractorClass" ->
 *                {@link mil.nga.giat.geowave.analytics.extract.CentroidExtractor}
 *                <p/>
 *                "KMeansDistortionMapReduce.Jump.CountOfCentroids" -> May be
 *                different from actual.
 * @formatter:on
 * @see CentroidManagerGeoWave
 */
public class KMeansDistortionMapReduce
{

	protected static final Logger LOGGER = Logger.getLogger(KMeansDistortionMapReduce.class);

	public static class KMeansDistortionMapper extends
			GeoWaveWritableInputMapper<Text, CountofDoubleWritable>
	{

		private NestedGroupCentroidAssignment<Object> nestedGroupCentroidAssigner;
		private final Text outputKeyWritable = new Text(
				"1");
		private final CountofDoubleWritable outputValWritable = new CountofDoubleWritable();
		private CentroidExtractor<Object> centroidExtractor;
		private AnalyticItemWrapperFactory<Object> itemWrapperFactory;

		AssociationNotification<Object> centroidAssociationFn = new AssociationNotification<Object>() {
			@Override
			public void notify(
					final CentroidPairing<Object> pairing ) {
				outputKeyWritable.set(pairing.getCentroid().getGroupID());
				final double extraFromItem[] = pairing.getPairedItem().getDimensionValues();
				final double extraCentroid[] = pairing.getCentroid().getDimensionValues();
				final Point p = centroidExtractor.getCentroid(pairing.getPairedItem().getWrappedItem());

				final Point centroid = centroidExtractor.getCentroid(pairing.getCentroid().getWrappedItem());

				// calculate error for dp
				// using identity matrix for the common covariance, therefore
				// E[(p - c)^-1 * cov * (p - c)] => (px - cx)^2 + (py - cy)^2
				double expectation = 0.0;
				for (int i = 0; i < extraCentroid.length; i++) {
					expectation += Math.pow(
							extraFromItem[i] - extraCentroid[i],
							2);
				}
				expectation += (Math.pow(
						p.getCoordinate().x - centroid.getCoordinate().x,
						2) + Math.pow(
						p.getCoordinate().y - centroid.getCoordinate().y,
						2));
				// + Math.pow(
				// p.getCoordinate().z - centroid.getCoordinate().z,
				// 2));
				outputValWritable.set(
						expectation,
						1);
			}
		};

		@Override
		protected void mapNativeValue(
				final GeoWaveInputKey key,
				final Object value,
				final org.apache.hadoop.mapreduce.Mapper<GeoWaveInputKey, ObjectWritable, Text, CountofDoubleWritable>.Context context )
				throws IOException,
				InterruptedException {
			nestedGroupCentroidAssigner.findCentroidForLevel(
					itemWrapperFactory.create(value),
					centroidAssociationFn);
			context.write(
					outputKeyWritable,
					outputValWritable);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, ObjectWritable, Text, CountofDoubleWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			final ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context,
					KMeansDistortionMapReduce.LOGGER);

			try {
				nestedGroupCentroidAssigner = new NestedGroupCentroidAssignment<Object>(
						config);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}

			try {
				centroidExtractor = config.getInstance(
						CentroidParameters.Centroid.EXTRACTOR_CLASS,
						KMeansDistortionMapReduce.class,
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
						KMeansDistortionMapReduce.class,
						AnalyticItemWrapperFactory.class,
						SimpleFeatureItemWrapperFactory.class);
			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}
		}
	}

	public static class KMeansDistorationCombiner extends
			Reducer<Text, CountofDoubleWritable, Text, CountofDoubleWritable>
	{
		final CountofDoubleWritable outputValue = new CountofDoubleWritable();

		@Override
		public void reduce(
				final Text key,
				final Iterable<CountofDoubleWritable> values,
				final Reducer<Text, CountofDoubleWritable, Text, CountofDoubleWritable>.Context context )
				throws IOException,
				InterruptedException {

			double expectation = 0;
			double ptCount = 0;
			for (final CountofDoubleWritable value : values) {
				expectation += value.getValue();
				ptCount += value.getCount();
			}
			outputValue.set(
					expectation,
					ptCount);
			context.write(
					key,
					outputValue);
		}
	}

	public static class KMeansDistortionReduce extends
			Reducer<Text, CountofDoubleWritable, Text, Mutation>
	{
		private String expectedK = null;
		final protected Text output = new Text(
				"");
		private CentroidManagerGeoWave<Object> centroidManager;

		@Override
		public void reduce(
				final Text key,
				final Iterable<CountofDoubleWritable> values,
				final Reducer<Text, CountofDoubleWritable, Text, Mutation>.Context context )
				throws IOException,
				InterruptedException {
			double expectation = 0.0;
			final List<AnalyticItemWrapper<Object>> centroids = centroidManager.getCentroidsForGroup(key.toString());
			// it is possible that the number of items in a group are smaller
			// than the cluster
			final String kCount = expectedK == null ? Integer.toString(centroids.size()) : expectedK;
			if (centroids.size() == 0) {
				return;
			}
			final double numDimesions = 2 + centroids.get(
					0).getExtraDimensions().length;

			double ptCount = 0;
			for (final CountofDoubleWritable value : values) {
				expectation += value.getValue();
				ptCount += value.getCount();
			}

			if (ptCount > 0) {
				expectation /= ptCount;

				final Double distortion = Math.pow(
						expectation / numDimesions,
						-(numDimesions / 2));

				// key: group ID | "DISTORTION" | K
				// value: distortion value
				final Mutation m = new Mutation(
						key.toString());
				m.put(
						new Text(
								"dt"),
						new Text(
								kCount),
						new Value(
								distortion.toString().getBytes(
										StringUtils.UTF8_CHAR_SET)));

				// write distortion to accumulo, defaults to table given to
				// AccumuloOutputFormat, in driver
				context.write(
						output, // default table
						m);
			}
		}

		@Override
		protected void setup(
				final Reducer<Text, CountofDoubleWritable, Text, Mutation>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			final ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context,
					KMeansDistortionMapReduce.LOGGER);

			final int k = config.getInt(
					JumpParameters.Jump.COUNT_OF_CENTROIDS,
					KMeansDistortionMapReduce.class,
					-1);
			if (k > 0) {
				expectedK = Integer.toString(k);
			}

			try {
				centroidManager = new CentroidManagerGeoWave<Object>(
						config);
			}
			catch (final Exception e) {
				KMeansDistortionMapReduce.LOGGER.warn(
						"Unable to initialize centroid manager",
						e);
				throw new IOException(
						"Unable to initialize centroid manager");
			}

		}
	}

}
