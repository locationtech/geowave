package mil.nga.giat.geowave.analytics.kmeans.mapreduce;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveConfiguratorBase;
import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveWritableInputMapper;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.analytics.clustering.CentroidManager;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners.StripWeakCentroidsRunner.BreakStrategy;
import mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners.StripWeakCentroidsRunner.MaxChangeBreakStrategy;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.JobContextConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.opengis.feature.simple.SimpleFeature;

/**
 * Strip weak centroids from the centroid set.
 * 
 * Nothing is output in the map reduce. Instead, items are deleted.
 * 
 * For Properties, see {@link CentroidManagerGeoWave}
 * 
 * @formatter off
 * 
 *            "StripWeakMapReduce.Centroid.WrapperFactoryClass" ->
 *            {@link AnalyticItemWrapperFactory} to extract wrap spatial objects
 *            with Centroid management functions
 * 
 * @See CentroidManagerGeoWave
 * 
 * @formatter on
 */
public class StripWeakMapReduce
{
	protected static final Logger LOGGER = Logger.getLogger(StripWeakMapReduce.class);

	public static class StripWeakMap extends
			GeoWaveWritableInputMapper<Text, ObjectWritable>
	{

		protected Text outputKey = new Text();
		private ObjectWritable currentValue;
		private AnalyticItemWrapperFactory<Object> itemWrapperFactory;

		// Override parent since there is not need to decode the value.
		@Override
		protected void mapWritableValue(
				final GeoWaveInputKey key,
				final ObjectWritable value,
				final Mapper<GeoWaveInputKey, ObjectWritable, Text, ObjectWritable>.Context context )
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

		@SuppressWarnings("unchecked")
		@Override
		protected void mapNativeValue(
				final GeoWaveInputKey key,
				final Object value,
				final org.apache.hadoop.mapreduce.Mapper.Context context )
				throws IOException,
				InterruptedException {
			outputKey.set(itemWrapperFactory.create(
					value).getGroupID());
			context.write(
					outputKey,
					currentValue);
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, ObjectWritable, Text, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);

			ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context,
					LOGGER);
			try {
				itemWrapperFactory = config.getInstance(
						CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
						StripWeakMapReduce.class,
						AnalyticItemWrapperFactory.class,
						SimpleFeatureItemWrapperFactory.class);

				itemWrapperFactory.initialize(config);
			}
			catch (final Exception e) {
				LOGGER.error("Cannot instantiate " + GeoWaveConfiguratorBase.enumToConfKey(
						StripWeakMapReduce.class,
						CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS));
				throw new IOException(
						e);
			}
		}
	}

	public static class StripWeakReducer extends
			Reducer<Text, ObjectWritable, GeoWaveOutputKey, SimpleFeature>
	{

		private CentroidManager<Object> centroidManager;
		private final int minimum = 3;
		private final BreakStrategy<Object> breakStrategy = new	MaxChangeBreakStrategy<Object>();

		@Override
		public void reduce(
				final Text key,
				final Iterable<ObjectWritable> values,
				final Reducer<Text, ObjectWritable, GeoWaveOutputKey, SimpleFeature>.Context context )
				throws IOException,
				InterruptedException {
			final List<AnalyticItemWrapper<Object>> centroids = centroidManager.getCentroidsForGroup(key.toString());

			if (centroids.size() <= minimum) {
				return;
			}

			Collections.sort(
					centroids,
					new Comparator<AnalyticItemWrapper<?>>() {

						@Override
						public int compare(
								final AnalyticItemWrapper<?> arg0,
								final AnalyticItemWrapper<?> arg1 ) {
							// be careful of overflow
							// also, descending
							return (arg1.getAssociationCount() - arg0.getAssociationCount()) < 0 ? -1 : 1;
						}
					});
			int position = breakStrategy.getBreakPoint(centroids);

			// make sure we do not delete too many
			// trim bottom third
			position = Math.max(
					minimum,
					position);

			final String toDelete[] = new String[centroids.size() - position];

			int count = 0;
			final Iterator<AnalyticItemWrapper<Object>> it = centroids.iterator();
			while (it.hasNext()) {
				final AnalyticItemWrapper<Object> centroid = it.next();
				if (count++ >= position) {
					toDelete[count - position - 1] = centroid.getID();
				}
			}

			centroidManager.delete(toDelete);

		}

		@Override
		protected void setup(
				final Reducer<Text, ObjectWritable, GeoWaveOutputKey, SimpleFeature>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);
			try {
				centroidManager = new CentroidManagerGeoWave<Object>(
						new JobContextConfigurationWrapper(
								context,
								LOGGER));

			}
			catch (final AccumuloException e) {
				LOGGER.error("Cannot connect to Accumulo");
				throw new IOException(
						e);
			}
			catch (final AccumuloSecurityException e) {
				LOGGER.error("Cannot connect to Accumulo");
				throw new IOException(
						e);
			}
		}

	}
}
