package mil.nga.giat.geowave.analytics.kmeans.mapreduce;

import java.io.IOException;
import java.util.List;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveWritableInputMapper;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.accumulo.mapreduce.output.GeoWaveOutputKey;
import mil.nga.giat.geowave.analytics.clustering.CentroidManager;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.clustering.CentroidPairing;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.kmeans.AssociationNotification;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.mapreduce.CountofDoubleWritable;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GroupIDText;
import mil.nga.giat.geowave.analytics.tools.mapreduce.JobContextConfigurationWrapper;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

/**
 * Update the SINGLE cost of the clustering as a measure of distance from all
 * points to their closest center.
 * 
 * As an FYI: During the clustering algorithm, the cost should be monotonic
 * decreasing.
 * 
 * @formatter:off
 * 
 *                Context configuration parameters include:
 * 
 *                "UpdateCentroidCostMapReduce.Common.DistanceFunctionClass" ->
 *                Used to determine distance to centroid
 * 
 *                "UpdateCentroidCostMapReduce.Centroid.WrapperFactoryClass" ->
 *                {@link AnalyticItemWrapperFactory} to extract wrap spatial
 *                objects with Centroid management functions
 * 
 * @see CentroidManagerGeoWave
 * 
 * @formatter:on
 * 
 */

public class UpdateCentroidCostMapReduce
{

	protected static final Logger LOGGER = Logger.getLogger(UpdateCentroidCostMapReduce.class);

	public static class UpdateCentroidCostMap extends
			GeoWaveWritableInputMapper<GroupIDText, CountofDoubleWritable>
	{
		private NestedGroupCentroidAssignment<Object> nestedGroupCentroidAssigner;
		private final CountofDoubleWritable dw = new CountofDoubleWritable();
		protected final GroupIDText outputWritable = new GroupIDText();
		protected AnalyticItemWrapperFactory<Object> itemWrapperFactory;

		private final AssociationNotification<Object> centroidAssociationFn = new AssociationNotification<Object>() {
			@Override
			public void notify(
					final CentroidPairing<Object> pairing ) {
				outputWritable.set(
						pairing.getCentroid().getGroupID(),
						pairing.getCentroid().getID());
			}
		};

		@Override
		protected void mapNativeValue(
				final GeoWaveInputKey key,
				final Object value,
				final Mapper<GeoWaveInputKey, ObjectWritable, GroupIDText, CountofDoubleWritable>.Context context )
				throws IOException,
				InterruptedException {
			final AnalyticItemWrapper<Object> wrappedItem = itemWrapperFactory.create(value);
			dw.set(
					nestedGroupCentroidAssigner.findCentroidForLevel(
							wrappedItem,
							centroidAssociationFn),
					1.0);

			context.write(
					outputWritable,
					dw);
		}

		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, ObjectWritable, GroupIDText, CountofDoubleWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);

			final ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context,
					UpdateCentroidCostMapReduce.LOGGER);

			try {
				nestedGroupCentroidAssigner = new NestedGroupCentroidAssignment<Object>(
						config);

			}
			catch (final Exception e1) {
				throw new IOException(
						e1);
			}

			try {
				itemWrapperFactory = config.getInstance(
						CentroidParameters.Centroid.WRAPPER_FACTORY_CLASS,
						KMeansMapReduce.class,
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

	public static class UpdateCentroidCostCombiner extends
			Reducer<GroupIDText, CountofDoubleWritable, GroupIDText, CountofDoubleWritable>
	{
		final CountofDoubleWritable outputValue = new CountofDoubleWritable();

		@Override
		public void reduce(
				final GroupIDText key,
				final Iterable<CountofDoubleWritable> values,
				final Reducer<GroupIDText, CountofDoubleWritable, GroupIDText, CountofDoubleWritable>.Context context )
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

	public static class UpdateCentroidCostReducer extends
			Reducer<GroupIDText, CountofDoubleWritable, GeoWaveOutputKey, Object>
	{

		private CentroidManager<Object> centroidManager;

		@Override
		protected void reduce(
				final GroupIDText key,
				final Iterable<CountofDoubleWritable> values,
				final Reducer<GroupIDText, CountofDoubleWritable, GeoWaveOutputKey, Object>.Context context )
				throws IOException,
				InterruptedException {

			final String id = key.getID();
			final String groupID = key.getGroupID();

			double cum = 0.0;
			double count = 0;
			for (final CountofDoubleWritable next : values) {
				cum += next.getValue();
				count += next.getCount();
			}

			final AnalyticItemWrapper<Object> centroid = getFeatureForCentroid(
					id,
					groupID);
			centroid.setCost(cum);
			centroid.resetAssociatonCount();
			centroid.incrementAssociationCount((long) count);

			UpdateCentroidCostMapReduce.LOGGER.info("Update centroid " + centroid.toString());
			context.write(
					new GeoWaveOutputKey(
							centroidManager.getDataTypeId(),
							centroidManager.getIndexId()),
					centroid.getWrappedItem());
		}

		private AnalyticItemWrapper<Object> getFeatureForCentroid(
				final String id,
				final String groupID )
				throws IOException {
			return getFeatureForCentroid(
					id,
					centroidManager.getCentroidsForGroup(groupID));
		}

		private AnalyticItemWrapper<Object> getFeatureForCentroid(
				final String id,
				final List<AnalyticItemWrapper<Object>> centroids ) {
			for (final AnalyticItemWrapper<Object> centroid : centroids) {
				if (centroid.getID().equals(
						id)) {
					return centroid;
				}
			}
			return null;
		}

		@Override
		protected void setup(
				final Reducer<GroupIDText, CountofDoubleWritable, GeoWaveOutputKey, Object>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);

			final ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context,
					UpdateCentroidCostMapReduce.LOGGER);

			try {
				centroidManager = new CentroidManagerGeoWave<Object>(
						config);
			}
			catch (final Exception e) {
				UpdateCentroidCostMapReduce.LOGGER.warn(
						"Unable to initialize centroid manager",
						e);
				throw new IOException(
						"Unable to initialize centroid manager");
			}
		}
	}

}
