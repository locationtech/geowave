package mil.nga.giat.geowave.analytics.clustering.mapreduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import mil.nga.giat.geowave.accumulo.mapreduce.GeoWaveWritableInputMapper;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputKey;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.clustering.CentroidPairing;
import mil.nga.giat.geowave.analytics.clustering.NestedGroupCentroidAssignment;
import mil.nga.giat.geowave.analytics.extract.CentroidExtractor;
import mil.nga.giat.geowave.analytics.extract.SimpleFeatureCentroidExtractor;
import mil.nga.giat.geowave.analytics.kmeans.AssociationNotification;
import mil.nga.giat.geowave.analytics.parameters.CentroidParameters;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.ConfigurationWrapper;
import mil.nga.giat.geowave.analytics.tools.SimpleFeatureItemWrapperFactory;
import mil.nga.giat.geowave.analytics.tools.mapreduce.GroupIDText;
import mil.nga.giat.geowave.analytics.tools.mapreduce.JobContextConfigurationWrapper;

import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

/**
 * Adjust input items so that so that the assigned centroid becomes the group
 * ID. If the item has an assigned group ID, the resulting item's group ID is
 * replaced in the output.
 * 
 * From a multi-level clustering algorithm, an item has a different grouping in
 * each level. Items are clustered within their respective groups.
 * 
 * @formatter:off
 * 
 *                Context configuration parameters include:
 * 
 *                "GroupAssignmentMapReduce.Common.DistanceFunctionClass" ->
 *                Used to determine distance to centroid
 * 
 *                "GroupAssignmentMapReduce.Centroid.ExtractorClass" ->
 *                {@link mil.nga.giat.geowave.analytics.extract.CentroidExtractor}
 * 
 *                "GroupAssignmentMapReduce.Centroid.WrapperFactoryClass" ->
 *                {@link AnalyticItemWrapperFactory} to extract wrap spatial
 *                objects with Centroid management functions
 * 
 *                "GroupAssignmentMapReduce.Centroid.ZoomLevel" -> The current
 *                zoom level
 * 
 * @see CentroidManagerGeoWave
 * @formatter:on
 * 
 */
public class GroupAssignmentMapReduce
{

	protected static final Logger LOGGER = Logger.getLogger(GroupAssignmentMapReduce.class);

	public static class GroupAssignmentMapper extends
			GeoWaveWritableInputMapper<GeoWaveInputKey, ObjectWritable>
	{

		private NestedGroupCentroidAssignment<Object> nestedGroupCentroidAssigner;
		protected GroupIDText outputKeyWritable = new GroupIDText();
		protected ObjectWritable outputValWritable = new ObjectWritable();
		protected CentroidExtractor<Object> centroidExtractor;
		protected AnalyticItemWrapperFactory<Object> itemWrapperFactory;
		private Map<String, AtomicInteger> LogCounts = new HashMap<String, AtomicInteger>();

		@Override
		protected void mapNativeValue(
				final GeoWaveInputKey key,
				final Object value,
				final org.apache.hadoop.mapreduce.Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			final AssociationNotification<Object> centroidAssociationFn = new AssociationNotification<Object>() {
				@Override
				public void notify(
						final CentroidPairing<Object> pairing ) {
					pairing.getPairedItem().setGroupID(
							pairing.getCentroid().getID());
					pairing.getPairedItem().setZoomLevel(
							pairing.getCentroid().getZoomLevel()+1);
					// just get the contents of the returned ObjectWritable to avoid
					// having to assign outputValWritable rather than update its contents.
					// the 'toWritabeValue' method is efficient, not creating an extra instance of
					// ObjectWritable each time, so this is just a simple exchange of a reference
					outputValWritable.set(toWritableValue(
							key,
							pairing.getPairedItem().getWrappedItem()).get());
					AtomicInteger ii = LogCounts.get(pairing.getCentroid().getID());
					
					if (ii == null) {
						ii = new AtomicInteger(
								0);
						LogCounts.put(
								pairing.getCentroid().getID(),
								ii);
					}
					ii.incrementAndGet();
				}
			};

			nestedGroupCentroidAssigner.findCentroidForLevel(
					itemWrapperFactory.create(value),
					centroidAssociationFn);

			context.write(
					key,
					outputValWritable);
		}

		@Override
		protected void cleanup(
				final org.apache.hadoop.mapreduce.Mapper.Context context )
				throws IOException,
				InterruptedException {

			for (Entry<String, AtomicInteger> e : LogCounts.entrySet()) {
				GroupAssignmentMapReduce.LOGGER.trace(e.getKey() + " = " + e.getValue());
			}
			super.cleanup(context);
		}

		@Override
		protected void setup(
				final Mapper<GeoWaveInputKey, ObjectWritable, GeoWaveInputKey, ObjectWritable>.Context context )
				throws IOException,
				InterruptedException {
			super.setup(context);

			final ConfigurationWrapper config = new JobContextConfigurationWrapper(
					context,
					GroupAssignmentMapReduce.LOGGER);

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
						GroupAssignmentMapReduce.class,
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
						GroupAssignmentMapReduce.class,
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
}
