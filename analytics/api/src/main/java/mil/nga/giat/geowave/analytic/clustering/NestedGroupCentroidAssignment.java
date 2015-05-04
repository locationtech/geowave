package mil.nga.giat.geowave.analytic.clustering;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.RunnerUtils;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.kmeans.AssociationNotification;
import mil.nga.giat.geowave.analytic.kmeans.CentroidAssociationFn;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.commons.cli.Option;
import org.apache.hadoop.conf.Configuration;

/**
 * 
 * A helper class that finds the closest centroid to a point at a specific zoom
 * level.
 * 
 * If the starting level does match the specified level, then the centroid tree
 * is 'walked' down. Walking up to higher levels is not supported.
 * 
 * Levels are number 1 to n where 1 is the top tier. The current tier being
 * computed may have a different batch ID (temporary) than all upper level
 * tiers. In this case, a parent batch id is provided to resolve groups for
 * those tiers. This approach is often used in speculative computation at each
 * tier.
 * 
 * Parameters include:
 * 
 * @formatter:off
 * 
 *                "NestedGroupCentroidAssignment.Global.ParentBatchId" -> Parent
 *                Tier Batch IDs. If not present then assume value
 *                NestedGroupCentroidAssignment.Global.BatchId
 * 
 *                "NestedGroupCentroidAssignment.Global.BatchId" -> batch id for
 *                current tier.
 * 
 *                "NestedGroupCentroidAssignment.Global.ZoomLevel" -> current
 *                tier (level)
 * 
 *                "NestedGroupCentroidAssignment.Common.DistanceFunctionClass"
 *                -> distance function used for association of data points to
 *                centroid.
 * 
 * @see mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave
 * @formatter:on
 * 
 * @param <T>
 */
public class NestedGroupCentroidAssignment<T>
{
	private final CentroidAssociationFn<T> associationdFunction = new CentroidAssociationFn<T>();
	private final CentroidManager<T> centroidManager;
	private final int endZoomLevel;
	private final String parentBatchID;

	public NestedGroupCentroidAssignment(
			final CentroidManager<T> centroidManager,
			final int endZoomLevel,
			final String parentBatchID,
			final DistanceFn<T> distanceFunction ) {
		super();
		this.centroidManager = centroidManager;
		this.endZoomLevel = endZoomLevel;
		this.parentBatchID = parentBatchID;
		this.associationdFunction.setDistanceFunction(distanceFunction);
	}

	/**
	 * Override zoomLevel from parameters
	 * 
	 * @param config
	 * @param runTimeProperties
	 * @param zoomLevel
	 */
	public static void setZoomLevel(
			final Configuration config,
			final int zoomLevel ) {
		RunnerUtils.setParameter(
				config,
				NestedGroupCentroidAssignment.class,
				new Object[] {
					zoomLevel
				},
				new ParameterEnum[] {
					CentroidParameters.Centroid.ZOOM_LEVEL
				});
	}

	/**
	 * Override zoomLevel from parameters
	 * 
	 * @param config
	 * @param runTimeProperties
	 * @param zoomLevel
	 */
	public static void setParentBatchID(
			final Configuration config,
			final String parentID ) {
		RunnerUtils.setParameter(
				config,
				NestedGroupCentroidAssignment.class,
				new Object[] {
					parentID
				},
				new ParameterEnum[] {
					GlobalParameters.Global.PARENT_BATCH_ID
				});
	}

	public static void fillOptions(
			final Set<Option> options ) {
		CentroidManagerGeoWave.fillOptions(options);

		GlobalParameters.fillOptions(
				options,
				new GlobalParameters.Global[] {
					GlobalParameters.Global.PARENT_BATCH_ID
				});

		CommonParameters.fillOptions(
				options,
				new CommonParameters.Common[] {
					CommonParameters.Common.DISTANCE_FUNCTION_CLASS
				});

		CentroidParameters.fillOptions(
				options,
				new CentroidParameters.Centroid[] {
					CentroidParameters.Centroid.ZOOM_LEVEL
				});
	}

	public NestedGroupCentroidAssignment(
			final ConfigurationWrapper wrapper )
			throws InstantiationException,
			IllegalAccessException,
			AccumuloException,
			IOException,
			AccumuloSecurityException {
		endZoomLevel = wrapper.getInt(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				NestedGroupCentroidAssignment.class,
				1);
		parentBatchID = wrapper.getString(
				GlobalParameters.Global.PARENT_BATCH_ID,
				NestedGroupCentroidAssignment.class,
				wrapper.getString(
						GlobalParameters.Global.BATCH_ID,
						NestedGroupCentroidAssignment.class,
						null));
		@SuppressWarnings("unchecked")
		final DistanceFn<T> distanceFunction = wrapper.getInstance(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				NestedGroupCentroidAssignment.class,
				DistanceFn.class,
				FeatureCentroidDistanceFn.class);
		this.associationdFunction.setDistanceFunction(distanceFunction);
		centroidManager = new CentroidManagerGeoWave<T>(
				wrapper);

	}

	public List<AnalyticItemWrapper<T>> getCentroidsForGroup(
			final String groupID )
			throws IOException {
		return centroidManager.getCentroidsForGroup(groupID);
	}

	/**
	 * Get the associated group id from the current zoom level
	 * 
	 */
	public String getGroupForLevel(
			final AnalyticItemWrapper<T> item )
			throws IOException {
		final GroupHolder group = new GroupHolder();
		group.setGroupID(item.getGroupID());
		int currentLevel = item.getZoomLevel();
		while (endZoomLevel != currentLevel) {
			final List<AnalyticItemWrapper<T>> centroids = centroidManager.getCentroidsForGroup(
					parentBatchID,
					group.getGroupID());
			if (centroids.size() == 0) {
				throw new IOException(
						"Cannot find group " + group.getGroupID());
			}
			associationdFunction.compute(
					item,
					centroids,
					new AssociationNotification<T>() {
						@Override
						public void notify(
								final CentroidPairing<T> pairing ) {
							group.setGroupID(pairing.getCentroid().getID());
						}
					});
			currentLevel = centroids.get(
					0).getZoomLevel() + 1;
		}
		return group.getGroupID();
	}

	public double findCentroidForLevel(
			final AnalyticItemWrapper<T> item,
			final AssociationNotification<T> associationNotification )
			throws IOException {
		final GroupHolder group = new GroupHolder();
		group.setGroupID(item.getGroupID());
		double currentDistance = Double.NaN;
		int currentLevel = item.getZoomLevel();
		boolean atEndLevel = false;
		// force one time through
		while (!atEndLevel) {
			// save status as 'final' to use in the following closure.
			final boolean reachedEndLevel = currentLevel == endZoomLevel;
			atEndLevel = reachedEndLevel;

			// only use the parent batch ID for upper levels, otherwise use the
			// current batch ID.
			final List<AnalyticItemWrapper<T>> centroids = (currentLevel == endZoomLevel) ? centroidManager.getCentroidsForGroup(group.getGroupID()) : centroidManager.getCentroidsForGroup(
					parentBatchID,
					group.getGroupID());
			if (centroids.size() == 0) {
				throw new IOException(
						"Cannot find group " + group.getGroupID());
			}

			currentDistance = associationdFunction.compute(
					item,
					centroids,
					new AssociationNotification<T>() {
						@Override
						public void notify(
								final CentroidPairing<T> pairing ) {
							group.setGroupID(pairing.getCentroid().getID());
							if (reachedEndLevel) {
								associationNotification.notify(pairing);
							}
						}
					});
			// update for next loop
			currentLevel = centroids.get(
					0).getZoomLevel() + 1;

		}
		return currentDistance;
	}

	public static void setParameters(
			final Configuration config,
			final PropertyManagement runTimeProperties ) {
		CentroidManagerGeoWave.setParameters(
				config,
				runTimeProperties);

		RunnerUtils.setParameter(
				config,
				NestedGroupCentroidAssignment.class,
				runTimeProperties,
				new ParameterEnum[] {
					CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
					CentroidParameters.Centroid.ZOOM_LEVEL,
					GlobalParameters.Global.BATCH_ID,
					GlobalParameters.Global.PARENT_BATCH_ID
				});
	}

	private class GroupHolder
	{
		private String groupID;

		public String getGroupID() {
			return groupID;
		}

		public void setGroupID(
				final String groupID ) {
			this.groupID = groupID;
		}

	}
}
