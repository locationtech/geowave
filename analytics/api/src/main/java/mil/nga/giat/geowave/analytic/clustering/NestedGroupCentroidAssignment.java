/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.analytic.clustering;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.ScopedJobConfiguration;
import mil.nga.giat.geowave.analytic.distance.DistanceFn;
import mil.nga.giat.geowave.analytic.distance.FeatureCentroidDistanceFn;
import mil.nga.giat.geowave.analytic.kmeans.AssociationNotification;
import mil.nga.giat.geowave.analytic.kmeans.CentroidAssociationFn;
import mil.nga.giat.geowave.analytic.param.CentroidParameters;
import mil.nga.giat.geowave.analytic.param.CommonParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;

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

	public NestedGroupCentroidAssignment(
			final JobContext context,
			final Class<?> scope,
			final Logger logger )
			throws InstantiationException,
			IllegalAccessException,
			IOException {
		final ScopedJobConfiguration config = new ScopedJobConfiguration(
				context.getConfiguration(),
				scope,
				logger);
		endZoomLevel = config.getInt(
				CentroidParameters.Centroid.ZOOM_LEVEL,
				1);
		parentBatchID = config.getString(
				GlobalParameters.Global.PARENT_BATCH_ID,
				config.getString(
						GlobalParameters.Global.BATCH_ID,
						null));
		@SuppressWarnings("unchecked")
		final DistanceFn<T> distanceFunction = config.getInstance(
				CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
				DistanceFn.class,
				FeatureCentroidDistanceFn.class);
		this.associationdFunction.setDistanceFunction(distanceFunction);
		centroidManager = new CentroidManagerGeoWave<T>(
				context,
				scope);

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
			final Class<?> scope,
			final int zoomLevel ) {
		CentroidParameters.Centroid.ZOOM_LEVEL.getHelper().setValue(
				config,
				scope,
				zoomLevel);
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
			final Class<?> scope,
			final String parentID ) {
		GlobalParameters.Global.PARENT_BATCH_ID.getHelper().setValue(
				config,
				scope,
				parentID);
	}

	public static Collection<ParameterEnum<?>> getParameters() {
		final Set<ParameterEnum<?>> params = new HashSet<ParameterEnum<?>>();
		params.addAll(CentroidManagerGeoWave.getParameters());

		params.addAll(Arrays.asList(new ParameterEnum<?>[] {
			CentroidParameters.Centroid.ZOOM_LEVEL,
			GlobalParameters.Global.PARENT_BATCH_ID,
			CommonParameters.Common.DISTANCE_FUNCTION_CLASS
		}));
		return params;
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
			final List<AnalyticItemWrapper<T>> centroids = (currentLevel == endZoomLevel) ? centroidManager
					.getCentroidsForGroup(group.getGroupID()) : centroidManager.getCentroidsForGroup(
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
			final Class<?> scope,
			final PropertyManagement runTimeProperties ) {
		CentroidManagerGeoWave.setParameters(
				config,
				scope,
				runTimeProperties);

		runTimeProperties.setConfig(
				new ParameterEnum[] {
					CommonParameters.Common.DISTANCE_FUNCTION_CLASS,
					CentroidParameters.Centroid.ZOOM_LEVEL,
					GlobalParameters.Global.BATCH_ID,
					GlobalParameters.Global.PARENT_BATCH_ID
				},
				config,
				scope);
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
