package mil.nga.giat.geowave.analytic.clustering;

import java.io.IOException;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.AnalyticItemWrapperFactory;
import mil.nga.giat.geowave.analytic.ConfigurationWrapper;
import mil.nga.giat.geowave.analytic.kmeans.AssociationNotification;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

/**
 * Determine the group ID for an item dynamically.
 * 
 * 
 * 
 * @param <T>
 */
public class CentroidItemWrapperFactory<T> implements
		AnalyticItemWrapperFactory<T>
{

	final static Logger LOGGER = LoggerFactory.getLogger(CentroidItemWrapperFactory.class);
	private AnalyticItemWrapperFactory<T> itemFactory;
	private NestedGroupCentroidAssignment<T> nestedGroupCentroidAssignment;

	@Override
	public AnalyticItemWrapper<T> create(
			final T item ) {
		return new CentroidItemWrapper(
				item);
	}

	@Override
	public void initialize(
			final ConfigurationWrapper context )
			throws IOException {
		try {
			nestedGroupCentroidAssignment = new NestedGroupCentroidAssignment<T>(
					context);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			throw new IOException(
					"Failed to connect to Accumulo",
					e);
		}
		catch (InstantiationException | IllegalAccessException e) {
			throw new IOException(
					"Failed to instantiate",
					e);
		}

		itemFactory.initialize(context);
	}

	public AnalyticItemWrapperFactory<T> getItemFactory() {
		return itemFactory;
	}

	public void setItemFactory(
			final AnalyticItemWrapperFactory<T> itemFactory ) {
		this.itemFactory = itemFactory;
	}

	public class CentroidItemWrapper implements
			AnalyticItemWrapper<T>
	{
		final AnalyticItemWrapper<T> wrappedItem;
		AnalyticItemWrapper<T> centroidItem;

		public CentroidItemWrapper(
				final T item ) {
			wrappedItem = itemFactory.create(item);
			try {
				nestedGroupCentroidAssignment.findCentroidForLevel(
						wrappedItem,
						new AssociationNotification<T>() {
							@Override
							public void notify(
									final CentroidPairing<T> pairing ) {
								centroidItem = pairing.getCentroid();
							}
						});
			}
			catch (final IOException e) {
				LOGGER.error("Cannot resolve paired centroid for " + wrappedItem.getID());
				centroidItem = wrappedItem;
			}
		}

		@Override
		public String getID() {
			return centroidItem.getID();
		}

		@Override
		public T getWrappedItem() {
			return centroidItem.getWrappedItem();
		}

		@Override
		public long getAssociationCount() {
			return centroidItem.getAssociationCount();
		}

		@Override
		public int getIterationID() {
			return centroidItem.getIterationID();
		}

		// this is not a mistake...the group id is the centroid itself
		@Override
		public String getGroupID() {
			return centroidItem.getID();
		}

		@Override
		public void setGroupID(
				final String groupID ) {

		}

		@Override
		public void resetAssociatonCount() {

		}

		@Override
		public void incrementAssociationCount(
				final long increment ) {

		}

		@Override
		public double getCost() {
			return centroidItem.getCost();
		}

		@Override
		public void setCost(
				final double cost ) {}

		@Override
		public String getName() {
			return centroidItem.getName();
		}

		@Override
		public String[] getExtraDimensions() {
			return new String[0];
		}

		@Override
		public double[] getDimensionValues() {
			return new double[0];
		}

		@Override
		public Geometry getGeometry() {
			return centroidItem.getGeometry();
		}

		@Override
		public void setZoomLevel(
				final int level ) {

		}

		@Override
		public int getZoomLevel() {
			return centroidItem.getZoomLevel();
		}

		@Override
		public void setBatchID(
				final String batchID ) {}

		@Override
		public String getBatchID() {
			return centroidItem.getBatchID();
		}
	}

	/*
	 * @see
	 * mil.nga.giat.geowave.analytics.tools.CentroidFactory#createNextCentroid
	 * (java.lang.Object, com.vividsolutions.jts.geom.Coordinate,
	 * java.lang.String[], double[])
	 */

	@Override
	public AnalyticItemWrapper<T> createNextItem(
			final T feature,
			final String groupID,
			final Coordinate coordinate,
			final String[] extraNames,
			final double[] extraValues ) {
		return this.itemFactory.createNextItem(
				feature,
				groupID,
				coordinate,
				extraNames,
				extraValues);
	}

}
