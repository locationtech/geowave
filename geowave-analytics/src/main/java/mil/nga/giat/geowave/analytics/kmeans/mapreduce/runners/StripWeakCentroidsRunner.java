package mil.nga.giat.geowave.analytics.kmeans.mapreduce.runners;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.analytics.clustering.CentroidManager;
import mil.nga.giat.geowave.analytics.clustering.CentroidManager.CentroidProcessingFn;
import mil.nga.giat.geowave.analytics.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytics.tools.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytics.tools.PropertyManagement;
import mil.nga.giat.geowave.analytics.tools.mapreduce.MapReduceJobRunner;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;

/**
 * Remove weak centers. Looking for a large gaps of distances AND retain a
 * minimum set.
 * 
 */
public class StripWeakCentroidsRunner<T> implements
		MapReduceJobRunner
{

	protected static final Logger LOGGER = Logger.getLogger(StripWeakCentroidsRunner.class);

	private int minimum = 1;
	private int maximum = 1000;
	private int currentCentroidCount = 0;
	private BreakStrategy<T> breakStrategy = new StableChangeBreakStrategy<T>();

	public StripWeakCentroidsRunner() {}

	public void setBreakStrategy(
			final BreakStrategy<T> breakStrategy ) {
		this.breakStrategy = breakStrategy;
	}

	/**
	 * 
	 * @param minimum
	 *            new minimum number of centroids to retain, regardless of weak
	 *            center;
	 */
	public void setRange(
			final int minimum,
			final int maximum ) {
		this.minimum = minimum;
		this.maximum = maximum;
	}

	/**
	 * Available only after execution.
	 * 
	 * @return The count of current centroids after execution
	 */
	public int getCurrentCentroidCount() {
		return currentCentroidCount;
	}

	protected CentroidManager<T> constructCentroidManager(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws IOException {
		try {
			return new CentroidManagerGeoWave<T>(
					runTimeProperties);
		}
		catch (AccumuloException | AccumuloSecurityException e) {
			throw new IOException(
					e);
		}
	}

	@Override
	public int run(
			final Configuration config,
			final PropertyManagement runTimeProperties )
			throws Exception {

		currentCentroidCount = 0;

		final CentroidManager<T> centroidManager = constructCentroidManager(
				config,
				runTimeProperties);

		return centroidManager.processForAllGroups(new CentroidProcessingFn<T>() {
			@Override
			public int processGroup(
					final String groupID,
					final List<AnalyticItemWrapper<T>> centroids ) {

				if (centroids.size() <= minimum) {
					currentCentroidCount = centroids.size();
					return 0;
				}

				Collections.sort(
						centroids,
						new Comparator<AnalyticItemWrapper<T>>() {

							@Override
							public int compare(
									final AnalyticItemWrapper<T> arg0,
									final AnalyticItemWrapper<T> arg1 ) {
								// be careful of overflow
								// also, descending
								return (arg1.getAssociationCount() - arg0.getAssociationCount()) < 0 ? -1 : 1;
							}
						});
				int position = breakStrategy.getBreakPoint(centroids);

				// make sure we do not delete too many
				// trim bottom third
				position = Math.min(
						Math.max(
								minimum,
								position),
						maximum);

				final String toDelete[] = new String[centroids.size() - position];

				LOGGER.info("Deleteing " + toDelete.length + " out of " + centroids.size());

				int count = 0;
				final Iterator<AnalyticItemWrapper<T>> it = centroids.iterator();
				while (it.hasNext()) {
					final AnalyticItemWrapper<T> centroid = it.next();
					if (count++ >= position) {
						toDelete[count - position - 1] = centroid.getID();
					}
				}
				try {
					centroidManager.delete(toDelete);
				}
				catch (final Exception e) {
					return -1;
				}

				currentCentroidCount += position;

				return 0;
			}
		});

	}

	public static class MaxChangeBreakStrategy<T> implements
			BreakStrategy<T>
	{
		@Override
		public int getBreakPoint(
				final List<AnalyticItemWrapper<T>> centroids ) {
			int position = 0;
			int count = 0;
			double prior = Double.NaN;
			double max = 0.0;

			// look for largest change
			for (final AnalyticItemWrapper<T> centroid : centroids) {
				if (centroid.getAssociationCount() <= 1) {
					if (position == 0) {
						position = count;
					}
					break;
				}
				if (!Double.isNaN(prior)) {
					final double chg = Math.abs(prior - centroid.getAssociationCount());
					if (Math.max(
							max,
							chg) == chg) {
						position = count;
						max = chg;
					}
				}
				prior = centroid.getAssociationCount();

				count++;
			}

			return position;
		}
	}

	private static class ChangeFromLast implements
			Comparable<ChangeFromLast>
	{
		int position;
		double chg;

		public ChangeFromLast(
				int position,
				double chg ) {
			super();
			this.position = position;
			this.chg = chg;
		}

		@Override
		public String toString() {
			return "ChangeFromLast [position=" + position + ", chg=" + chg + "]";
		}

		@Override
		public int compareTo(
				ChangeFromLast arg0 ) {
			return new Double(
					(arg0).chg).compareTo(chg);
		}

		@Override
		public boolean equals(
				Object obj ) {
			if (obj == null) {
				return false;
			}
			if (!(obj instanceof ChangeFromLast)) {
				return false;
			}
			return this.compareTo((ChangeFromLast) obj) == 0;
		}

		@Override
		public int hashCode() {
			return Double.valueOf(
					chg).hashCode();
		}

	}

	public static class StableChangeBreakStrategy<T> implements
			BreakStrategy<T>
	{
		@Override
		public int getBreakPoint(
				final List<AnalyticItemWrapper<T>> centroids ) {

			List<ChangeFromLast> changes = new ArrayList<ChangeFromLast>(
					centroids.size());

			double prior = Double.NaN;
			int count = 0;

			// look for largest change
			for (final AnalyticItemWrapper<T> centroid : centroids) {
				if (!Double.isNaN(prior)) {
					changes.add(new ChangeFromLast(
							count,
							Math.abs(prior - centroid.getAssociationCount())));

				}
				else {
					changes.add(new ChangeFromLast(
							count,
							0.0));
				}
				prior = centroid.getAssociationCount();
				count++;
			}
			Collections.sort(changes);

			int position = 0;
			count = 0;
			ChangeFromLast priorChg = null;
			double max = 0.0;

			position = changes.get(0).position;
			// look for largest change
			for (final ChangeFromLast changeFromLast : changes) {
				if (priorChg != null) {
					final double chgOfChg = Math.abs(priorChg.chg - changeFromLast.chg);
					if (chgOfChg > max) {
						position = Math.max(
								position,
								changeFromLast.position);
						max = chgOfChg;
					}
				}
				priorChg = changeFromLast;

				count++;
			}

			return position;
		}
	}

	public interface BreakStrategy<T>
	{
		public int getBreakPoint(
				List<AnalyticItemWrapper<T>> centroids );

	}

}