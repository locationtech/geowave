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
package mil.nga.giat.geowave.analytic.mapreduce.kmeans.runner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import mil.nga.giat.geowave.analytic.AnalyticItemWrapper;
import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager;
import mil.nga.giat.geowave.analytic.clustering.CentroidManager.CentroidProcessingFn;
import mil.nga.giat.geowave.analytic.clustering.CentroidManagerGeoWave;
import mil.nga.giat.geowave.analytic.mapreduce.MapReduceJobRunner;
import mil.nga.giat.geowave.core.index.FloatCompareUtils;

import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Remove weak centers. Looking for a large gaps of distances AND retain a
 * minimum set.
 * 
 */
public class StripWeakCentroidsRunner<T> implements
		MapReduceJobRunner
{

	protected static final Logger LOGGER = LoggerFactory.getLogger(StripWeakCentroidsRunner.class);

	private int minimum = 1;
	private int maximum = 1000;
	private int currentCentroidCount = 0;
	private BreakStrategy<T> breakStrategy = new TailMaxBreakStrategy<T>();

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
		return new CentroidManagerGeoWave<T>(
				runTimeProperties);
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

				LOGGER.info(
						"Deleting {} out of {}",
						toDelete.length,
						centroids.size());

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
				catch (final IOException e) {
					LOGGER.warn(
							"Unable to delete the centriod mamager",
							e);
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
			int position = centroids.size();
			int count = 0;
			final StandardDeviation st = new StandardDeviation();
			double total = 0.0;
			double prior = Double.NaN;

			for (final AnalyticItemWrapper<T> centroid : centroids) {
				if (!Double.isNaN(prior)) {
					final double chg = Math.abs(prior - centroid.getAssociationCount());
					st.increment(chg);
					total += chg;
				}
				prior = centroid.getAssociationCount();
			}

			double max = getInitialMaximum(
					st,
					total);
			prior = Double.NaN;
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
					if (FloatCompareUtils.checkDoublesEqual(
							Math.max(
									max,
									chg),
							chg)) {
						position = count;
						max = chg;
					}
				}
				prior = centroid.getAssociationCount();

				count++;
			}

			return position;
		}

		protected double getInitialMaximum(
				final StandardDeviation stats,
				final double total ) {
			return 0.0;
		}
	}

	private static class ChangeFromLast implements
			Comparable<ChangeFromLast>
	{
		int position;
		double chg;

		public ChangeFromLast(
				final int position,
				final double chg ) {
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
				final ChangeFromLast arg0 ) {
			return new Double(
					(arg0).chg).compareTo(chg);
		}

		@Override
		public boolean equals(
				final Object obj ) {
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

			final List<ChangeFromLast> changes = new ArrayList<ChangeFromLast>(
					centroids.size());

			final StandardDeviation st = new StandardDeviation();
			double prior = Double.NaN;
			double total = 0;
			int count = 0;

			// look for largest change
			for (final AnalyticItemWrapper<T> centroid : centroids) {
				final double chgValue = (!Double.isNaN(prior)) ? Math.abs(prior - centroid.getAssociationCount()) : 0.0;

				changes.add(new ChangeFromLast(
						count,
						chgValue));

				prior = centroid.getAssociationCount();
				count++;
			}
			Collections.sort(changes);

			int position = centroids.size();
			count = 0;
			ChangeFromLast priorChg = null;

			for (final ChangeFromLast changeFromLast : changes) {
				if (priorChg != null) {
					final double chgOfChg = Math.abs(priorChg.chg - changeFromLast.chg);
					total += chgOfChg;
					st.increment(chgOfChg);
				}
				priorChg = changeFromLast;
				count++;
			}

			double max = getInitialMaximum(
					st,
					total);

			position = changes.get(0).position;
			if (changes.get(0).chg < max) {
				return centroids.size();
			}
			priorChg = null;
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
			}

			return position;
		}

		protected double getInitialMaximum(
				final StandardDeviation stats,
				final double total ) {
			return 0.0;
		}
	}

	public static class TailMaxBreakStrategy<T> extends
			MaxChangeBreakStrategy<T> implements
			BreakStrategy<T>
	{
		@Override
		protected double getInitialMaximum(
				final StandardDeviation stats,
				final double total ) {
			return (total / stats.getN()) + stats.getResult();
		}
	}

	public static class TailStableChangeBreakStrategy<T> extends
			StableChangeBreakStrategy<T> implements
			BreakStrategy<T>
	{
		@Override
		protected double getInitialMaximum(
				final StandardDeviation stats,
				final double total ) {
			return (total / stats.getN()) + stats.getResult();
		}
	}

	public interface BreakStrategy<T>
	{
		public int getBreakPoint(
				List<AnalyticItemWrapper<T>> centroids );

	}

}
