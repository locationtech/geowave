package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.commons.cli.Option;

public class ClusteringParameters
{

	public enum Clustering
			implements
			ParameterEnum {
		MAX_REDUCER_COUNT(
				Integer.class),
		RETAIN_GROUP_ASSIGNMENTS(
				Boolean.class),
		MINIMUM_SIZE(
				Integer.class),
		MAX_ITERATIONS(
				Integer.class),
		CONVERGANCE_TOLERANCE(
				Double.class),
		DISTANCE_THRESHOLDS(
				String.class),
		GEOMETRIC_DISTANCE_UNIT(
				String.class),
		ZOOM_LEVELS(
				Integer.class);

		private final Class<?> baseClass;

		Clustering(
				final Class<?> baseClass ) {
			this.baseClass = baseClass;
		}

		@Override
		public Class<?> getBaseClass() {
			return baseClass;
		}

		@Override
		public Enum<?> self() {
			return this;
		}
	}

	public static final void fillOptions(
			Set<Option> options,
			Clustering[] params ) {
		if (contains(
				params,
				Clustering.ZOOM_LEVELS)) {
			options.add(PropertyManagement.newOption(
					Clustering.ZOOM_LEVELS,
					"zl",
					"Number of Zoom Levels to Process",
					true));
		}
		if (contains(
				params,
				Clustering.MINIMUM_SIZE)) {
			options.add(PropertyManagement.newOption(
					Clustering.MINIMUM_SIZE,
					"cms",
					"Minimum Cluster Size",
					true));
		}
		if (contains(
				params,
				Clustering.GEOMETRIC_DISTANCE_UNIT)) {
			options.add(PropertyManagement.newOption(
					Clustering.GEOMETRIC_DISTANCE_UNIT,
					"du",
					"Geometric distance unit (m=meters,km=kilometers, see symbols for javax.units.BaseUnit)",
					true));
		}
		if (contains(
				params,
				Clustering.DISTANCE_THRESHOLDS)) {
			options.add(PropertyManagement.newOption(
					Clustering.DISTANCE_THRESHOLDS,
					"dt",
					"Comma separated list of distance thresholds, per dimension",
					true));
		}
		if (contains(
				params,
				Clustering.RETAIN_GROUP_ASSIGNMENTS)) {
			options.add(PropertyManagement.newOption(
					Clustering.RETAIN_GROUP_ASSIGNMENTS,
					"ga",
					"Retain Group assignments during execution",
					false));
		}
		if (contains(
				params,
				Clustering.CONVERGANCE_TOLERANCE)) {
			options.add(PropertyManagement.newOption(
					Clustering.CONVERGANCE_TOLERANCE,
					"cct",
					"Convergence Tolerance",
					true));
		}
		if (contains(
				params,
				Clustering.MAX_REDUCER_COUNT)) {
			options.add(PropertyManagement.newOption(
					Clustering.MAX_REDUCER_COUNT,
					"crc",
					"Maximum Clustering Reducer Count",
					true));
		}
		if (contains(
				params,
				Clustering.MAX_ITERATIONS)) {
			options.add(PropertyManagement.newOption(
					Clustering.MAX_ITERATIONS,
					"cmi",
					"Maximum number of iterations when finding optimal clusters",
					true));
		}

	}

	private static boolean contains(
			Clustering[] params,
			Clustering option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
