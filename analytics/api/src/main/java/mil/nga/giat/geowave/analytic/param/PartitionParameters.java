package mil.nga.giat.geowave.analytic.param;

import java.util.Arrays;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.partitioner.Partitioner;

import org.apache.commons.cli.Option;

public class PartitionParameters
{
	public enum Partition
			implements
			ParameterEnum {
		PARTITION_DISTANCE(
				Double.class),
		PARTITION_PRECISION(
				Double.class),
		MAX_MEMBER_SELECTION(
				Integer.class),
		PARTITIONER_CLASS(
				Partitioner.class);

		private final Class<?> baseClass;

		Partition(
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
			final Set<Option> options,
			final Partition[] params ) {
		if (contains(
				params,
				Partition.PARTITIONER_CLASS)) {
			options.add(PropertyManagement.newOption(
					Partition.PARTITIONER_CLASS,
					"pc",
					"Index Identifier for Centroids",
					true));
		}
		if (contains(
				params,
				Partition.MAX_MEMBER_SELECTION)) {
			options.add(PropertyManagement.newOption(
					Partition.MAX_MEMBER_SELECTION,
					"pms",
					"Maximum number of members selected from a partition",
					true));
		}
		if (contains(
				params,
				Partition.PARTITION_DISTANCE)) {
			options.add(PropertyManagement.newOption(
					Partition.PARTITION_DISTANCE,
					"pd",
					"Partition Distance",
					true));
		}
	}

	private static boolean contains(
			final Partition[] params,
			final Partition option ) {
		return Arrays.asList(
				params).contains(
				option);
	}
}
