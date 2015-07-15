package mil.nga.giat.geowave.analytic.param;

import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;

import org.apache.commons.cli.Option;

public class MapReduceParameters
{

	public enum MRConfig
			implements
			ParameterEnum {
		CONFIG_FILE(
				String.class),
		HDFS_HOST_PORT(
				String.class),
		HDFS_BASE_DIR(
				String.class),
		YARN_RESOURCE_MANAGER(
				String.class),
		JOBTRACKER_HOST_PORT(
				String.class);

		private final Class<?> baseClass;

		MRConfig(
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
			final Set<Option> options ) {

		options.add(PropertyManagement.newOption(
				MRConfig.CONFIG_FILE,
				"conf",
				"MapReduce Configuration",
				true));

		options.add(PropertyManagement.newOption(
				MRConfig.HDFS_HOST_PORT,
				"hdfs",
				"HDFS hostname and port in the format hostname:port",
				true));

		options.add(PropertyManagement.newOption(
				MRConfig.HDFS_BASE_DIR,
				"hdfsbase",
				"Fully qualified path to the base directory in hdfs",
				true));

		options.add(PropertyManagement.newOption(
				MRConfig.JOBTRACKER_HOST_PORT,
				"jobtracker",
				"Hadoop job tracker hostname and port in the format hostname:port",
				true));

		options.add(PropertyManagement.newOption(
				MRConfig.YARN_RESOURCE_MANAGER,
				"resourceman",
				"Yarn resource manager hostname and port in the format hostname:port",
				true));

	}

}
