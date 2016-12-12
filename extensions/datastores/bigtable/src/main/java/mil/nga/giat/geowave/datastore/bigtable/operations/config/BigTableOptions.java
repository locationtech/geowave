package mil.nga.giat.geowave.datastore.bigtable.operations.config;

import org.apache.hadoop.hbase.HConstants;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.store.StoreFactoryFamilySpi;
import mil.nga.giat.geowave.core.store.StoreFactoryOptions;
import mil.nga.giat.geowave.datastore.hbase.HBaseStoreFactoryFamily;
import mil.nga.giat.geowave.datastore.hbase.operations.config.HBaseOptions;

public class BigTableOptions extends
		StoreFactoryOptions
{

	public static final String DEFAULT_PROJECT_ID = "geowave-bigtable-project-id";
	public static final String DEFAULT_INSTANCE_ID = "geowave-bigtable-instance-id";

	@Parameter(names = "--scanCacheSize")
	protected int scanCacheSize = HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING;
	@Parameter(names = "--projectId")
	protected String projectId = DEFAULT_PROJECT_ID;
	@Parameter(names = "--instanceId")
	protected String instanceId = DEFAULT_INSTANCE_ID;
	private final HBaseOptions internalHBaseOptions = new InternalHBaseOptions();

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new HBaseStoreFactoryFamily();
	}

	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(
			final String projectId ) {
		this.projectId = projectId;
	}

	public String getInstanceId() {
		return instanceId;
	}

	public void setInstanceId(
			final String instanceId ) {
		this.instanceId = instanceId;
	}

	public HBaseOptions getHBaseOptions() {
		return internalHBaseOptions;
	}

	private class InternalHBaseOptions extends
			HBaseOptions
	{

		@Override
		public boolean isServerSideDisabled() {
			return true;
		}

		@Override
		public int getScanCacheSize() {
			return BigTableOptions.this.scanCacheSize;
		}

		@Override
		public boolean isEnableCustomFilters() {
			return false;
		}

		@Override
		public boolean isEnableCoprocessors() {
			return false;
		}

		@Override
		public boolean isVerifyCoprocessors() {
			return false;
		}

	}
}
