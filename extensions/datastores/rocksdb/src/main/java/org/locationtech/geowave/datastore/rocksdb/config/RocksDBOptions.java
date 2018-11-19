package org.locationtech.geowave.datastore.rocksdb.config;

import org.locationtech.geowave.core.store.BaseDataStoreOptions;
import org.locationtech.geowave.core.store.DataStoreOptions;
import org.locationtech.geowave.core.store.StoreFactoryFamilySpi;
import org.locationtech.geowave.core.store.StoreFactoryOptions;
import org.locationtech.geowave.datastore.rocksdb.RocksDBStoreFactoryFamily;
import org.locationtech.geowave.datastore.rocksdb.util.RocksDBUtils;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;

public class RocksDBOptions extends
		StoreFactoryOptions
{
	@Parameter(names = "--directory", description = "The directory to read/write to.  Defaults to \"rocksdb\" in the working directory.")
	private String directory = "rocksdb";
	@ParametersDelegate
	protected BaseDataStoreOptions baseOptions = new BaseDataStoreOptions() {
		@Override
		public boolean isServerSideLibraryEnabled() {
			return false;
		}

		@Override
		protected int defaultMaxRangeDecomposition() {
			return RocksDBUtils.ROCKSDB_DEFAULT_MAX_RANGE_DECOMPOSITION;
		}

		@Override
		protected int defaultAggregationMaxRangeDecomposition() {
			return RocksDBUtils.ROCKSDB_DEFAULT_AGGREGATION_MAX_RANGE_DECOMPOSITION;
		}
	};

	public RocksDBOptions() {
		super();
	}

	public RocksDBOptions(
			final String geowaveNamespace ) {
		super(
				geowaveNamespace);
	}

	public void setDirectory(
			final String directory ) {
		this.directory = directory;
	}

	public String getDirectory() {
		return directory;
	}

	@Override
	public StoreFactoryFamilySpi getStoreFactory() {
		return new RocksDBStoreFactoryFamily();
	}

	@Override
	public DataStoreOptions getStoreOptions() {
		return baseOptions;
	}
}
