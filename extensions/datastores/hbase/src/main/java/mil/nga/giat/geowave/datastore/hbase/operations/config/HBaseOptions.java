package mil.nga.giat.geowave.datastore.hbase.operations.config;

import org.apache.hadoop.hbase.HConstants;

import mil.nga.giat.geowave.core.store.DataStoreOptions;

import com.beust.jcommander.Parameter;

public class HBaseOptions implements
		DataStoreOptions
{
	public static final String COPROCESSOR_JAR_KEY = "coprocessorJar";

	@Parameter(names = "--persistAdapter", hidden = true, arity = 1)
	protected boolean persistAdapter = true;

	@Parameter(names = "--persistIndex", hidden = true, arity = 1)
	protected boolean persistIndex = true;

	@Parameter(names = "--persistDataStatistics", hidden = true, arity = 1)
	protected boolean persistDataStatistics = true;

	@Parameter(names = "--createTable", hidden = true, arity = 1)
	protected boolean createTable = true;

	@Parameter(names = "--useLocalityGroups", hidden = true, arity = 1)
	protected boolean useLocalityGroups = true;

	@Parameter(names = "--useAltIndex", hidden = true, arity = 1)
	protected boolean useAltIndex = false;

	@Parameter(names = "--enableBlockCache", hidden = true, arity = 1)
	protected boolean enableBlockCache = true;

	@Parameter(names = "--scanCacheSize")
	protected int scanCacheSize = HConstants.DEFAULT_HBASE_CLIENT_SCANNER_CACHING;

	@Parameter(names = "--enableCustomFilters")
	protected boolean enableCustomFilters = false;

	@Parameter(names = "--enableCoprocessors")
	protected boolean enableCoprocessors = false;

	@Parameter(names = "--verifyCoprocessors")
	protected boolean verifyCoprocessors = false;

	@Parameter(names = {
		"--" + COPROCESSOR_JAR_KEY
	}, description = "Path (HDFS URL) to the jar containing coprocessor classes")
	private String coprocessorJar;

	public boolean isPersistDataStatistics() {
		return persistDataStatistics;
	}

	public void setPersistDataStatistics(
			final boolean persistDataStatistics ) {
		this.persistDataStatistics = persistDataStatistics;
	}

	public boolean isPersistAdapter() {
		return persistAdapter;
	}

	public void setPersistAdapter(
			final boolean persistAdapter ) {
		this.persistAdapter = persistAdapter;
	}

	public boolean isPersistIndex() {
		return persistIndex;
	}

	public void setPersistIndex(
			final boolean persistIndex ) {
		this.persistIndex = persistIndex;
	}

	public boolean isCreateTable() {
		return createTable;
	}

	public void setCreateTable(
			final boolean createTable ) {
		this.createTable = createTable;
	}

	public boolean isUseLocalityGroups() {
		return useLocalityGroups;
	}

	public void setUseLocalityGroups(
			final boolean useLocalityGroups ) {
		this.useLocalityGroups = useLocalityGroups;
	}

	public boolean isUseAltIndex() {
		return useAltIndex;
	}

	public void setUseAltIndex(
			final boolean useAltIndex ) {
		this.useAltIndex = useAltIndex;
	}

	public boolean isEnableBlockCache() {
		return enableBlockCache;
	}

	public void setEnableBlockCache(
			boolean enableBlockCache ) {
		this.enableBlockCache = enableBlockCache;
	}

	public int getScanCacheSize() {
		return scanCacheSize;
	}

	public void setScanCacheSize(
			int scanCacheSize ) {
		this.scanCacheSize = scanCacheSize;
	}

	public boolean isEnableCustomFilters() {
		return enableCustomFilters;
	}

	public void setEnableCustomFilters(
			boolean enableCustomFilters ) {
		this.enableCustomFilters = enableCustomFilters;
	}

	public boolean isEnableCoprocessors() {
		return enableCoprocessors;
	}

	public void setEnableCoprocessors(
			boolean enableCoprocessors ) {
		this.enableCoprocessors = enableCoprocessors;
	}

	public boolean isVerifyCoprocessors() {
		return verifyCoprocessors;
	}

	public void setVerifyCoprocessors(
			boolean verifyCoprocessors ) {
		this.verifyCoprocessors = verifyCoprocessors;
	}

	public String getCoprocessorJar() {
		return coprocessorJar;
	}

	public void setCoprocessorJar(
			String coprocessorJar ) {
		this.coprocessorJar = coprocessorJar;
	}
}
