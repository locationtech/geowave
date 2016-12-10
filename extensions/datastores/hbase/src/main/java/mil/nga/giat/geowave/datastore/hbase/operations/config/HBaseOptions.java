package mil.nga.giat.geowave.datastore.hbase.operations.config;

import org.apache.hadoop.hbase.HConstants;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.store.BaseDataStoreOptions;

public class HBaseOptions extends
		BaseDataStoreOptions
{
	public static final String COPROCESSOR_JAR_KEY = "coprocessorJar";

	@Parameter(names = "--bigtable")
	protected boolean bigtable = false;

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

	public boolean isBigtable() {
		return bigtable;
	}

	public void setBigtable(
			final boolean bigtable ) {
		this.bigtable = bigtable;
	}

	public int getScanCacheSize() {
		return scanCacheSize;
	}

	public void setScanCacheSize(
			final int scanCacheSize ) {
		this.scanCacheSize = scanCacheSize;
	}

	public boolean isEnableCustomFilters() {
		return enableCustomFilters && !bigtable;
	}

	public void setEnableCustomFilters(
			final boolean enableCustomFilters ) {
		this.enableCustomFilters = enableCustomFilters;
	}

	public boolean isEnableCoprocessors() {
		return enableCoprocessors && !bigtable;
	}

	public void setEnableCoprocessors(
			final boolean enableCoprocessors ) {
		this.enableCoprocessors = enableCoprocessors;
	}

	public boolean isVerifyCoprocessors() {
		return verifyCoprocessors && !bigtable;
	}

	public void setVerifyCoprocessors(
			final boolean verifyCoprocessors ) {
		this.verifyCoprocessors = verifyCoprocessors;
	}

	public String getCoprocessorJar() {
		return coprocessorJar;
	}

	public void setCoprocessorJar(
			final String coprocessorJar ) {
		this.coprocessorJar = coprocessorJar;
	}
}
