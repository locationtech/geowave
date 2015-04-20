package mil.nga.giat.geowave.adapter.vector.util;

import java.util.ArrayList;
import java.util.List;

import mil.nga.giat.geowave.adapter.vector.FeatureCollectionDataAdapter;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.store.adapter.NativeFieldHandler;
import mil.nga.giat.geowave.core.store.adapter.PersistentIndexFieldHandler;
import mil.nga.giat.geowave.core.store.data.field.FieldVisibilityHandler;
import mil.nga.giat.geowave.core.store.index.CommonIndexValue;

import org.geotools.feature.DefaultFeatureCollection;
import org.opengis.feature.simple.SimpleFeatureType;

public class RedistributeConfig
{
	// BasicAccumuloOperations config
	private String zookeeperUrl;
	private String instanceName;
	private String userName;
	private String password;
	private String tableNamespace;

	// Index config
	private IndexType indexType;
	private int tier;

	// FeatureCollectionDataAdapter config
	private SimpleFeatureType featureType;
	private int featuresPerEntry;
	private List<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> customIndexHandlers;
	private FieldVisibilityHandler<DefaultFeatureCollection, Object> fieldVisiblityHandler;

	// Redistribute config
	private int numThreads;

	public RedistributeConfig() {
		zookeeperUrl = null;
		instanceName = null;
		userName = null;
		password = null;
		tableNamespace = null;

		indexType = null;
		tier = -1;

		featureType = null;
		featuresPerEntry = FeatureCollectionDataAdapter.DEFAULT_FEATURES_PER_ENTRY;
		customIndexHandlers = new ArrayList<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>>();
		fieldVisiblityHandler = null;

		numThreads = 1;
	}

	public String getZookeeperUrl() {
		return zookeeperUrl;
	}

	public RedistributeConfig setZookeeperUrl(
			final String zookeeperUrl ) {
		this.zookeeperUrl = zookeeperUrl;
		return this;
	}

	public String getInstanceName() {
		return instanceName;
	}

	public RedistributeConfig setInstanceName(
			final String instanceName ) {
		this.instanceName = instanceName;
		return this;
	}

	public String getUserName() {
		return userName;
	}

	public RedistributeConfig setUserName(
			final String userName ) {
		this.userName = userName;
		return this;
	}

	public String getPassword() {
		return password;
	}

	public RedistributeConfig setPassword(
			final String password ) {
		this.password = password;
		return this;
	}

	public String getTableNamespace() {
		return tableNamespace;
	}

	public RedistributeConfig setTableNamespace(
			final String tableNamespace ) {
		this.tableNamespace = tableNamespace;
		return this;
	}

	public IndexType getIndexType() {
		return indexType;
	}

	public RedistributeConfig setIndexType(
			final IndexType indexType ) {
		this.indexType = indexType;
		return this;
	}

	public int getTier() {
		return tier;
	}

	public RedistributeConfig setTier(
			final int tier ) {
		this.tier = tier;
		return this;
	}

	public SimpleFeatureType getFeatureType() {
		return featureType;
	}

	public RedistributeConfig setFeatureType(
			final SimpleFeatureType featureType ) {
		this.featureType = featureType;
		return this;
	}

	public int getFeaturesPerEntry() {
		return featuresPerEntry;
	}

	public RedistributeConfig setFeaturesPerEntry(
			final int featuresPerEntry ) {
		this.featuresPerEntry = featuresPerEntry;
		return this;
	}

	public List<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> getCustomIndexHandlers() {
		return customIndexHandlers;
	}

	public RedistributeConfig setCustomIndexHandlers(
			final List<PersistentIndexFieldHandler<DefaultFeatureCollection, ? extends CommonIndexValue, Object>> customIndexHandlers ) {
		this.customIndexHandlers = customIndexHandlers;
		return this;
	}

	public FieldVisibilityHandler<DefaultFeatureCollection, Object> getFieldVisiblityHandler() {
		return fieldVisiblityHandler;
	}

	public RedistributeConfig setFieldVisiblityHandler(
			final FieldVisibilityHandler<DefaultFeatureCollection, Object> fieldVisiblityHandler ) {
		this.fieldVisiblityHandler = fieldVisiblityHandler;
		return this;
	}

	public int getNumThreads() {
		return numThreads;
	}

	public RedistributeConfig setNumThreads(
			final int numThreads ) {
		this.numThreads = numThreads;
		return this;
	}
}
