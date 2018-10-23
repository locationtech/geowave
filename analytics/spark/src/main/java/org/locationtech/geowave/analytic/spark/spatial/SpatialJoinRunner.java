/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 *   
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 *  All rights reserved. This program and the accompanying materials
 *  are made available under the terms of the Apache License,
 *  Version 2.0 which accompanies this distribution and is available at
 *  http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package org.locationtech.geowave.analytic.spark.spatial;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.geowave.adapter.vector.FeatureDataAdapter;
import org.locationtech.geowave.adapter.vector.util.FeatureDataUtils;
import org.locationtech.geowave.analytic.spark.GeoWaveIndexedRDD;
import org.locationtech.geowave.analytic.spark.GeoWaveRDD;
import org.locationtech.geowave.analytic.spark.GeoWaveRDDLoader;
import org.locationtech.geowave.analytic.spark.GeoWaveSparkConf;
import org.locationtech.geowave.analytic.spark.RDDOptions;
import org.locationtech.geowave.analytic.spark.RDDUtils;
import org.locationtech.geowave.analytic.spark.sparksql.udf.GeomFunction;
import org.locationtech.geowave.core.index.ByteArrayId;
import org.locationtech.geowave.core.index.NumericIndexStrategy;
import org.locationtech.geowave.core.store.adapter.AdapterStore;
import org.locationtech.geowave.core.store.adapter.InternalAdapterStore;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.index.PrimaryIndex;
import org.locationtech.geowave.core.store.query.QueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SpatialJoinRunner implements
		Serializable
{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;

	private final static Logger LOGGER = LoggerFactory.getLogger(SpatialJoinRunner.class);

	// Options provided by user to run join
	private SparkSession session = null;
	private SparkContext sc = null;
	private String appName = "SpatialJoinRunner";
	private String master = "yarn";
	private String host = "localhost";
	private Integer partCount = -1;
	private DataStorePluginOptions leftStore = null;
	private ByteArrayId leftAdapterId = null;
	private ByteArrayId outLeftAdapterId = null;
	private DataStorePluginOptions rightStore = null;
	private ByteArrayId rightAdapterId = null;
	private ByteArrayId outRightAdapterId = null;
	private boolean negativeTest = false;

	private DataStorePluginOptions outputStore = null;
	private GeomFunction predicate = null;
	private NumericIndexStrategy indexStrategy = null;
	// Variables loaded during runner. This can be updated to something cleaner
	// like GeoWaveRDD in future
	// to support different situations (indexed vs non indexed etc..) but keep
	// it hidden in implementation details
	private GeoWaveIndexedRDD leftRDD = null;
	private GeoWaveIndexedRDD rightRDD = null;

	private InternalAdapterStore leftInternalAdapterStore;
	private InternalAdapterStore rightInternalAdapterStore;

	private IndexStore leftIndexStore;
	private IndexStore rightIndexStore;

	// TODO: Join strategy could be supplied as variable or determined
	// automatically from index store (would require associating index and join
	// strategy)
	// for now will just use TieredSpatialJoin as that is the only one we have
	// implemented.
	private final JoinStrategy joinStrategy = new TieredSpatialJoin();

	public SpatialJoinRunner() {}

	public SpatialJoinRunner(
			final SparkSession session ) {
		this.session = session;
	}

	public void run()
			throws InterruptedException,
			ExecutionException,
			IOException {
		leftInternalAdapterStore = leftStore.createInternalAdapterStore();
		rightInternalAdapterStore = rightStore.createInternalAdapterStore();
		leftIndexStore = leftStore.createIndexStore();
		rightIndexStore = rightStore.createIndexStore();
		// Init context
		initContext();
		// Load RDDs
		loadDatasets();
		// Verify CRS match/transform possible
		verifyCRS();
		// Run join

		joinStrategy.getJoinOptions().setNegativePredicate(
				negativeTest);
		joinStrategy.join(
				session,
				leftRDD,
				rightRDD,
				predicate);

		writeResultsToNewAdapter();
	}

	public void close() {
		if (session != null) {
			session.close();
			session = null;
		}
	}

	private PrimaryIndex[] getIndicesForAdapter(
			DataStorePluginOptions storeOptions,
			ByteArrayId adapterId,
			InternalAdapterStore internalAdapterStore,
			IndexStore indexStore ) {
		return storeOptions.createAdapterIndexMappingStore().getIndicesForAdapter(
				internalAdapterStore.getInternalAdapterId(adapterId)).getIndices(
				indexStore);
	}

	private FeatureDataAdapter createOutputAdapter(
			DataStorePluginOptions originalOptions,
			ByteArrayId originalId,
			PrimaryIndex[] indices,
			ByteArrayId outputAdapterId ) {

		if (outputAdapterId == null) {
			outputAdapterId = createDefaultAdapterName(
					originalId,
					originalOptions);
		}
		FeatureDataAdapter newAdapter = FeatureDataUtils.cloneFeatureDataAdapter(
				originalOptions,
				originalId,
				outputAdapterId);
		newAdapter.init(indices);
		return newAdapter;
	}

	private void writeResultsToNewAdapter()
			throws IOException {
		if (outputStore != null) {
			final PrimaryIndex[] leftIndices = this.getIndicesForAdapter(
					leftStore,
					leftAdapterId,
					leftInternalAdapterStore,
					leftIndexStore);
			final FeatureDataAdapter newLeftAdapter = this.createOutputAdapter(
					leftStore,
					leftAdapterId,
					leftIndices,
					outLeftAdapterId);

			final PrimaryIndex[] rightIndices = this.getIndicesForAdapter(
					rightStore,
					rightAdapterId,
					rightInternalAdapterStore,
					rightIndexStore);
			final FeatureDataAdapter newRightAdapter = this.createOutputAdapter(
					rightStore,
					rightAdapterId,
					rightIndices,
					outRightAdapterId);
			// Write each feature set to new adapter and store using original
			// indexing methods.
			RDDUtils.writeRDDToGeoWave(
					sc,
					leftIndices,
					outputStore,
					newLeftAdapter,
					getLeftResults());
			RDDUtils.writeRDDToGeoWave(
					sc,
					rightIndices,
					outputStore,
					newRightAdapter,
					getRightResults());
		}
	}

	private ByteArrayId createDefaultAdapterName(
			final ByteArrayId adapterId,
			final DataStorePluginOptions storeOptions ) {
		final String defaultAdapterName = adapterId + "_joined";
		ByteArrayId defaultAdapterId = new ByteArrayId(
				defaultAdapterName);
		final AdapterStore adapterStore = storeOptions.createAdapterStore();
		if (!adapterStore.adapterExists(defaultAdapterId)) {
			return defaultAdapterId;
		}
		Integer iSuffix = 0;
		String uniNum = "_" + String.format(
				"%02d",
				iSuffix);
		defaultAdapterId = new ByteArrayId(
				defaultAdapterName + uniNum);
		while (adapterStore.adapterExists(defaultAdapterId)) {
			// Should be _00 _01 etc
			iSuffix += 1;
			uniNum = "_" + String.format(
					"%02d",
					iSuffix);
			defaultAdapterId = new ByteArrayId(
					defaultAdapterName + uniNum);
		}
		return defaultAdapterId;
	}

	private void initContext() {
		if (session == null) {
			String jar = "";
			try {
				jar = SpatialJoinRunner.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			}
			catch (final URISyntaxException e) {
				LOGGER.error(
						"Unable to set jar location in spark configuration",
						e);
			}
			SparkConf addonOptions = GeoWaveSparkConf.getDefaultConfig();
			addonOptions = addonOptions.setAppName(
					appName).setMaster(
					master).set(
					"spark.jars",
					jar);

			if (!Objects.equals(
					master,
					"yarn")) {
				addonOptions = addonOptions.set(
						"spark.driver.host",
						host);
			}

			// Since default parallelism is normally set by spark-defaults only
			// set this to config if supplied by user
			if (partCount != -1) {
				addonOptions = addonOptions.set(
						"spark.default.parallelism",
						partCount.toString());
			}
			session = GeoWaveSparkConf.createDefaultSession(addonOptions);
		}
		sc = session.sparkContext();
	}

	private GeoWaveIndexedRDD createRDDFromOptions(
			DataStorePluginOptions storeOptions,
			ByteArrayId adapterId,
			InternalAdapterStore internalAdapterStore,
			IndexStore indexStore )
			throws IOException {
		QueryOptions adapterOptions;

		// If no adapterId provided by user grab first adapterId
		// available.
		if (adapterId == null) {
			List<ByteArrayId> byteIds = FeatureDataUtils.getFeatureAdapterIds(storeOptions);
			if (!byteIds.isEmpty()) {
				adapterId = byteIds.get(0);
			}
			else {
				LOGGER.error("No valid adapter found in store to perform join.");
				return null;
			}
		}

		adapterOptions = new QueryOptions(
				storeOptions.createAdapterStore().getAdapter(
						internalAdapterStore.getInternalAdapterId(adapterId)));

		RDDOptions rddOpts = new RDDOptions();
		rddOpts.setQueryOptions(adapterOptions);
		rddOpts.setMinSplits(this.partCount);
		rddOpts.setMaxSplits(this.partCount);

		NumericIndexStrategy rddStrategy = null;
		// Did the user provide a strategy for join?
		if (this.indexStrategy == null) {
			PrimaryIndex[] rddIndices = this.getIndicesForAdapter(
					storeOptions,
					adapterId,
					internalAdapterStore,
					indexStore);
			if (rddIndices.length > 0) {
				rddStrategy = rddIndices[0].getIndexStrategy();
			}

		}
		else {
			rddStrategy = this.indexStrategy;
		}

		return GeoWaveRDDLoader.loadIndexedRDD(
				sc,
				storeOptions,
				rddOpts,
				rddStrategy);
	}

	private void loadDatasets()
			throws IOException {
		if (leftStore != null) {
			if (leftRDD == null) {
				leftRDD = this.createRDDFromOptions(
						leftStore,
						leftAdapterId,
						leftInternalAdapterStore,
						leftIndexStore);
			}
		}

		if (rightStore != null) {
			if (rightRDD == null) {
				rightRDD = this.createRDDFromOptions(
						rightStore,
						rightAdapterId,
						rightInternalAdapterStore,
						rightIndexStore);
			}

		}
	}

	private void verifyCRS() {
		// TODO: Verify that both stores have matching CRS or that one CRS can
		// be transformed into the other
	}

	// Accessors and Mutators
	public GeoWaveRDD getLeftResults() {
		return joinStrategy.getLeftResults();
	}

	public GeoWaveRDD getRightResults() {
		return joinStrategy.getRightResults();
	}

	public DataStorePluginOptions getLeftStore() {
		return leftStore;
	}

	public void setLeftStore(
			final DataStorePluginOptions leftStore ) {
		this.leftStore = leftStore;
	}

	public ByteArrayId getLeftAdapterId() {
		return leftAdapterId;
	}

	public void setLeftAdapterId(
			final ByteArrayId leftAdapterId ) {
		this.leftAdapterId = leftAdapterId;
	}

	public DataStorePluginOptions getRightStore() {
		return rightStore;
	}

	public void setRightStore(
			final DataStorePluginOptions rightStore ) {
		this.rightStore = rightStore;
	}

	public ByteArrayId getRightAdapterId() {
		return rightAdapterId;
	}

	public void setRightAdapterId(
			final ByteArrayId rightAdapterId ) {
		this.rightAdapterId = rightAdapterId;
	}

	public DataStorePluginOptions getOutputStore() {
		return outputStore;
	}

	public void setOutputStore(
			final DataStorePluginOptions outputStore ) {
		this.outputStore = outputStore;
	}

	public GeomFunction getPredicate() {
		return predicate;
	}

	public void setPredicate(
			final GeomFunction predicate ) {
		this.predicate = predicate;
	}

	public NumericIndexStrategy getIndexStrategy() {
		return indexStrategy;
	}

	public void setIndexStrategy(
			final NumericIndexStrategy indexStrategy ) {
		this.indexStrategy = indexStrategy;
	}

	public String getAppName() {
		return appName;
	}

	public void setAppName(
			final String appName ) {
		this.appName = appName;
	}

	public String getMaster() {
		return master;
	}

	public void setMaster(
			final String master ) {
		this.master = master;
	}

	public String getHost() {
		return host;
	}

	public void setHost(
			final String host ) {
		this.host = host;
	}

	public Integer getPartCount() {
		return partCount;
	}

	public void setPartCount(
			final Integer partCount ) {
		this.partCount = partCount;
	}

	public void setSession(
			final SparkSession session ) {
		this.session = session;
	}

	public ByteArrayId getOutputLeftAdapterId() {
		return outLeftAdapterId;
	}

	public void setOutputLeftAdapterId(
			final ByteArrayId outLeftAdapterId ) {
		this.outLeftAdapterId = outLeftAdapterId;
	}

	public ByteArrayId getOutputRightAdapterId() {
		return outRightAdapterId;
	}

	public void setOutputRightAdapterId(
			final ByteArrayId outRightAdapterId ) {
		this.outRightAdapterId = outRightAdapterId;
	}

	public void setLeftRDD(
			final GeoWaveIndexedRDD leftRDD ) {
		this.leftRDD = leftRDD;
	}

	public void setRightRDD(
			final GeoWaveIndexedRDD rightRDD ) {
		this.rightRDD = rightRDD;
	}

	public boolean isNegativeTest() {
		return negativeTest;
	}

	public void setNegativeTest(
			final boolean negativeTest ) {
		this.negativeTest = negativeTest;
	}

}
