/*******************************************************************************
 * Copyright (c) 2013-2018 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.format.theia;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.format.theia.AnalyzeRunner;
import mil.nga.giat.geowave.format.theia.BandFeatureIterator;
import mil.nga.giat.geowave.format.theia.TheiaBasicCommandLineOptions;
import mil.nga.giat.geowave.format.theia.RasterIngestRunner;
import mil.nga.giat.geowave.format.theia.SceneFeatureIterator;

public class VectorIngestRunner extends
		AnalyzeRunner
{
	private final static Logger LOGGER = LoggerFactory.getLogger(RasterIngestRunner.class);

	protected final List<String> parameters;
	private IndexWriter<SimpleFeature> bandWriter;
	private IndexWriter<SimpleFeature> sceneWriter;
	private SimpleFeatureType sceneType;

	public VectorIngestRunner(
			final TheiaBasicCommandLineOptions analyzeOptions,
			final List<String> parameters ) {
		super(
				analyzeOptions);
		this.parameters = parameters;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void runInternal(
			final OperationParams params )
			throws Exception {
		try {
			// Ensure we have all the required arguments
			if (parameters.size() != 2) {
				throw new ParameterException(
						"Requires arguments: <storename> <comma delimited index/group list>");
			}

			final String inputStoreName = parameters.get(0);
			final String indexList = parameters.get(1);

			// Config file
			final File configFile = (File) params.getContext().get(
					ConfigOptions.PROPERTIES_FILE_CONTEXT);

			// Attempt to load input store.
			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}

			final DataStorePluginOptions storeOptions = inputStoreLoader.getDataStorePlugin();
			final DataStore store = storeOptions.createDataStore();

			// Load the Indices
			final IndexLoader indexLoader = new IndexLoader(
					indexList);
			if (!indexLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find index(s) by name: " + indexList);
			}

			final List<IndexPluginOptions> indexOptions = indexLoader.getLoadedIndexes();
			final PrimaryIndex[] indices = new PrimaryIndex[indexOptions.size()];
			int i = 0;
			for (final IndexPluginOptions dimensionType : indexOptions) {
				final PrimaryIndex primaryIndex = dimensionType.createPrimaryIndex();
				if (primaryIndex == null) {
					LOGGER.error("Could not get index instance, getIndex() returned null;");
					throw new IOException(
							"Could not get index instance, getIndex() returned null");
				}
				indices[i++] = primaryIndex;
			}

			sceneType = SceneFeatureIterator.defaultSceneFeatureTypeBuilder().buildFeatureType();
			final FeatureDataAdapter sceneAdapter = new FeatureDataAdapter(
					sceneType);
			sceneWriter = store.createWriter(
					sceneAdapter,
					indices);

			final SimpleFeatureType bandType = BandFeatureIterator.defaultBandFeatureTypeBuilder().buildFeatureType();
			final FeatureDataAdapter bandAdapter = new FeatureDataAdapter(
					bandType);
			bandWriter = store.createWriter(
					bandAdapter,
					indices);

			super.runInternal(params);
		}
		finally {
			if (sceneWriter != null) {
				try {
					sceneWriter.close();
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to close writer for scene vectors",
							e);
				}
			}
			if (bandWriter != null) {
				try {
					bandWriter.close();
				}
				catch (final IOException e) {
					LOGGER.error(
							"Unable to close writer for band vectors",
							e);
				}
			}
		}
	}

	@Override
	protected void nextBand(
			final SimpleFeature band,
			final AnalysisInfo analysisInfo ) {
		try {
			bandWriter.write(band);
		}
		catch (IOException e) {
			LOGGER.error(
					"Unable to write next band",
					e);
		}
		super.nextBand(
				band,
				analysisInfo);
	}

	@Override
	protected void nextScene(
			final SimpleFeature firstBandOfScene,
			final AnalysisInfo analysisInfo ) {
		writeScene(
				sceneType,
				firstBandOfScene,
				sceneWriter);
		super.nextScene(
				firstBandOfScene,
				analysisInfo);
	}

	public static void writeScene(
			final SimpleFeatureType sceneType,
			final SimpleFeature firstBandOfScene,
			final IndexWriter<SimpleFeature> sceneWriter ) {
		final SimpleFeatureBuilder featureBuilder = new SimpleFeatureBuilder(
				sceneType);
		String fid = null;

		for (int i = 0; i < sceneType.getAttributeCount(); i++) {
			final AttributeDescriptor descriptor = sceneType.getDescriptor(i);

			final String name = descriptor.getLocalName();
			final Object value = firstBandOfScene.getAttribute(name);

			if (value != null) {
				featureBuilder.set(
						i,
						value);

				if (name.equals(SceneFeatureIterator.ENTITY_ID_ATTRIBUTE_NAME)) {
					fid = value.toString();
				}
			}
		}
		if (fid != null) {
			try {
				sceneWriter.write(featureBuilder.buildFeature(fid));
			}
			catch (IOException e) {
				LOGGER.error(
						"Unable to write scene",
						e);
			}
		}
	}
}
