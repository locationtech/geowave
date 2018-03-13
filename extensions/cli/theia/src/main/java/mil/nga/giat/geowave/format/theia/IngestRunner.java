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

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
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
import mil.nga.giat.geowave.format.theia.BandFeatureIterator;
import mil.nga.giat.geowave.format.theia.IngestRunner;
import mil.nga.giat.geowave.format.theia.TheiaBasicCommandLineOptions;
import mil.nga.giat.geowave.format.theia.TheiaDownloadCommandLineOptions;
import mil.nga.giat.geowave.format.theia.TheiaRasterIngestCommandLineOptions;
import mil.nga.giat.geowave.format.theia.RasterIngestRunner;
import mil.nga.giat.geowave.format.theia.SceneFeatureIterator;
import mil.nga.giat.geowave.format.theia.VectorIngestRunner;
import mil.nga.giat.geowave.format.theia.VectorOverrideCommandLineOptions;

public class IngestRunner extends
		RasterIngestRunner
{
	private final static Logger LOGGER = LoggerFactory.getLogger(IngestRunner.class);

	private IndexWriter<SimpleFeature> bandWriter;
	private IndexWriter<SimpleFeature> sceneWriter;
	private final VectorOverrideCommandLineOptions vectorOverrideOptions;
	private SimpleFeatureType sceneType;

	public IngestRunner(
			final TheiaBasicCommandLineOptions analyzeOptions,
			final TheiaDownloadCommandLineOptions downloadOptions,
			final TheiaRasterIngestCommandLineOptions ingestOptions,
			final VectorOverrideCommandLineOptions vectorOverrideOptions,
			final List<String> parameters ) {
		super(
				analyzeOptions,
				downloadOptions,
				ingestOptions,
				parameters);
		this.vectorOverrideOptions = vectorOverrideOptions;
	}

	@SuppressWarnings("unchecked")
	@Override
	protected void processParameters(
			final OperationParams params )
			throws Exception { // Ensure we have all the required arguments
		super.processParameters(params);

		final DataStore vectorStore;
		final PrimaryIndex[] vectorIndices;

		// Config file
		final File configFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);

		if ((vectorOverrideOptions.getVectorStore() != null)
				&& !vectorOverrideOptions.getVectorStore().trim().isEmpty()) {
			String vectorStoreName = vectorOverrideOptions.getVectorStore();

			final StoreLoader vectorStoreLoader = new StoreLoader(
					vectorStoreName);
			if (!vectorStoreLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find vector store name: " + vectorStoreLoader.getStoreName());
			}

			final DataStorePluginOptions vectorStoreOptions = vectorStoreLoader.getDataStorePlugin();
			vectorStore = vectorStoreOptions.createDataStore();
		}
		else {
			vectorStore = store;
		}

		if ((vectorOverrideOptions.getVectorIndex() != null)
				&& !vectorOverrideOptions.getVectorIndex().trim().isEmpty()) {
			String vectorIndexList = vectorOverrideOptions.getVectorIndex();

			// Load the Indices
			final IndexLoader indexLoader = new IndexLoader(
					vectorIndexList);
			if (!indexLoader.loadFromConfig(configFile)) {
				throw new ParameterException(
						"Cannot find index(s) by name: " + vectorIndexList);
			}

			final List<IndexPluginOptions> indexOptions = indexLoader.getLoadedIndexes();
			vectorIndices = new PrimaryIndex[indexOptions.size()];
			int i = 0;
			for (final IndexPluginOptions dimensionType : indexOptions) {
				final PrimaryIndex primaryIndex = dimensionType.createPrimaryIndex();
				if (primaryIndex == null) {
					LOGGER.error("Could not get index instance, getIndex() returned null;");
					throw new IOException(
							"Could not get index instance, getIndex() returned null");
				}
				vectorIndices[i++] = primaryIndex;
			}
		}
		else {
			vectorIndices = indices;
		}

		sceneType = SceneFeatureIterator.defaultSceneFeatureTypeBuilder().buildFeatureType();
		final FeatureDataAdapter sceneAdapter = new FeatureDataAdapter(
				sceneType);
		sceneWriter = vectorStore.createWriter(
				sceneAdapter,
				vectorIndices);

		final SimpleFeatureType bandType = BandFeatureIterator.defaultBandFeatureTypeBuilder().buildFeatureType();
		final FeatureDataAdapter bandAdapter = new FeatureDataAdapter(
				bandType);
		bandWriter = vectorStore.createWriter(
				bandAdapter,
				vectorIndices);
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
		VectorIngestRunner.writeScene(
				sceneType,
				firstBandOfScene,
				sceneWriter);
		super.nextScene(
				firstBandOfScene,
				analysisInfo);
	}

	@Override
	protected void runInternal(
			OperationParams params )
			throws Exception {
		try {
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
}
