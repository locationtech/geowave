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
package org.locationtech.geowave.core.ingest.spark;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLStreamHandlerFactory;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;
import java.nio.file.attribute.BasicFileAttributes;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.ingest.IngestUtils;
import org.locationtech.geowave.core.ingest.IngestUtils.URLTYPE;
import org.locationtech.geowave.core.ingest.local.AbstractLocalFileDriver;
import org.locationtech.geowave.core.ingest.local.LocalFileIngestDriver;
import org.locationtech.geowave.core.ingest.local.LocalIngestRunData;
import org.locationtech.geowave.core.ingest.local.LocalInputCommandLineOptions;
import org.locationtech.geowave.core.ingest.local.LocalPluginFileVisitor.PluginVisitor;
import org.locationtech.geowave.core.ingest.operations.ConfigAWSCommand;
import org.locationtech.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.cli.remote.options.DataStorePluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.IndexLoader;
import org.locationtech.geowave.core.store.cli.remote.options.IndexPluginOptions;
import org.locationtech.geowave.core.store.cli.remote.options.StoreLoader;
import org.locationtech.geowave.core.store.cli.remote.options.VisibilityOptions;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import org.locationtech.geowave.mapreduce.s3.GeoWaveAmazonS3Factory;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.ParameterException;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;
import com.upplication.s3fs.S3Path;

public class SparkIngestDriver implements
		Serializable
{

	private final static Logger LOGGER = LoggerFactory.getLogger(SparkIngestDriver.class);

	public SparkIngestDriver() {

	}

	public boolean runOperation(
			File configFile,
			LocalInputCommandLineOptions localInput,
			String inputStoreName,
			String indexList,
			VisibilityOptions ingestOptions,
			SparkCommandLineOptions sparkOptions,
			String basePath )
			throws IOException {

		final Properties configProperties = ConfigOptions.loadProperties(
				configFile);

		JavaSparkContext jsc = null;
		SparkSession session = null;
		int numExecutors;
		int numCores;
		int numPartitions;
		Path inputPath;
		String s3EndpointUrl = null;
		
		
		boolean isS3 = basePath.startsWith(
				"s3://");
		boolean isHDFS = !isS3 && (basePath.startsWith(
				"hdfs://") || basePath.startsWith(
						"file:/"));
		
		// If input path is S3
		if (isS3) {

			s3EndpointUrl = ConfigAWSCommand.getS3Url(
					configProperties);
			inputPath = (S3Path) IngestUtils.setupS3FileSystem(
					basePath,
					s3EndpointUrl);
		}
		// If input path is HDFS
		else if (isHDFS) {

			String hdfsFSUrl = ConfigHDFSCommand.getHdfsUrl(
					configProperties);
			inputPath = setUpHDFSFilesystem(
					basePath,
					hdfsFSUrl,
					basePath.startsWith(
							"file:/"));
		}
		else {
			LOGGER.warn(
					"Spark ingest support only S3 or HDFS as input location");
			return false;
		}

		if ((inputPath == null) || (!Files.exists(
				inputPath))) {
			LOGGER.error(
					"Error in accessing Input path " + basePath);
			return false;
		}

		List<Path> inputFileList = new ArrayList<Path>();
		Files.walkFileTree(
				inputPath,
				new SimpleFileVisitor<Path>() {

					@Override
					public FileVisitResult visitFile(
							Path file,
							BasicFileAttributes attrs )
							throws IOException {
						inputFileList.add(
								file);
						return FileVisitResult.CONTINUE;
					}
				});

		int numInputFiles = inputFileList.size();

		if (sparkOptions.getNumExecutors() < 1) {
			numExecutors = (int) Math.ceil(
					(double) numInputFiles / 8);
		}
		else {
			numExecutors = sparkOptions.getNumExecutors();
		}

		if (sparkOptions.getNumCores() < 1) {
			numCores = 4;
		}
		else {
			numCores = sparkOptions.getNumCores();
		}
		numPartitions = numExecutors * numCores * 2;

		if (session == null) {
			String jar = "";
			try {
				jar = SparkIngestDriver.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
			}
			catch (final URISyntaxException e) {
				LOGGER.error(
						"Unable to set jar location in spark configuration",
						e);
			}

			session = SparkSession
					.builder()
					.appName(
							sparkOptions.getAppName())
					.master(
							sparkOptions.getMaster())
					.config(
							"spark.driver.host",
							sparkOptions.getHost())
					.config(
							"spark.jars",
							jar)
					.config("spark.executor.instances",Integer.toString(
							numExecutors))
					.config("spark.executor.cores",Integer.toString(
							numCores))
					.getOrCreate();

			jsc = JavaSparkContext.fromSparkContext(
					session.sparkContext());
		}

		JavaRDD<URI> fileRDD = jsc.parallelize(
				Lists.transform(
						inputFileList,
						new Function<Path, URI>() {

							@Override
							public URI apply(
									Path arg0 ) {
								return arg0.toUri();
							}
						}),
				numPartitions);
		if (isS3) {
			final String s3FinalEndpointUrl = s3EndpointUrl;
			fileRDD.foreachPartition(
					uri -> {

						S3FileSystem fs = initializeS3FS(
								s3FinalEndpointUrl);
						List<URI> inputFiles = new ArrayList<URI>();
						while (uri.hasNext()) {
							Path inputFile = (S3Path) fs.getPath(
									uri.next().toString().replaceFirst(
											s3FinalEndpointUrl,
											""));
							inputFiles.add(
									inputFile.toUri());
						}

						processInput(
								configFile,
								localInput,
								inputStoreName,
								indexList,
								ingestOptions,
								configProperties,
								inputFiles.iterator());
					});
		}
		else if (isHDFS) {
			try {
				setHdfsURLStreamHandlerFactory();
			} catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			fileRDD.foreachPartition(
					uri -> {		
						processInput(
								configFile,
								localInput,
								inputStoreName,
								indexList,
								ingestOptions,
								configProperties,
								uri);
					});
		}
	
		close(session);
		return true;
	}

	public void processInput(
			File configFile,
			LocalInputCommandLineOptions localInput,
			String inputStoreName,
			String indexList,
			VisibilityOptions ingestOptions,
			Properties configProperties,
			Iterator<URI> inputFiles )
			throws IOException {

		// Based on the selected formats, select the format plugins
		IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();
		// Based on the selected formats, select the format plugins
		pluginFormats.selectPlugin(localInput.getFormats());
		DataStorePluginOptions inputStoreOptions = null;
		List<IndexPluginOptions> indexOptions = null;

		// Ingest Plugins
		final Map<String, LocalFileIngestPlugin<?>> ingestPlugins = pluginFormats.createLocalIngestPlugins();

		final StoreLoader inputStoreLoader = new StoreLoader(
				inputStoreName);
		if (!inputStoreLoader.loadFromConfig(
				configProperties,
				DataStorePluginOptions.getStoreNamespace(inputStoreName),
				configFile)) {
			throw new ParameterException(
					"Cannot find store name: " + inputStoreLoader.getStoreName());
		}
		inputStoreOptions = inputStoreLoader.getDataStorePlugin();

		final IndexLoader indexLoader = new IndexLoader(
				indexList);
		if (!indexLoader.loadFromConfig(configProperties)) {
			throw new ParameterException(
					"Cannot find index(s) by name: " + indexList);
		}
		indexOptions = indexLoader.getLoadedIndexes();

		// first collect the local file ingest plugins
		final Map<String, LocalFileIngestPlugin<?>> localFileIngestPlugins = new HashMap<String, LocalFileIngestPlugin<?>>();
		final List<DataTypeAdapter<?>> adapters = new ArrayList<DataTypeAdapter<?>>();
		for (Entry<String, LocalFileIngestPlugin<?>> pluginEntry : ingestPlugins.entrySet()) {

			if (!IngestUtils.checkIndexesAgainstProvider(
					pluginEntry.getKey(),
					pluginEntry.getValue(),
					indexOptions)) {
				continue;
			}

			localFileIngestPlugins.put(
					pluginEntry.getKey(),
					pluginEntry.getValue());

			adapters.addAll(Arrays.asList(pluginEntry.getValue().getDataAdapters(
					ingestOptions.getVisibility())));
		}

		LocalFileIngestDriver localIngestDriver = new LocalFileIngestDriver(
				inputStoreOptions,
				indexOptions,
				localFileIngestPlugins,
				ingestOptions,
				localInput,
				1);

		localIngestDriver.startExecutor();

		DataStore dataStore = inputStoreOptions.createDataStore();
		try (LocalIngestRunData runData = new LocalIngestRunData(
				adapters,
				dataStore)) {

			List<PluginVisitor<LocalFileIngestPlugin<?>>> pluginVisitors = new ArrayList<>(
					localFileIngestPlugins.size());
			for (final Entry<String, LocalFileIngestPlugin<?>> localPlugin : localFileIngestPlugins.entrySet()) {
				pluginVisitors.add(new PluginVisitor<LocalFileIngestPlugin<?>>(
						localPlugin.getValue(),
						localPlugin.getKey(),
						localInput.getExtensions()));
			}

			while (inputFiles.hasNext()) {
				final URL file = inputFiles.next().toURL();
				for (final PluginVisitor<LocalFileIngestPlugin<?>> visitor : pluginVisitors) {
					if (visitor.supportsFile(file)) {
						localIngestDriver.processFile(
								file,
								visitor.getTypeName(),
								visitor.getLocalPluginBase(),
								runData);
					}
				}
			}

		}
		catch (MalformedURLException e) {
			LOGGER.error(
					"Error in converting input path to URL for " + inputFiles,
					e);
			throw new MalformedURLException(
					"Error in converting input path to URL for " + inputFiles);
		}
		catch (Exception e) {
			LOGGER.error(
					"Error processing in processing input",
					e);
			throw new RuntimeException(
					"Error processing in processing input",
					e);
		}
		finally {
			localIngestDriver.shutdownExecutor();
		}
	}

	public void close(
			SparkSession session ) {
		if (session != null) {
			session.close();
			session = null;
		}
	}

	public Path setUpHDFSFilesystem(
			String basePath,
			String hdfsFSUrl,
			boolean isLocalPath ) {

		String hdfsInputPath = basePath.replaceFirst(
				"hdfs://",
				"/");

		Path path = null;
		try {

			URI uri = null;
			if (isLocalPath) {
				uri = new URI(
						hdfsInputPath);
			}
			else {
				uri = new URI(
						hdfsFSUrl + hdfsInputPath);
			}
			path = Paths.get(uri);
			// HP Fortify "Path Traversal" false positive
			// What Fortify considers "user input" comes only
			// from users with OS-level access anyway

		}
		catch (URISyntaxException e) {
			LOGGER.error(
					"Unable to ingest data, Inavlid HDFS Path",
					e);
			return null;
		}

		return path;

	}

	public S3FileSystem initializeS3FS(
			String s3EndpointUrl )
			throws URISyntaxException {

		try {
			IngestUtils.setURLStreamHandlerFactory(URLTYPE.S3);
		}
		catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e1) {
			LOGGER.error(
					"Error in setting up S3URLStreamHandler Factory",
					e1);
			throw new RuntimeException(
					"Error in setting up S3URLStreamHandler Factory",
					e1);
		}

		return (S3FileSystem) new S3FileSystemProvider().getFileSystem(
				new URI(
						s3EndpointUrl),
				Collections.singletonMap(
						S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS,
						GeoWaveAmazonS3Factory.class.getName()));
	}

	public static void setHdfsURLStreamHandlerFactory()
			throws NoSuchFieldException,
			SecurityException,
			IllegalArgumentException,
			IllegalAccessException {
		Field factoryField = URL.class.getDeclaredField("factory");
		factoryField.setAccessible(true);
		// HP Fortify "Access Control" false positive
		// The need to change the accessibility here is
		// necessary, has been review and judged to be safe

		URLStreamHandlerFactory urlStreamHandlerFactory = (URLStreamHandlerFactory) factoryField.get(null);

		if (urlStreamHandlerFactory == null) {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		}
		else {
			try {
				factoryField.setAccessible(true);
				// HP Fortify "Access Control" false positive
				// The need to change the accessibility here is
				// necessary, has been review and judged to be safe
				factoryField.set(
						null,
						new FsUrlStreamHandlerFactory());
			}
			catch (IllegalAccessException e1) {
				LOGGER.error(
						"Could not access URLStreamHandler factory field on URL class: {}",
						e1);
				throw new RuntimeException(
						"Could not access URLStreamHandler factory field on URL class: {}",
						e1);
			}
		}
	}

}
