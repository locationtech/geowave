/*******************************************************************************
 * Copyright (c) 2013-2017 Contributors to the Eclipse Foundation
 * 
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License,
 * Version 2.0 which accompanies this distribution and is available at
 * http://www.apache.org/licenses/LICENSE-2.0.txt
 ******************************************************************************/
package mil.nga.giat.geowave.core.ingest.spark;

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
import org.apache.spark.api.java.JavaRDD;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.ingest.IngestUtils;
import mil.nga.giat.geowave.core.ingest.IngestUtils.URLTYPE;
import mil.nga.giat.geowave.core.ingest.local.AbstractLocalFileDriver;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestDriver;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalIngestRunData;
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.local.LocalPluginFileVisitor.PluginVisitor;
import mil.nga.giat.geowave.core.ingest.operations.ConfigAWSCommand;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.IndexPluginOptions;
import mil.nga.giat.geowave.core.store.operations.remote.options.StoreLoader;
import mil.nga.giat.geowave.core.store.operations.remote.options.VisibilityOptions;
import mil.nga.giat.geowave.mapreduce.operations.ConfigHDFSCommand;

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

		final Properties configProperties = ConfigOptions.loadProperties(configFile);

		JavaSparkContext jsc = null;
		SparkSession session = null;
		int numExecutors;
		int numCores;
		int numPartitions;
		Path inputPath;
		String s3EndpointUrl = null;

		if (jsc == null) {
			String jar = "";
			try {
				jar = SparkIngestDriver.class.getProtectionDomain()
						.getCodeSource().getLocation().toURI().getPath();
			} catch (final URISyntaxException e) {
				LOGGER.error(
						"Unable to set jar location in spark configuration", e);
			}

			session = SparkSession.builder().appName(sparkOptions.getAppName())
					.master(sparkOptions.getMaster())
					.config("spark.driver.host", sparkOptions.getHost())
					.config("spark.jars", jar).getOrCreate();

			jsc = new JavaSparkContext(session.sparkContext());
		}
		boolean isS3 = basePath.startsWith("s3://");
		boolean isHDFS = !isS3 && basePath.startsWith("hdfs://");
		// If input path is S3
		if (isS3) {

			s3EndpointUrl = ConfigAWSCommand.getS3Url(configProperties);
			inputPath = (S3Path) setUpS3Filesystem(configProperties, basePath,
					s3EndpointUrl);
		}
		// If input path is HDFS
		else if (isHDFS) {

			String hdfsFSUrl = ConfigHDFSCommand.getHdfsUrl(configProperties);
			inputPath = setUpHDFSFilesystem(basePath, hdfsFSUrl);
		} 
		else {
			LOGGER.warn("Spark ingest support only S3 or HDFS as input location");
			close(jsc, session);
			return false;
		}

		if ((inputPath == null) || (!Files.exists(inputPath))) {
			LOGGER.error("Error in accessing Input path " + basePath);
			close(jsc, session);
			return false;
		}

		List<Path> inputFileList = new ArrayList<Path>();
		Files.walkFileTree(inputPath, new SimpleFileVisitor<Path>() {

			@Override
			public FileVisitResult visitFile(Path file,
					BasicFileAttributes attrs) throws IOException {
				inputFileList.add(file);
				return FileVisitResult.CONTINUE;
			}
		});

		int numInputFiles = inputFileList.size();

		if (sparkOptions.getNumExecutors() < 1) {
			numExecutors = (int) Math.ceil((double) numInputFiles / 8);
		} else {
			numExecutors = sparkOptions.getNumExecutors();
		}

		if (sparkOptions.getNumCores() < 1) {
			numCores = 4;
		} else {
			numCores = sparkOptions.getNumCores();
		}

		jsc.sc().conf().set("spark.executor.instances", Integer.toString(numExecutors));
		jsc.sc().conf().set("spark.executor.cores", Integer.toString(numCores));
		numPartitions = numExecutors * numCores * 2;

		JavaRDD<URI> fileRDD = jsc.parallelize(
				Lists.transform(inputFileList, new Function<Path, URI>() {

					@Override
					public URI apply(Path arg0) {
						return arg0.toUri();
					}
				}), numPartitions);
		if (isS3) {
			final String s3FinalEndpointUrl = s3EndpointUrl;
			fileRDD.foreachPartition(uri -> {

				S3FileSystem fs = initializeS3FS(s3FinalEndpointUrl);
				List<URI> inputFiles = new ArrayList<URI>();
				while (uri.hasNext()) {
					Path inputFile = (S3Path) fs.getPath(uri.next().toString()
							.replaceFirst(s3FinalEndpointUrl, ""));
					inputFiles.add(inputFile.toUri());
				}

				processInput(configFile, localInput, inputStoreName, indexList,
						ingestOptions, configProperties, inputFiles.iterator());
			});
		}
		else if (isHDFS) {
			fileRDD.foreachPartition(uri -> {

				setHdfsURLStreamHandlerFactory();
				processInput(configFile, localInput, inputStoreName, indexList,
						ingestOptions, configProperties, uri);
			});
		}

		if (jsc != null) {
			close(jsc, session);
		}

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

		if (inputStoreOptions == null) {
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
		}

		// Attempt to load input store.
		if (inputStoreOptions == null) {
			final StoreLoader inputStoreLoader = new StoreLoader(
					inputStoreName);
			if (!inputStoreLoader.loadFromConfig(
					configProperties,
					DataStorePluginOptions.getStoreNamespace(inputStoreName),
					null)) {
				throw new ParameterException(
						"Cannot find store name: " + inputStoreLoader.getStoreName());
			}
			inputStoreOptions = inputStoreLoader.getDataStorePlugin();
		}
		// Load the Indexes
		if (indexOptions == null) {
			final IndexLoader indexLoader = new IndexLoader(
					indexList);
			if (!indexLoader.loadFromConfig(configProperties)) {
				throw new ParameterException(
						"Cannot find index(s) by name: " + indexList);
			}
			indexOptions = indexLoader.getLoadedIndexes();
		}

		// first collect the local file ingest plugins
		final Map<String, LocalFileIngestPlugin<?>> localFileIngestPlugins = new HashMap<String, LocalFileIngestPlugin<?>>();
		final List<WritableDataAdapter<?>> adapters = new ArrayList<WritableDataAdapter<?>>();
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
			JavaSparkContext jsc,
			SparkSession session ) {
		if (jsc != null) {
			jsc.close();
			jsc = null;
		}

		if (session != null) {
			session.close();
			session = null;
		}
	}

	public Path setUpS3Filesystem(
			Properties configProperties,
			String basePath,
			String s3EndpointUrl )
			throws IOException {

		Path path = null;
		FileSystem fs = null;
		try {
			fs = FileSystems.newFileSystem(
					new URI(
							s3EndpointUrl + "/"),
					new HashMap<String, Object>(),
					Thread.currentThread().getContextClassLoader());
			// HP Fortify "Path Traversal" false positive
			// What Fortify considers "user input" comes only
			// from users with OS-level access anyway

		}
		catch (URISyntaxException e) {
			LOGGER.error("Unable to ingest data, Inavlid S3 path");
			return null;
		}
		catch (FileSystemAlreadyExistsException e) {
			LOGGER.info("File system " + s3EndpointUrl + "already exists");
			try {
				fs = FileSystems.getFileSystem(new URI(
						s3EndpointUrl + "/"));
			}
			catch (URISyntaxException e1) {
				LOGGER.error("Unable to ingest data, Inavlid S3 path");
				return null;
			}
		}

		String s3InputPath = basePath.replaceFirst(
				"s3://",
				"/");
		try {
			path = fs.getPath(s3InputPath);
		}
		catch (InvalidPathException e) {
			LOGGER.error("Input valid input path " + s3InputPath);
			return null;
		}

		return path;

	}

	public Path setUpHDFSFilesystem(
			String basePath,
			String hdfsFSUrl ) {

		String hdfsInputPath = basePath.replaceFirst(
				"hdfs://",
				"/");

		Path path = null;
		try {

			URI uri = new URI(
					hdfsFSUrl + hdfsInputPath);
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

		S3FileSystem fs = null;
		try {
			if (new S3FileSystemProvider().getFileSystem(new URI(
					s3EndpointUrl)) != null) {
				fs = new S3FileSystemProvider().getFileSystem(new URI(
						s3EndpointUrl));

			}
		}
		catch (FileSystemNotFoundException e) {
			fs = (S3FileSystem) new S3FileSystemProvider().newFileSystem(
					new URI(
							s3EndpointUrl),
					new HashMap<String, Object>());
		}

		return fs;
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
				LOGGER.error("Could not access URLStreamHandler factory field on URL class: {}");
				throw new RuntimeException(
						"Could not access URLStreamHandler factory field on URL class: {}",
						e1);
			}
		}
	}

}
