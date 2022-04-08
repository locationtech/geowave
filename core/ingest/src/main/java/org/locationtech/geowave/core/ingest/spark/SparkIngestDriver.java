/**
 * Copyright (c) 2013-2022 Contributors to the Eclipse Foundation
 *
 * <p> See the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. All rights reserved. This program and the accompanying materials are made available
 * under the terms of the Apache License, Version 2.0 which accompanies this distribution and is
 * available at http://www.apache.org/licenses/LICENSE-2.0.txt
 */
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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.locationtech.geowave.core.cli.operations.config.options.ConfigOptions;
import org.locationtech.geowave.core.ingest.URLIngestUtils;
import org.locationtech.geowave.core.ingest.URLIngestUtils.URLTYPE;
import org.locationtech.geowave.core.ingest.local.LocalFileIngestCLIDriver;
import org.locationtech.geowave.core.ingest.operations.ConfigAWSCommand;
import org.locationtech.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import org.locationtech.geowave.core.store.api.DataStore;
import org.locationtech.geowave.core.store.api.DataTypeAdapter;
import org.locationtech.geowave.core.store.api.Index;
import org.locationtech.geowave.core.store.cli.CLIUtils;
import org.locationtech.geowave.core.store.cli.VisibilityOptions;
import org.locationtech.geowave.core.store.cli.store.DataStorePluginOptions;
import org.locationtech.geowave.core.store.index.IndexStore;
import org.locationtech.geowave.core.store.ingest.IngestUtils;
import org.locationtech.geowave.core.store.ingest.LocalFileIngestPlugin;
import org.locationtech.geowave.core.store.ingest.LocalIngestRunData;
import org.locationtech.geowave.core.store.ingest.LocalInputCommandLineOptions;
import org.locationtech.geowave.core.store.ingest.LocalPluginFileVisitor.PluginVisitor;
import org.locationtech.geowave.core.store.util.DataStoreUtils;
import org.locationtech.geowave.mapreduce.operations.ConfigHDFSCommand;
import org.locationtech.geowave.mapreduce.s3.GeoWaveAmazonS3Factory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.internal.Console;
import com.google.common.collect.Lists;
import com.upplication.s3fs.S3FileSystem;
import com.upplication.s3fs.S3FileSystemProvider;

public class SparkIngestDriver implements Serializable {

  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkIngestDriver.class);

  public SparkIngestDriver() {}

  public boolean runOperation(
      final File configFile,
      final LocalInputCommandLineOptions localInput,
      final String inputStoreName,
      final String indexList,
      final VisibilityOptions ingestOptions,
      final SparkCommandLineOptions sparkOptions,
      final String basePath,
      final Console console) throws IOException {

    final Properties configProperties = ConfigOptions.loadProperties(configFile);

    JavaSparkContext jsc = null;
    SparkSession session = null;
    int numExecutors;
    int numCores;
    int numPartitions;
    Path inputPath;
    String s3EndpointUrl = null;

    final boolean isS3 = basePath.startsWith("s3://");
    final boolean isHDFS =
        !isS3 && (basePath.startsWith("hdfs://") || basePath.startsWith("file:/"));

    // If input path is S3
    if (isS3) {

      s3EndpointUrl = ConfigAWSCommand.getS3Url(configProperties);
      inputPath = URLIngestUtils.setupS3FileSystem(basePath, s3EndpointUrl);
    }
    // If input path is HDFS
    else if (isHDFS) {

      final String hdfsFSUrl = ConfigHDFSCommand.getHdfsUrl(configProperties);
      inputPath = setUpHDFSFilesystem(basePath, hdfsFSUrl, basePath.startsWith("file:/"));
    } else {
      LOGGER.warn("Spark ingest support only S3 or HDFS as input location");
      return false;
    }

    if ((inputPath == null) || (!Files.exists(inputPath))) {
      LOGGER.error("Error in accessing Input path " + basePath);
      return false;
    }

    final List<Path> inputFileList = new ArrayList<>();
    Files.walkFileTree(inputPath, new SimpleFileVisitor<Path>() {

      @Override
      public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
          throws IOException {
        inputFileList.add(file);
        return FileVisitResult.CONTINUE;
      }
    });

    final int numInputFiles = inputFileList.size();

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
    numPartitions = numExecutors * numCores * 2;

    if (session == null) {
      String jar = "";
      try {
        jar =
            SparkIngestDriver.class.getProtectionDomain().getCodeSource().getLocation().toURI().getPath();
      } catch (final URISyntaxException e) {
        LOGGER.error("Unable to set jar location in spark configuration", e);
      }

      session =
          SparkSession.builder().appName(sparkOptions.getAppName()).master(
              sparkOptions.getMaster()).config("spark.driver.host", sparkOptions.getHost()).config(
                  "spark.jars",
                  jar).config("spark.executor.instances", Integer.toString(numExecutors)).config(
                      "spark.executor.cores",
                      Integer.toString(numCores)).getOrCreate();

      jsc = JavaSparkContext.fromSparkContext(session.sparkContext());
    }

    final JavaRDD<URI> fileRDD =
        jsc.parallelize(Lists.transform(inputFileList, path -> path.toUri()), numPartitions);
    if (isS3) {
      final String s3FinalEndpointUrl = s3EndpointUrl;
      fileRDD.foreachPartition(uri -> {
        final S3FileSystem fs = initializeS3FS(s3FinalEndpointUrl);
        final List<URI> inputFiles = new ArrayList<>();
        while (uri.hasNext()) {
          final Path inputFile =
              fs.getPath(uri.next().toString().replaceFirst(s3FinalEndpointUrl, ""));
          inputFiles.add(inputFile.toUri());
        }

        processInput(
            configFile,
            localInput,
            inputStoreName,
            indexList,
            ingestOptions,
            configProperties,
            inputFiles.iterator(),
            console);
      });
    } else if (isHDFS) {
      try {
        setHdfsURLStreamHandlerFactory();
      } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
          | IllegalAccessException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      fileRDD.foreachPartition(uri -> {
        processInput(
            configFile,
            localInput,
            inputStoreName,
            indexList,
            ingestOptions,
            configProperties,
            uri,
            new JCommander().getConsole());
      });
    }

    close(session);
    return true;
  }

  public void processInput(
      final File configFile,
      final LocalInputCommandLineOptions localInput,
      final String inputStoreName,
      final String indexList,
      final VisibilityOptions visibilityOptions,
      final Properties configProperties,
      final Iterator<URI> inputFiles,
      final Console console) throws IOException {

    // Based on the selected formats, select the format plugins
    final IngestFormatPluginOptions pluginFormats = new IngestFormatPluginOptions();
    // Based on the selected formats, select the format plugins
    pluginFormats.selectPlugin(localInput.getFormats());
    DataStorePluginOptions inputStoreOptions = null;
    List<Index> indices = null;

    // Ingest Plugins
    final Map<String, LocalFileIngestPlugin<?>> ingestPlugins =
        pluginFormats.createLocalIngestPlugins();

    inputStoreOptions = CLIUtils.loadStore(configProperties, inputStoreName, configFile, console);

    final IndexStore indexStore = inputStoreOptions.createIndexStore();
    indices = DataStoreUtils.loadIndices(indexStore, indexList);

    // first collect the local file ingest plugins
    final Map<String, LocalFileIngestPlugin<?>> localFileIngestPlugins = new HashMap<>();
    final List<DataTypeAdapter<?>> adapters = new ArrayList<>();
    for (final Entry<String, LocalFileIngestPlugin<?>> pluginEntry : ingestPlugins.entrySet()) {

      if (!IngestUtils.checkIndexesAgainstProvider(
          pluginEntry.getKey(),
          pluginEntry.getValue(),
          indices)) {
        continue;
      }

      localFileIngestPlugins.put(pluginEntry.getKey(), pluginEntry.getValue());

      adapters.addAll(Arrays.asList(pluginEntry.getValue().getDataAdapters()));
    }

    final LocalFileIngestCLIDriver localIngestDriver =
        new LocalFileIngestCLIDriver(
            inputStoreOptions,
            indices,
            localFileIngestPlugins,
            visibilityOptions,
            localInput,
            1);

    localIngestDriver.startExecutor();

    final DataStore dataStore = inputStoreOptions.createDataStore();
    try (LocalIngestRunData runData =
        new LocalIngestRunData(
            adapters,
            dataStore,
            visibilityOptions.getConfiguredVisibilityHandler())) {

      final List<PluginVisitor<LocalFileIngestPlugin<?>>> pluginVisitors =
          new ArrayList<>(localFileIngestPlugins.size());
      for (final Entry<String, LocalFileIngestPlugin<?>> localPlugin : localFileIngestPlugins.entrySet()) {
        pluginVisitors.add(
            new PluginVisitor<LocalFileIngestPlugin<?>>(
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

    } catch (final MalformedURLException e) {
      LOGGER.error("Error in converting input path to URL for " + inputFiles, e);
      throw new MalformedURLException("Error in converting input path to URL for " + inputFiles);
    } catch (final Exception e) {
      LOGGER.error("Error processing in processing input", e);
      throw new RuntimeException("Error processing in processing input", e);
    } finally {
      localIngestDriver.shutdownExecutor();
    }
  }

  public void close(SparkSession session) {
    if (session != null) {
      session.close();
      session = null;
    }
  }

  public Path setUpHDFSFilesystem(
      final String basePath,
      final String hdfsFSUrl,
      final boolean isLocalPath) {

    final String hdfsInputPath = basePath.replaceFirst("hdfs://", "/");

    Path path = null;
    try {

      URI uri = null;
      if (isLocalPath) {
        uri = new URI(hdfsInputPath);
      } else {
        uri = new URI(hdfsFSUrl + hdfsInputPath);
      }
      path = Paths.get(uri);
      // HP Fortify "Path Traversal" false positive
      // What Fortify considers "user input" comes only
      // from users with OS-level access anyway

    } catch (final URISyntaxException e) {
      LOGGER.error("Unable to ingest data, Inavlid HDFS Path", e);
      return null;
    }

    return path;
  }

  public S3FileSystem initializeS3FS(final String s3EndpointUrl) throws URISyntaxException {

    try {
      URLIngestUtils.setURLStreamHandlerFactory(URLTYPE.S3);
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
        | IllegalAccessException e1) {
      LOGGER.error("Error in setting up S3URLStreamHandler Factory", e1);
      throw new RuntimeException("Error in setting up S3URLStreamHandler Factory", e1);
    }

    return (S3FileSystem) new S3FileSystemProvider().getFileSystem(
        new URI(s3EndpointUrl),
        Collections.singletonMap(
            S3FileSystemProvider.AMAZON_S3_FACTORY_CLASS,
            GeoWaveAmazonS3Factory.class.getName()));
  }

  public static void setHdfsURLStreamHandlerFactory() throws NoSuchFieldException,
      SecurityException, IllegalArgumentException, IllegalAccessException {
    final Field factoryField = URL.class.getDeclaredField("factory");
    factoryField.setAccessible(true);
    // HP Fortify "Access Control" false positive
    // The need to change the accessibility here is
    // necessary, has been review and judged to be safe

    final URLStreamHandlerFactory urlStreamHandlerFactory =
        (URLStreamHandlerFactory) factoryField.get(null);

    if (urlStreamHandlerFactory == null) {
      URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
    } else {
      try {
        factoryField.setAccessible(true);
        // HP Fortify "Access Control" false positive
        // The need to change the accessibility here is
        // necessary, has been review and judged to be safe
        factoryField.set(null, new FsUrlStreamHandlerFactory());
      } catch (final IllegalAccessException e1) {
        LOGGER.error("Could not access URLStreamHandler factory field on URL class: {}", e1);
        throw new RuntimeException(
            "Could not access URLStreamHandler factory field on URL class: {}",
            e1);
      }
    }
  }
}
