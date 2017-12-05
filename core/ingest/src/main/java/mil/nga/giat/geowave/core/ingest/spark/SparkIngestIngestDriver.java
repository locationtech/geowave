package mil.nga.giat.geowave.core.ingest.spark;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandlerFactory;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
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

import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.JavaRDD;

import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.ingest.DataAdapterProvider;
import mil.nga.giat.geowave.core.ingest.GeoWaveData;
import mil.nga.giat.geowave.core.ingest.IngestUtils;
import mil.nga.giat.geowave.core.ingest.local.AbstractLocalFileDriver;
import mil.nga.giat.geowave.core.ingest.local.LocalFileIngestPlugin;
import mil.nga.giat.geowave.core.ingest.local.LocalIngestRunData;
import mil.nga.giat.geowave.core.ingest.local.LocalInputCommandLineOptions;
import mil.nga.giat.geowave.core.ingest.operations.ConfigAWSCommand;
import mil.nga.giat.geowave.core.ingest.operations.options.IngestFormatPluginOptions;
import mil.nga.giat.geowave.core.store.AdapterToIndexMapping;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
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

public class SparkIngestIngestDriver implements
		Serializable
{

	private final static Logger LOGGER = LoggerFactory.getLogger(SparkIngestIngestDriver.class);

	public SparkIngestIngestDriver() {

	}

	public boolean runOperation(
			File configFile,
			LocalInputCommandLineOptions localInput, String inputStoreName,
			String indexList, VisibilityOptions ingestOptions,
			SparkCommandLineOptions sparkOptions, String basePath)
			throws IOException {

		final Properties configProperties = ConfigOptions.loadProperties(
				configFile,
				null);
		
		JavaSparkContext jsc = null;
		SparkSession session = null;
		int numExecutors ;
		int numCores ;
		int numPartitions;
		
		if (jsc == null) {
			String jar = "";
			try {
				jar = SparkIngestIngestDriver.class.getProtectionDomain()
						.getCodeSource().getLocation().toURI().getPath();
			} catch (final URISyntaxException e) {
				LOGGER.error(
						"Unable to set jar location in spark configuration", e);
			}

			session = SparkSession.builder().appName(sparkOptions.getAppName())
					.master(sparkOptions.getMaster())
					.config("spark.driver.host", sparkOptions.getHost())
					.config("spark.jars", jar)
					.getOrCreate();

			jsc = new JavaSparkContext(session.sparkContext());
		}

		// If input path is S3
		if (basePath.startsWith("s3://")) {
			
			String s3EndpointUrl = ConfigAWSCommand.getS3Url(configProperties);
			
			Path inpuPath = (S3Path) setUpS3Filesystem(configProperties, basePath,s3EndpointUrl);
			if (inpuPath == null || !inpuPath.isAbsolute()) {
				LOGGER.error("Error in accessing S3 Input path " + basePath);
				close(jsc,session);
				return false;
			}
			
			List<Path> inputFileList = new ArrayList<Path>();
			Files.walkFileTree(inpuPath, new SimpleFileVisitor<Path>() {
			    
			    @Override
			    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			      inputFileList.add(file);
			      return FileVisitResult.CONTINUE;
			    }
			  });
		
			int numInputFiles  = inputFileList.size();
	
			if (sparkOptions.getNumExecutors() == 1) {
				 numExecutors = (int) Math.ceil((double) numInputFiles / 8);
			}
			else{
				numExecutors = sparkOptions.getNumExecutors();
			}
			
			if (sparkOptions.getNumCores() == 1) {
				 numCores = 4;
			}
			else{
				numCores = sparkOptions.getNumCores();
			}
			
			jsc.sc().conf().set("spark.executor.instances", Integer.toString(numExecutors));
			jsc.sc().conf().set("spark.executor.cores", Integer.toString(numCores));
		    numPartitions = numExecutors * numCores * 2;
		    
			JavaRDD<URI> FileRDD = jsc.parallelize(Lists.transform(
					inputFileList, new Function<Path, URI>() {

						@Override
						public URI apply(Path arg0) {
							return arg0.toUri();
						}
					}),numPartitions);
			
			FileRDD.foreachPartition(uri -> {
			
				S3FileSystem fs = initializeS3FS(s3EndpointUrl);
				List<URI> inputFiles  = new ArrayList<URI>();
				while(uri.hasNext()){
					Path inputFile = (S3Path) fs.getPath(uri.next().toString().replaceFirst(s3EndpointUrl,""));
					inputFiles.add(inputFile.toUri());
				}
				
				processInput( configFile,localInput,inputStoreName, indexList, ingestOptions,configProperties,inputFiles.iterator());	
			});
		}
		// If input path is HDFS
		else if (basePath.startsWith("hdfs://")) {		
	
			String hdfsFSUrl = ConfigHDFSCommand.getHdfsUrl(configProperties);
			String hdfsInputPath = basePath.replaceFirst(
					"hdfs://",
					"/");

			Path inpuPath = null;
			try {

				URI uri = new URI(
						hdfsFSUrl + hdfsInputPath);
				inpuPath = Paths.get(uri);
				if (!Files.exists(inpuPath)) {
					LOGGER.error("Input path " + basePath + " does not exist");
					close(jsc,session);
					return false;
				}
			}
			catch (URISyntaxException e) {
				LOGGER.error(
						"Unable to ingest data, Inavlid HDFS Path",
						e);
				close(jsc,session);
				return false;
			}

			List<Path> inputFileList = new ArrayList<Path>();
			Files.walkFileTree(inpuPath, new SimpleFileVisitor<Path>() {
			    
			    @Override
			    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
			      inputFileList.add(file);
			      return FileVisitResult.CONTINUE;
			    }
			  });

			int numInputFiles  = inputFileList.size();
			
			if (sparkOptions.getNumExecutors() == 1) {
				 numExecutors = (int) Math.ceil((double) numInputFiles / 8);
			}
			else{
				numExecutors = sparkOptions.getNumExecutors();
			}
			
			if (sparkOptions.getNumCores() == 1) {
				 numCores = 4;
			}
			else{
				numCores = sparkOptions.getNumCores();
			}
			
			jsc.sc().conf().set("spark.executor.instances", Integer.toString(numExecutors));
			jsc.sc().conf().set("spark.executor.cores", Integer.toString(numCores));
		    numPartitions = numExecutors * numCores * 2;
		    
			JavaRDD<URI> FileRDD = jsc.parallelize(Lists.transform(
					inputFileList, new Function<Path, URI>() {

						@Override
						public URI apply(Path arg0) {
							return arg0.toUri();
						}
					}),numPartitions);
		
			FileRDD.foreachPartition(uri -> {
				
				setHdfsURLStreamHandlerFactory();
				processInput( configFile,localInput,inputStoreName, indexList, ingestOptions,configProperties,uri);	
			});
		}
		else{
			LOGGER.warn("Spark ingest support only S3 or HDFS as input location");
			close(jsc,session);
			return false;
		}
	
		if (jsc != null) {
			close(jsc,session);
		}

		return true;

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

			if (!checkIndexesAgainstProvider(
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

		DataStore dataStore = inputStoreOptions.createDataStore();
		try (LocalIngestRunData runData = new LocalIngestRunData(
				adapters,
				dataStore)) {

			List<PluginVisitor> pluginVisitors = new ArrayList<PluginVisitor>(
					localFileIngestPlugins.size());
			for (final Entry<String, LocalFileIngestPlugin<?>> localPlugin : localFileIngestPlugins.entrySet()) {
				pluginVisitors.add(new PluginVisitor(
						localPlugin.getValue(),
						localPlugin.getKey(),
						localInput.getExtensions()));
			}

			while (inputFiles.hasNext()) {
				final URL file = inputFiles.next().toURL();
				for (final PluginVisitor visitor : pluginVisitors) {
					if (visitor.supportsFile(file)) {
						processFile(
								file,
								visitor.typeName,
								visitor.localPlugin,
								runData,
								indexOptions,
								ingestOptions);
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
	}

	protected void processFile(
			final URL file,
			final String typeName,
			final LocalFileIngestPlugin<?> plugin,
			final LocalIngestRunData ingestRunData,
			final List<IndexPluginOptions> indexOptions,
			VisibilityOptions ingestOptions )
			throws IOException {

		int count = 0;
		long dbWriteMs = 0L;

		Map<ByteArrayId, IndexWriter> indexWriters = new HashMap<ByteArrayId, IndexWriter>();
		Map<ByteArrayId, AdapterToIndexMapping> adapterMappings = new HashMap<ByteArrayId, AdapterToIndexMapping>();

		LOGGER.info(String.format(
				"Beginning ingest for file: [%s]",
				FilenameUtils.getName(file.getPath())));

		// This loads up the primary indexes that are specified on the command
		// line.
		// Usually spatial or spatial-temporal
		final Map<ByteArrayId, PrimaryIndex> specifiedPrimaryIndexes = new HashMap<ByteArrayId, PrimaryIndex>();
		for (final IndexPluginOptions dimensionType : indexOptions) {
			final PrimaryIndex primaryIndex = dimensionType.createPrimaryIndex();
			if (primaryIndex == null) {
				LOGGER.error("Could not get index instance, getIndex() returned null;");
				throw new IOException(
						"Could not get index instance, getIndex() returned null");
			}
			specifiedPrimaryIndexes.put(
					primaryIndex.getId(),
					primaryIndex);
		}

		// This gets the list of required indexes from the Plugin.
		// If for some reason a GeoWaveData specifies an index that isn't
		// originally
		// in the specifiedPrimaryIndexes list, then this array is used to
		// determine
		// if the Plugin supports it. If it does, then we allow the creation of
		// the
		// index.
		final Map<ByteArrayId, PrimaryIndex> requiredIndexMap = new HashMap<ByteArrayId, PrimaryIndex>();
		final PrimaryIndex[] requiredIndices = plugin.getRequiredIndices();
		if ((requiredIndices != null) && (requiredIndices.length > 0)) {
			for (final PrimaryIndex requiredIndex : requiredIndices) {
				requiredIndexMap.put(
						requiredIndex.getId(),
						requiredIndex);
			}
		}

		// Read files until EOF from the command line.
		try (CloseableIterator<?> geowaveDataIt = plugin.toGeoWaveData(
				file,
				specifiedPrimaryIndexes.keySet(),
				ingestOptions.getVisibility())) {

			while (geowaveDataIt.hasNext()) {
				final GeoWaveData<?> geowaveData = (GeoWaveData<?>) geowaveDataIt.next();
				try {
					final WritableDataAdapter adapter = ingestRunData.getDataAdapter(geowaveData);
					if (adapter == null) {
						LOGGER.warn(String.format(
								"Adapter not found for [%s] file [%s]",
								geowaveData.getValue(),
								FilenameUtils.getName(file.getPath())));
						continue;
					}

					// Ingest the data!
					dbWriteMs += ingestData(
							geowaveData,
							adapter,
							ingestRunData,
							specifiedPrimaryIndexes,
							requiredIndexMap,
							indexWriters,
							adapterMappings);

					count++;

				}
				catch (Exception e) {
					throw new RuntimeException(
							"Interrupted ingesting GeoWaveData",
							e);
				}
			}

			LOGGER.debug(String.format(
					"Finished ingest for file: [%s]; Ingested %d items in %d seconds",
					FilenameUtils.getName(file.getPath()),
					count,
					(int) dbWriteMs / 1000));

		}
		finally {
			// Clean up index writers
			for (Entry<ByteArrayId, IndexWriter> writerEntry : indexWriters.entrySet()) {
				try {
					ingestRunData.releaseIndexWriter(
							adapterMappings.get(writerEntry.getKey()),
							writerEntry.getValue());
				}
				catch (Exception e) {
					LOGGER.warn(
							String.format(
									"Could not return index writer: [%s]",
									writerEntry.getKey()),
							e);
				}
			}

		}

	}

	private long ingestData(
			GeoWaveData<?> geowaveData,
			WritableDataAdapter adapter,
			LocalIngestRunData runData,
			Map<ByteArrayId, PrimaryIndex> specifiedPrimaryIndexes,
			Map<ByteArrayId, PrimaryIndex> requiredIndexMap,
			Map<ByteArrayId, IndexWriter> indexWriters,
			Map<ByteArrayId, AdapterToIndexMapping> adapterMappings )
			throws Exception {

		try {

			AdapterToIndexMapping mapping = adapterMappings.get(adapter.getAdapterId());

			if (mapping == null) {
				List<PrimaryIndex> indices = new ArrayList<PrimaryIndex>();
				for (final ByteArrayId indexId : geowaveData.getIndexIds()) {
					PrimaryIndex index = specifiedPrimaryIndexes.get(indexId);
					if (index == null) {
						index = requiredIndexMap.get(indexId);
						if (index == null) {
							LOGGER.warn(String.format(
									"Index '%s' not found for %s",
									indexId.getString(),
									geowaveData.getValue()));
							continue;
						}
					}
					indices.add(index);
				}
				runData.addIndices(indices);
				runData.addAdapter(adapter);

				mapping = new AdapterToIndexMapping(
						adapter.getAdapterId(),
						indices.toArray(new PrimaryIndex[indices.size()]));
				adapterMappings.put(
						mapping.getAdapterId(),
						mapping);

				// If we have the index checked out already, use that.
				if (!indexWriters.containsKey(mapping.getAdapterId())) {
					indexWriters.put(
							mapping.getAdapterId(),
							runData.getIndexWriter(mapping));
				}
			}

			// Write the data to the data store.
			IndexWriter writer = indexWriters.get(mapping.getAdapterId());

			// Time the DB write
			long hack = System.currentTimeMillis();
			writer.write(geowaveData.getValue());
			long durMs = System.currentTimeMillis() - hack;

			return durMs;
		}
		catch (RemoteException e) {
			IOException ioe = e.unwrapRemoteException(TableExistsException.class);

			if (ioe instanceof TableExistsException) {
				LOGGER.debug("Table already exists" + e.getMessage());
				return 0;
			}
			else {
				throw new RuntimeException(
						"Fatal remote error occured while trying write to an index writer.",
						e);
			}
		}
		catch (Exception e) {
			// This should really never happen, because we don't limit the
			// amount of items in the IndexWriter pool.
			LOGGER.error(
					"Fatal error occured while trying write to an index writer.",
					e);
			throw new RuntimeException(
					"Fatal error occured while trying write to an index writer.",
					e);
		}

	}

	public Path setUpS3Filesystem(
			Properties configProperties,
			String basePath,
			String s3EndpointUrl )
			throws IOException {

		Path path = null;

		try {
			FileSystem fs = FileSystems.newFileSystem(
					new URI(
							s3EndpointUrl + "/"),
					new HashMap<String, Object>(),
					Thread.currentThread().getContextClassLoader());
			String s3InputPath = basePath.replaceFirst(
					"s3://",
					"/");
			// path = (S3Path) fs.getPath(s3InputPath);
			path = fs.getPath(s3InputPath);
		}
		catch (URISyntaxException e) {
			LOGGER.error("Unable to ingest data, Inavlid S3 path");
			return null;
		}

		return path;

	}

	public S3FileSystem initializeS3FS(
			String s3EndpointUrl )
			throws URISyntaxException {

		try {
			AbstractLocalFileDriver.setURLStreamHandlerFactory();
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

		URLStreamHandlerFactory urlStreamHandlerFactory = (URLStreamHandlerFactory) factoryField.get(null);

		if (urlStreamHandlerFactory == null) {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());

		}
		else {
			try {
				factoryField.setAccessible(true);
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

	protected boolean checkIndexesAgainstProvider(
			String providerName,
			DataAdapterProvider<?> adapterProvider,
			List<IndexPluginOptions> indexOptions ) {
		boolean valid = true;
		for (IndexPluginOptions option : indexOptions) {
			if (!IngestUtils.isCompatible(
					adapterProvider,
					option)) {
				// HP Fortify "Log Forging" false positive
				// What Fortify considers "user input" comes only
				// from users with OS-level access anyway
				LOGGER.warn("Local file ingest plugin for ingest type '" + providerName
						+ "' does not support dimensionality '" + option.getType() + "'");
				valid = false;
			}
		}
		return valid;
	}

}
