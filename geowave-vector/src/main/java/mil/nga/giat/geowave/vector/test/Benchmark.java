package mil.nga.giat.geowave.vector.test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.AccumuloIndexWriter;
import mil.nga.giat.geowave.accumulo.AccumuloRowId;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.IteratorConfig;
import mil.nga.giat.geowave.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.accumulo.query.ArrayToElementsIterator;
import mil.nga.giat.geowave.accumulo.query.ElementsToArrayIterator;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.HierarchicalNumericIndexStrategy.SubStrategy;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.index.sfc.data.BasicNumericDataset;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.index.sfc.tiered.TieredSFCIndexStrategy;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.store.index.CommonIndexModel;
import mil.nga.giat.geowave.store.index.CustomIdIndex;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.vector.adapter.FeatureCollectionDataAdapter;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;
import mil.nga.giat.geowave.vector.util.FitToIndexDefaultFeatureCollection;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.io.Files;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class Benchmark
{
	private static enum AccumuloMode {
		MINI_ACCUMULO,
		DEPLOYED_ACCUMULO
	}

	private static enum IngestType {
		FEATURE_INGEST,
		COLLECTION_INGEST
	}

	private AccumuloMode mode = AccumuloMode.MINI_ACCUMULO;
	private static final String DEFAULT_MINI_ACCUMULO_PASSWORD = "Ge0wave";

	private final static Logger log = Logger.getLogger(Benchmark.class);

	private String zookeeperUrl;
	private String instancename;
	private String username;
	private String password;

	private MiniAccumuloCluster miniAccumulo;
	private File tempDir;

	private SimpleFeatureType TYPE;

	// this is the original feature collection size before chipping
	final private int[] pointsPerColl = new int[] {
		100000,
		500000,
		1000000
	};

	final private int collsPerRegion = 15;
	final private int numRegions = 15;

	// this is the chunk size for the feature collection data adapter
	final private int[] pointsPerTile = new int[] {
		100,
		500,
		1000,
		5000,
		10000,
		50000
	};

	// min and max width/height of a region
	final double[] regMinMaxSize = new double[] {
		5.0,
		10.0
	};

	// min and max width/height of a collection
	final double[] collMinMaxSize = new double[] {
		0.5,
		1.0
	};

	final private String featureNamespace = "featureTest_raster";
	final private String featureCollectionNamespace = "featureCollectionTest_raster_";

	private Geometry worldBBox;
	private final List<Geometry> smallBBoxes = new ArrayList<Geometry>();
	private final List<Geometry> medBBoxes = new ArrayList<Geometry>();
	private final List<Geometry> largeBBoxes = new ArrayList<Geometry>();

	private final int numSmallQueries = numRegions;
	private final int numMedQueries = numRegions;
	private final int numLargeQueries = numRegions;

	List<Long> ingestRuntimes = new ArrayList<Long>();
	List<Long> worldQueryRuntimes = new ArrayList<Long>();
	List<Long> smallQueryRuntimes = new ArrayList<Long>();
	List<Long> medQueryRuntimes = new ArrayList<Long>();
	List<Long> largeQueryRuntimes = new ArrayList<Long>();

	public void accumuloInit()
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			InterruptedException,
			SchemaException {

		final String miniEnabled = System.getProperty("miniAccumulo");

		if ((miniEnabled != null) && miniEnabled.equalsIgnoreCase("false")) {
			mode = AccumuloMode.DEPLOYED_ACCUMULO;
		}

		TYPE = DataUtilities.createType(
				"TestPoint",
				"location:Point:srid=4326,dim1:Double,dim2:Double,dim3:Double,startTime:Date,stopTime:Date,index:String");

		if (mode == AccumuloMode.MINI_ACCUMULO) {
			tempDir = Files.createTempDir();
			tempDir.deleteOnExit();

			log.debug(tempDir.getAbsolutePath());

			final MiniAccumuloConfig config = new MiniAccumuloConfig(
					tempDir,
					DEFAULT_MINI_ACCUMULO_PASSWORD);
			config.setNumTservers(4);
			miniAccumulo = new MiniAccumuloCluster(
					config);
			miniAccumulo.start();
			zookeeperUrl = miniAccumulo.getZooKeepers();
			instancename = miniAccumulo.getInstanceName();
			username = "root";
			password = DEFAULT_MINI_ACCUMULO_PASSWORD;
		}
		else if (mode == AccumuloMode.DEPLOYED_ACCUMULO) {
			zookeeperUrl = System.getProperty("zookeeperUrl");
			instancename = System.getProperty("instance");
			username = System.getProperty("username");
			password = System.getProperty("password");
		}
		else {
			// intentionally left blank
		}
	}

	public void runBenchmarks()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			IOException,
			InterruptedException,
			TableNotFoundException {

		ingestFeatureData(false);
		//ingestCollectionData(true);
		//saveIngestRuntimes();

		redistributeData();

		int numIters = 10;
		for (int i = 0; i < numIters; i++) {
			log.info("************************************************");
			log.info("***  RUNNING BENCHMARK ITERATION  " + (i + 1) + " OF " + numIters);
			log.info("************************************************");

			simpleFeatureTest();
			featureCollectionTest();
		}

		saveQueryRuntimes();
	}

	private void saveIngestRuntimes()
			throws FileNotFoundException,
			UnsupportedEncodingException {
		// write ingest runtimes
		final PrintWriter writer = new PrintWriter(
				"ingestRuntimes.txt",
				"UTF-8");

		for (final Long runtime : ingestRuntimes) {
			writer.println(Long.toString(runtime));
		}
		writer.close();
	}

	private void saveQueryRuntimes()
			throws FileNotFoundException,
			UnsupportedEncodingException {

		// write the world query runtimes
		PrintWriter writer = new PrintWriter(
				"worldQueryRuntimes.txt",
				"UTF-8");

		for (final Long runtime : worldQueryRuntimes) {
			writer.println(Long.toString(runtime));
		}
		writer.close();

		// write the small query runtimes
		writer = new PrintWriter(
				"smallQueryRuntimes.txt",
				"UTF-8");

		for (final Long runtime : smallQueryRuntimes) {
			writer.println(Long.toString(runtime));
		}
		writer.close();

		// write the medium query runtimes
		writer = new PrintWriter(
				"medQueryRuntimes.txt",
				"UTF-8");

		for (final Long runtime : medQueryRuntimes) {
			writer.println(Long.toString(runtime));
		}
		writer.close();

		// write the large query runtimes
		writer = new PrintWriter(
				"largeQueryRuntimes.txt",
				"UTF-8");

		for (final Long runtime : largeQueryRuntimes) {
			writer.println(Long.toString(runtime));
		}
		writer.close();
	}

	private void ingestFeatureData(
			final boolean write )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		log.info("****************************************************************************");
		log.info("                         Ingesting Feature Data                             ");
		log.info("****************************************************************************");

		// initialize the feature data adapter
		final BasicAccumuloOperations featureOperations = new BasicAccumuloOperations(
				zookeeperUrl,
				instancename,
				username,
				password,
				featureNamespace);
		final AccumuloDataStore featureDataStore = new AccumuloDataStore(
				featureOperations);
		final AccumuloIndexWriter featureWriter = new AccumuloIndexWriter(
				IndexType.SPATIAL_RASTER.createDefaultIndex(),
				featureOperations,
				featureDataStore);
		final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
				TYPE);
		if (write) {
			featureOperations.deleteAll();
		}
		ingestData(
				IngestType.FEATURE_INGEST,
				featureWriter,
				featureAdapter,
				write);
		featureWriter.close();
		log.info("");
	}

	private void ingestCollectionData(
			final boolean write )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		// ingest feature collection data multiple times with different settings
		// for batchSize
		log.info("****************************************************************************");
		log.info("                    Ingesting Feature Collection Data                       ");
		log.info("****************************************************************************");

		String msg = "*** Original Collection Sizes: [ ";
		for (final int numPts : pointsPerColl) {
			msg += numPts + " ";
		}
		msg += "]";
		log.info(msg);

		for (final int batchSize : pointsPerTile) {
			log.info("*** Features per tilespace: " + batchSize);
			final BasicAccumuloOperations featureCollectionOperations = new BasicAccumuloOperations(
					zookeeperUrl,
					instancename,
					username,
					password,
					featureCollectionNamespace + batchSize);
			final AccumuloDataStore featureCollectionDataStore = new AccumuloDataStore(
					featureCollectionOperations);
			final AccumuloIndexWriter featureCollectionWriter = new AccumuloIndexWriter(
					IndexType.SPATIAL_RASTER.createDefaultIndex(),
					featureCollectionOperations,
					featureCollectionDataStore);
			final FeatureCollectionDataAdapter featureCollectionAdapter = new FeatureCollectionDataAdapter(
					TYPE,
					batchSize);
			if (write) {
				featureCollectionOperations.deleteAll();
			}
			ingestData(
					IngestType.COLLECTION_INGEST,
					featureCollectionWriter,
					featureCollectionAdapter,
					write);
			featureCollectionWriter.close();
			log.info("");
		}
	}

	private void ingestData(
			final IngestType ingestType,
			final AccumuloIndexWriter writer,
			final WritableDataAdapter adapter,
			final boolean write )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final Random rand = new Random(
				0);

		smallBBoxes.clear();
		medBBoxes.clear();
		largeBBoxes.clear();

		int numPoints = 0;
		int numColls = 0;
		long runtime = 0;

		final GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();
		final SimpleFeatureBuilder builder = new SimpleFeatureBuilder(
				TYPE);

		for (int i = 0; i < numRegions; i++) {

			log.info("*** Generating region " + (i + 1) + " of " + numRegions);

			// width/height in degrees [range from 5 -10 degrees]
			final double[] regDims = new double[2];
			regDims[0] = (rand.nextDouble() * (regMinMaxSize[1] - regMinMaxSize[0])) + regMinMaxSize[0]; // width
			regDims[1] = (rand.nextDouble() * (regMinMaxSize[1] - regMinMaxSize[0])) + regMinMaxSize[0]; // height

			// pick the region center (lon/lat)
			final double[] regCenter = new double[2];
			regCenter[0] = ((rand.nextDouble() * 2.0) - 1.0) * (180.0 - regMinMaxSize[1]); // lon
			regCenter[1] = ((rand.nextDouble() * 2.0) - 1.0) * (90.0 - regMinMaxSize[1]); // lat

			// generate collections within the region
			for (int j = 0; j < collsPerRegion; j++) {

				log.info("***   Generating collection " + (j + 1) + " of " + collsPerRegion);

				// create a new collection
				final DefaultFeatureCollection coll = new DefaultFeatureCollection(
						null,
						TYPE);

				// width/height in degrees [range from 0.5 - 1.0 degrees]
				final double[] collDims = new double[2];
				collDims[0] = (rand.nextDouble() * (collMinMaxSize[1] - collMinMaxSize[0])) + collMinMaxSize[0]; // width
				collDims[1] = (rand.nextDouble() * (collMinMaxSize[1] - collMinMaxSize[0])) + collMinMaxSize[0]; // height

				// pick the collection center (lon/lat)
				final double[] collCenter = new double[2];
				collCenter[0] = ((rand.nextGaussian() * regDims[0]) / 2.0) + regCenter[0]; // lon
				collCenter[1] = ((rand.nextGaussian() * regDims[1]) / 2.0) + regCenter[1]; // lat

				// generate each of the features within the collection
				for (int k = 0; k < pointsPerColl[j % pointsPerColl.length]; k++) {

					// generate a lon/lat coordinate within the bounds of the
					// current collection
					final double[] coord = new double[2];
					coord[0] = (rand.nextDouble() * collDims[0]) + (collCenter[0] - (collDims[0] / 2.0)); // lon
					coord[1] = (rand.nextDouble() * collDims[1]) + (collCenter[1] - (collDims[1] / 2.0)); // lat

					coord[0] = Math.min(
							Math.max(
									-180.0,
									coord[0]),
							180.0);
					coord[1] = Math.min(
							Math.max(
									-90.0,
									coord[1]),
							90.0);

					final Point point = geometryFactory.createPoint(new Coordinate(
							coord[0],
							coord[1]));

					builder.set(
							"location",
							point);
					builder.set(
							"index",
							"[" + i + "," + j + "," + k + "]");
					builder.set(
							"dim1",
							rand.nextDouble());
					builder.set(
							"dim2",
							rand.nextDouble());
					builder.set(
							"dim3",
							rand.nextDouble());

					// generate the feature
					final SimpleFeature feature = builder.buildFeature(null);
					if (ingestType == IngestType.FEATURE_INGEST) {
						if ((k % (pointsPerColl[j % pointsPerColl.length] / 10)) == 0) {
							log.info("***     Ingesting feature " + (k + 1) + " of collection of size " + pointsPerColl[j % pointsPerColl.length]);
						}
						final long start = new Date().getTime();
						if (write) {
							writer.write(
									adapter,
									feature);
						}
						final long finish = new Date().getTime();
						runtime += (finish - start);
						numPoints++;
					}
					else {
						coll.add(feature);
						numPoints++;
					}
				}

				// write this feature collection to accumulo
				if (ingestType == IngestType.COLLECTION_INGEST) {
					log.info("***     Ingesting feature collection of size " + coll.size());
					final long start = new Date().getTime();
					if (write) {
						writer.write(
								adapter,
								coll);
					}
					final long finish = new Date().getTime();
					runtime += (finish - start);
					numColls++;
				}
			}

			// Generate a few queries for this region
			// town sized bounding box
			smallBBoxes.add(createBoundingBox(
					regCenter,
					0.05,
					0.05));

			// city sized bounding box
			medBBoxes.add(createBoundingBox(
					regCenter,
					1.0,
					1.0));

			// region sized bounding box
			largeBBoxes.add(createBoundingBox(
					regCenter,
					regDims[0],
					regDims[1]));
		}

		// world sized bounding box
		worldBBox = createBoundingBox(
				new double[] {
					0.0,
					0.0
				},
				360.0,
				180.0);

		// save the ingest runtime to the output list
		ingestRuntimes.add(runtime);

		log.info("*** Ingest runtime: " + runtime + " ms");
		log.info("*** Features Ingested: " + numPoints);
		if (ingestType == IngestType.COLLECTION_INGEST) {
			log.info("*** Collections Ingested: " + numColls);
		}
	}

	private Geometry createBoundingBox(
			final double[] centroid,
			final double width,
			final double height ) {

		final double north = centroid[1] + (height / 2.0);
		final double south = centroid[1] - (height / 2.0);
		final double east = centroid[0] + (width / 2.0);
		final double west = centroid[0] - (width / 2.0);

		final Coordinate[] coordArray = new Coordinate[5];
		coordArray[0] = new Coordinate(
				west,
				south);
		coordArray[1] = new Coordinate(
				east,
				south);
		coordArray[2] = new Coordinate(
				east,
				north);
		coordArray[3] = new Coordinate(
				west,
				north);
		coordArray[4] = new Coordinate(
				west,
				south);

		return new GeometryFactory().createPolygon(coordArray);
	}

	private void redistributeData()
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		// ingest feature collection data multiple times with different settings
		// for batchSize
		log.info("****************************************************************************");
		log.info("                  Redistributing Feature Collection Data                    ");
		log.info("****************************************************************************");

		for (final int tileSize : pointsPerTile) {
			log.info("*** Features per tilespace: " + tileSize);
			final BasicAccumuloOperations featureCollectionOperations = new BasicAccumuloOperations(
					zookeeperUrl,
					instancename,
					username,
					password,
					featureCollectionNamespace + tileSize);
			final FeatureCollectionDataAdapter featureCollectionAdapter = new FeatureCollectionDataAdapter(
					TYPE,
					tileSize);
			redistribute(
					tileSize,
					featureCollectionOperations,
					featureCollectionAdapter);
			log.info("");
		}
	}

	// this is used to ensure that the iterators aren't added when we ingest
	private static class RawFeatureCollectionDataAdapter extends
			FeatureCollectionDataAdapter
	{
		public RawFeatureCollectionDataAdapter(
				final SimpleFeatureType type,
				final int featuresPerEntry ) {
			super(
					type,
					featuresPerEntry);
		}

		@Override
		public IteratorConfig[] getAttachedIteratorConfig(
				final Index index ) {
			return null;
		}
	}

	private void removeIterators(
			final String tablename,
			final Connector connector )
			throws AccumuloSecurityException,
			AccumuloException,
			TableNotFoundException {
		connector.tableOperations().removeIterator(
				tablename,
				new IteratorSetting(
						FeatureCollectionDataAdapter.ARRAY_TO_ELEMENTS_PRIORITY,
						ArrayToElementsIterator.class).getName(),
				EnumSet.of(IteratorScope.scan));

		connector.tableOperations().removeIterator(
				tablename,
				new IteratorSetting(
						FeatureCollectionDataAdapter.ELEMENTS_TO_ARRAY_PRIORITY,
						ElementsToArrayIterator.class).getName(),
				EnumSet.of(IteratorScope.scan));
	}

	private void attachIterators(
			final CommonIndexModel indexModel,
			final String tablename,
			final Connector connector )
			throws AccumuloSecurityException,
			AccumuloException,
			TableNotFoundException {
		final String modelString = ByteArrayUtils.byteArrayToString(PersistenceUtils.toBinary(indexModel));
		final IteratorSetting decompSetting = new IteratorSetting(
				FeatureCollectionDataAdapter.ARRAY_TO_ELEMENTS_PRIORITY,
				ArrayToElementsIterator.class);
		decompSetting.addOption(
				ArrayToElementsIterator.MODEL,
				modelString);
		decompSetting.addOption(
				TransformingIterator.MAX_BUFFER_SIZE_OPT,
				Integer.toString(512000000));
		connector.tableOperations().attachIterator(
				tablename,
				decompSetting,
				EnumSet.of(IteratorScope.scan));

		final IteratorSetting builderSetting = new IteratorSetting(
				FeatureCollectionDataAdapter.ELEMENTS_TO_ARRAY_PRIORITY,
				ElementsToArrayIterator.class);
		builderSetting.addOption(
				ElementsToArrayIterator.MODEL,
				modelString);
		builderSetting.addOption(
				TransformingIterator.MAX_BUFFER_SIZE_OPT,
				Integer.toString(512000000));
		connector.tableOperations().attachIterator(
				tablename,
				builderSetting,
				EnumSet.of(IteratorScope.scan));
	}

	private boolean run = true;

	private void redistribute(
			final int tileSize,
			final BasicAccumuloOperations operations,
			final FeatureCollectionDataAdapter adapter )
			throws IOException,
			AccumuloSecurityException,
			AccumuloException,
			TableNotFoundException {

		final Index index = IndexType.SPATIAL_RASTER.createDefaultIndex();
		final TieredSFCIndexStrategy tieredStrat = (TieredSFCIndexStrategy) index.getIndexStrategy();

		final String tablename = AccumuloUtils.getQualifiedTableName(
				featureCollectionNamespace + tileSize,
				StringUtils.stringFromBinary(index.getId().getBytes()));

		// first, detach the transforming iterators
		removeIterators(
				tablename,
				operations.getConnector());

		final long startTime = new Date().getTime();

		final SubStrategy[] subStrategies = tieredStrat.getSubStrategies();

		// iterate over each tier
		for (int i = 0; i < subStrategies.length; i++) {

			totalNumOverflow = 0;
			totalNumOcuppiedSubTiles = 0;
			totalNumCollsProcessed = 0;

			log.info("***   Processing Tier " + (i + 1) + " of " + subStrategies.length);

			final SubStrategy subStrat = subStrategies[i];

			// create an index for this substrategy
			final CustomIdIndex customIndex = new CustomIdIndex(
					subStrat.getIndexStrategy(),
					index.getIndexModel(),
					index.getDimensionalityType(),
					index.getDataType(),
					index.getId());

			final AccumuloConstraintsQuery q = new AccumuloConstraintsQuery(
					Arrays.asList(new ByteArrayId[] {
						adapter.getAdapterId()
					}),
					customIndex,
					new SpatialQuery(
							worldBBox).getIndexConstraints(customIndex.getIndexStrategy()),
					null);
			q.setQueryFiltersEnabled(false);

			// query at the specified index
			final CloseableIterator<DefaultFeatureCollection> itr = (CloseableIterator<DefaultFeatureCollection>) q.query(
					operations,
					new MemoryAdapterStore(
							new DataAdapter[] {
								adapter
							}),
					null);

			// TODO:
			final int numThreads = 32;
			final ExecutorService executor = Executors.newFixedThreadPool(numThreads);
			final int subStratIdx = i;

			run = true;
			final ArrayBlockingQueue<DefaultFeatureCollection> queue = new ArrayBlockingQueue<DefaultFeatureCollection>(
					numThreads * 2);

			// create numThreads consumers
			for (int thread = 0; thread < numThreads; thread++) {
				executor.execute(new Runnable() {
					@Override
					public void run() {
						featCollConsumer(
								queue,
								subStratIdx,
								tileSize);
					}
				});
			}

			// iterate over each collection
			while (itr.hasNext()) {
				// create a new thread for each feature collection
				final DefaultFeatureCollection coll = itr.next();
				try {
					queue.put(coll);
				}
				catch (final InterruptedException e) {
					e.printStackTrace();
				}
			}
			run = false;

			try {
				executor.shutdown();
				executor.awaitTermination(
						Long.MAX_VALUE,
						TimeUnit.DAYS);
			}
			catch (final InterruptedException e) {
				e.printStackTrace();
			}

			itr.close();

			log.info("***     Colls. Processed: " + totalNumCollsProcessed);
			log.info("***     Overflowed Tiles: " + totalNumOverflow);
			log.info("***     Occupied SubTiles: " + totalNumOcuppiedSubTiles);
		}

		final long stopTime = new Date().getTime();

		// finally, attach the transforming iterators
		attachIterators(
				index.getIndexModel(),
				tablename,
				operations.getConnector());

		log.info("*** Runtime: " + (stopTime - startTime) + " ms");
	}

	private MultiDimensionalNumericData getSlimBounds(
			MultiDimensionalNumericData bounds ) {
		// Ideally: smallest dimension range / (4*max bins per dimension {i.e.
		// 2^31})
		double epsilon = 180.0 / (4.0 * Math.pow(
				2.0,
				31.0));

		NumericData[] slimRanges = new NumericData[2];
		slimRanges[0] = new NumericRange(
				bounds.getDataPerDimension()[0].getMin() + epsilon,
				bounds.getDataPerDimension()[0].getMax() - epsilon);
		slimRanges[1] = new NumericRange(
				bounds.getDataPerDimension()[1].getMin() + epsilon,
				bounds.getDataPerDimension()[1].getMax() - epsilon);

		return new BasicNumericDataset(
				slimRanges);
	}

	private int totalNumOverflow = 0;
	private int totalNumOcuppiedSubTiles = 0;
	private int totalNumCollsProcessed = 0;

	private void featCollConsumer(
			final ArrayBlockingQueue<DefaultFeatureCollection> queue,
			final int subStratIdx,
			final int tileSize ) {

		int numOverflow = 0;
		int numOcuppiedSubTiles = 0;
		int numCollsProcessed = 0;

		final Instance inst = new ZooKeeperInstance(
				instancename,
				zookeeperUrl);

		Connector connector = null;
		try {
			connector = inst.getConnector(
					username,
					password);
		}
		catch (AccumuloException | AccumuloSecurityException e1) {
			e1.printStackTrace();
		}

		final BasicAccumuloOperations featureCollectionOperations = new BasicAccumuloOperations(
				connector,
				featureCollectionNamespace + tileSize);

		final AccumuloDataStore featureCollectionDataStore = new AccumuloDataStore(
				featureCollectionOperations);
		final AccumuloIndexWriter featureCollectionWriter = new AccumuloIndexWriter(
				IndexType.SPATIAL_RASTER.createDefaultIndex(),
				featureCollectionOperations,
				featureCollectionDataStore);

		final Index index = IndexType.SPATIAL_RASTER.createDefaultIndex();
		final TieredSFCIndexStrategy tieredStrat = (TieredSFCIndexStrategy) index.getIndexStrategy();

		final FeatureDataAdapter featAdapter = new FeatureDataAdapter(
				TYPE);

		final RawFeatureCollectionDataAdapter featureCollectionAdapter = new RawFeatureCollectionDataAdapter(
				TYPE,
				tileSize);

		final SubStrategy[] subStrategies = tieredStrat.getSubStrategies();

		while ((queue.size() > 0) || run) {

			DefaultFeatureCollection featColl = null;
			try {
				featColl = queue.poll(
						100,
						TimeUnit.MILLISECONDS);
			}
			catch (final InterruptedException e1) {
				e1.printStackTrace();
			}

			if (featColl != null) {

				numCollsProcessed++;

				// use the first feature to determine the index insertion id
				final MultiDimensionalNumericData bounds = featAdapter.encode(
						featColl.features().next(),
						index.getIndexModel()).getNumericData(
						index.getIndexModel().getDimensions());

				final List<ByteArrayId> ids = subStrategies[subStratIdx].getIndexStrategy().getInsertionIds(
						bounds);

				// TODO: This will need to be modified to support polygons
				// geometries
				if (ids.size() > 1) {
					log.warn("Multiple row ids returned for this entry?!");
				}

				final ByteArrayId id = ids.get(0);

				boolean subTilesOccupied = false;
				final boolean tilespaceFull = featColl.size() > tileSize;

				// for each tier below the current one, make sure there that
				// none of this tile's subtiles are populated
				if (!tilespaceFull) {
					for (int j = subStratIdx + 1; j < subStrategies.length; j++) {

						// create an index for this substrategy
						final CustomIdIndex subIndex = new CustomIdIndex(
								subStrategies[j].getIndexStrategy(),
								index.getIndexModel(),
								index.getDimensionalityType(),
								index.getDataType(),
								index.getId());

						// build the subtier query
						final AccumuloConstraintsQuery subQuery = new AccumuloConstraintsQuery(
								Arrays.asList(new ByteArrayId[] {
									featureCollectionAdapter.getAdapterId()
								}),
								subIndex,
								getSlimBounds(subStrategies[subStratIdx].getIndexStrategy().getRangeForId(
										id)),
								null);
						subQuery.setQueryFiltersEnabled(false);

						// query at the specified subtier
						final CloseableIterator<DefaultFeatureCollection> subItr = (CloseableIterator<DefaultFeatureCollection>) subQuery.query(
								featureCollectionOperations,
								new MemoryAdapterStore(
										new DataAdapter[] {
											featureCollectionAdapter
										}),
								null);

						// if there are any points, we need to set a flag to
						// move this collection to the next lowest tier
						if (subItr.hasNext()) {
							subTilesOccupied = true;
							numOcuppiedSubTiles++;
							try {
								subItr.close();
							}
							catch (final IOException e) {
								e.printStackTrace();
							}
							break;
						}
					}
				}
				else {
					numOverflow++;
				}

				// if the collection size is greater than tilesize
				// or there are points in the subtiles below this tile
				if (tilespaceFull || subTilesOccupied) {

					// build a row id for deletion
					final AccumuloRowId rowId = new AccumuloRowId(
							id.getBytes(),
							new byte[] {},
							featureCollectionAdapter.getAdapterId().getBytes(),
							0);

					// delete this tile
					boolean result = featureCollectionOperations.delete(
							StringUtils.stringFromBinary(index.getId().getBytes()),
							new ByteArrayId(
									rowId.getRowId()),
							featureCollectionAdapter.getAdapterId().getString(),
							null);

					// if deletion failed, wait and try again a few times
					if (!result) {
						log.warn("Unable to delete row.  Trying again...");
						int attempts = 5;
						for (int attempt = 0; attempt < attempts; attempt++) {

							try {
								Thread.sleep(500);
							}
							catch (InterruptedException e) {
								log.error(
										"Sleep interrupted!",
										e);
							}

							result = featureCollectionOperations.delete(
									StringUtils.stringFromBinary(index.getId().getBytes()),
									new ByteArrayId(
											rowId.getRowId()),
									featureCollectionAdapter.getAdapterId().getString(),
									null);
						}
						if (!result)
							log.error("After " + attempts + " attempts, Index Id: [" + rowId.getIndexId().toString() + "] was NOT deleted successfully!");
						else
							log.info("The row was deleted successfully!");
					}

					// if the deletion was unsuccessful, don't re-ingest
					// anything
					if (result) {
						if (subTilesOccupied) {
							// re-ingest the data if the tiers below this one
							// are occupied. send the tier information along
							// with our data
							featureCollectionWriter.write(
									featureCollectionAdapter,
									new FitToIndexDefaultFeatureCollection(
											featColl,
											id,
											subStratIdx));
						}
						else {
							// re-ingest because the tile has overflowed
							featureCollectionWriter.write(
									featureCollectionAdapter,
									featColl);
						}
					}
				}
			}
		}

		synchronized (this) {
			totalNumOverflow += numOverflow;
			totalNumOcuppiedSubTiles += numOcuppiedSubTiles;
			totalNumCollsProcessed += numCollsProcessed;
		}
		featureCollectionWriter.close();
	}

	private void simpleFeatureTest()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			IOException,
			InterruptedException {

		log.info("****************************************************************************");
		log.info("                       Testing FeatureDataAdapter                           ");
		log.info("****************************************************************************");

		final BasicAccumuloOperations featureOperations = new BasicAccumuloOperations(
				zookeeperUrl,
				instancename,
				username,
				password,
				featureNamespace);
		final AccumuloDataStore featureDataStore = new AccumuloDataStore(
				featureOperations);
		final FeatureDataAdapter featureAdapter = new FeatureDataAdapter(
				TYPE);

		long runtime = 0;

		log.info("*** World Query");
		runtime = simpleFeatureQuery(
				featureDataStore,
				featureAdapter,
				worldBBox);

		worldQueryRuntimes.add(runtime);

		log.info("*** Small Queries");
		for (int i = 0; (i < numSmallQueries) && (i < smallBBoxes.size()); i++) {
			log.info("***   Query " + (i + 1));
			runtime = simpleFeatureQuery(
					featureDataStore,
					featureAdapter,
					smallBBoxes.get(i));

			smallQueryRuntimes.add(runtime);
		}

		log.info("*** Medium Queries");
		for (int i = 0; (i < numMedQueries) && (i < medBBoxes.size()); i++) {
			log.info("***   Query " + (i + 1));
			runtime = simpleFeatureQuery(
					featureDataStore,
					featureAdapter,
					medBBoxes.get(i));

			medQueryRuntimes.add(runtime);
		}

		log.info("*** Large Queries");
		for (int i = 0; (i < numLargeQueries) && (i < largeBBoxes.size()); i++) {
			log.info("***   Query " + (i + 1));
			runtime = simpleFeatureQuery(
					featureDataStore,
					featureAdapter,
					largeBBoxes.get(i));

			largeQueryRuntimes.add(runtime);
		}
	}

	private long simpleFeatureQuery(
			final AccumuloDataStore featureDataStore,
			final FeatureDataAdapter featureAdapter,
			final Geometry geom )
			throws IOException {
		final long queryStart = new Date().getTime();
		final CloseableIterator<SimpleFeature> itr = featureDataStore.query(
				featureAdapter,
				IndexType.SPATIAL_RASTER.createDefaultIndex(),
				new SpatialQuery(
						geom));

		int i = 0;
		while (itr.hasNext()) {
			itr.next();
			i++;
		}
		itr.close();
		final long queryStop = new Date().getTime();

		log.info("***     Query Runtime: " + (queryStop - queryStart) + " ms");
		log.info("***     Features: " + i);

		return (queryStop - queryStart);
	}

	private void featureCollectionTest()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			IOException,
			InterruptedException {

		log.info("****************************************************************************");
		log.info("                   Testing FeatureCollectionDataAdapter                     ");
		log.info("****************************************************************************");

		for (final int batchSize : pointsPerTile) {
			final BasicAccumuloOperations featureCollectionOperations = new BasicAccumuloOperations(
					zookeeperUrl,
					instancename,
					username,
					password,
					featureCollectionNamespace + batchSize);
			final AccumuloDataStore featureCollectionDataStore = new AccumuloDataStore(
					featureCollectionOperations);
			final FeatureCollectionDataAdapter featureCollectionAdapter = new FeatureCollectionDataAdapter(
					TYPE,
					batchSize);

			log.info("*** Features per tilespace: " + batchSize);

			long runtime = 0;

			log.info("*** World Query");
			runtime = featureCollectionQuery(
					featureCollectionDataStore,
					featureCollectionAdapter,
					worldBBox);

			worldQueryRuntimes.add(runtime);

			log.info("*** Small Queries");
			for (int i = 0; (i < numSmallQueries) && (i < smallBBoxes.size()); i++) {
				log.info("***   Query " + (i + 1));
				runtime = featureCollectionQuery(
						featureCollectionDataStore,
						featureCollectionAdapter,
						smallBBoxes.get(i));

				smallQueryRuntimes.add(runtime);
			}

			log.info("*** Medium Queries");
			for (int i = 0; (i < numMedQueries) && (i < medBBoxes.size()); i++) {
				log.info("***   Query " + (i + 1));
				runtime = featureCollectionQuery(
						featureCollectionDataStore,
						featureCollectionAdapter,
						medBBoxes.get(i));

				medQueryRuntimes.add(runtime);
			}

			log.info("*** Large Queries");
			for (int i = 0; (i < numLargeQueries) && (i < largeBBoxes.size()); i++) {
				log.info("***   Query " + (i + 1));
				runtime = featureCollectionQuery(
						featureCollectionDataStore,
						featureCollectionAdapter,
						largeBBoxes.get(i));

				largeQueryRuntimes.add(runtime);
			}
		}
	}

	private long featureCollectionQuery(
			final AccumuloDataStore featureCollectionDataStore,
			final FeatureCollectionDataAdapter featureCollectionAdapter,
			final Geometry geom )
			throws IOException {
		final long queryStart = new Date().getTime();
		final CloseableIterator<DefaultFeatureCollection> itr = featureCollectionDataStore.query(
				featureCollectionAdapter,
				IndexType.SPATIAL_RASTER.createDefaultIndex(),
				new SpatialQuery(
						geom));

		int i = 0;
		int j = 0;
		while (itr.hasNext()) {
			final SimpleFeatureCollection featColl = itr.next();
			j++;
			i += featColl.size();
		}
		itr.close();

		final long queryStop = new Date().getTime();
		log.info("***     Query Runtime: " + (queryStop - queryStart) + " ms");
		log.info("***     Features: " + i);
		log.info("***     Collections: " + j);

		return (queryStop - queryStart);
	}

	public static void main(
			final String[] args ) {
		final Benchmark tb = new Benchmark();
		try {
			tb.accumuloInit();
			tb.runBenchmarks();
		}
		catch (final Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
