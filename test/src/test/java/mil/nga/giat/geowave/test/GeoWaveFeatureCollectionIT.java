package mil.nga.giat.geowave.test;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import mil.nga.giat.geowave.adapter.vector.FeatureCollectionDataAdapter;
import mil.nga.giat.geowave.adapter.vector.util.FeatureCollectionRedistributor;
import mil.nga.giat.geowave.adapter.vector.util.RedistributeConfig;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.adapter.WritableDataAdapter;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloIndexWriter;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.SchemaException;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.junit.BeforeClass;
import org.junit.Test;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.opengis.feature.type.AttributeDescriptor;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class GeoWaveFeatureCollectionIT extends
		GeoWaveTestEnvironment
{
	private final static Logger log = Logger.getLogger(GeoWaveFeatureCollectionIT.class);

	// this is the original feature collection size before chipping
	private final static int[] pointsPerColl = new int[] {
		10,
		50,
		100
	};

	private final static int collsPerRegion = 15;
	private final static int numRegions = 15;

	// this is the chunk size for the feature collection data adapter
	private final static int[] pointsPerTile = new int[] {
		10,
		100,
		1000
	};

	// min and max width/height of a region
	private final static double[] regMinMaxSize = new double[] {
		5.0,
		10.0
	};

	// min and max width/height of a collection
	private final double[] collMinMaxSize = new double[] {
		0.5,
		1.0
	};

	private final static int numThreads = 8;

	private static SimpleFeatureType TYPE;
	private static Geometry worldBBox;

	@BeforeClass
	public static void setup()
			throws IOException {

		GeoWaveTestEnvironment.setup();

		try {
			TYPE = DataUtilities.createType(
					"TestPoint",
					"location:Point:srid=4326,dim1:Double,time:Date,index:String");
		}
		catch (final SchemaException e) {
			log.error(
					"Unable to create SimpleFeatureType",
					e);
		}

		// world sized bounding box
		worldBBox = createBoundingBox(
				new double[] {
					0.0,
					0.0
				},
				360.0,
				180.0);
	}

	@Test
	public void featureCollectionDataAdapterTest()
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException,
			SchemaException,
			InterruptedException {

		// ingest data
		log.warn("Ingesting Simple Feature Data");
		final Map<Integer, Map<String, SimpleFeature>> ingestedFeatures = ingestCollectionData();
		assertTrue(ingestedFeatures.size() == pointsPerTile.length);
		for (final int batchSize : pointsPerTile) {
			assertTrue(ingestedFeatures.get(
					batchSize).size() > 0);
		}

		// query data
		log.warn("Querying Simple Feature Data");
		Map<Integer, List<SimpleFeature>> returnedFeatures = queryCollectionData();
		assertTrue(returnedFeatures.size() == pointsPerTile.length);
		for (final int batchSize : pointsPerTile) {
			assertTrue(returnedFeatures.get(
					batchSize).size() > 0);
		}

		// compare each element
		log.warn("Comparing ingested and queried data");
		assertTrue(compare(
				ingestedFeatures,
				returnedFeatures));
		returnedFeatures = null;

		// redistribute data
		log.warn("Redistributing data");
		redistributeData();

		// run the query again
		log.warn("Querying redistributed Simple Feature Data");
		returnedFeatures = queryCollectionData();
		assertTrue(returnedFeatures.size() == pointsPerTile.length);
		for (final int batchSize : pointsPerTile) {
			assertTrue(returnedFeatures.get(
					batchSize).size() > 0);
		}

		// run the comparison again
		log.warn("Comparing ingested and queried data");
		assertTrue(compare(
				ingestedFeatures,
				returnedFeatures));
	}

	private Map<Integer, Map<String, SimpleFeature>> ingestCollectionData()
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		// ingest feature collection data multiple times with different settings
		// for batchSize
		log.info("****************************************************************************");
		log.info("                    Ingesting Feature Collection Data                       ");
		log.info("****************************************************************************");

		final StringBuilder sb = new StringBuilder();
		sb.append("*** Original Collection Sizes: [ ");
		for (final int numPts : pointsPerColl) {
			sb.append(numPts);
			sb.append(" ");
		}
		sb.append("]");
		log.info(sb.toString());

		final Map<Integer, Map<String, SimpleFeature>> ingestedFeatures = new HashMap<Integer, Map<String, SimpleFeature>>();

		for (final int batchSize : pointsPerTile) {
			log.info("*** Features per tilespace: " + batchSize);
			final BasicAccumuloOperations featureCollectionOperations = new BasicAccumuloOperations(
					zookeeper,
					accumuloInstance,
					accumuloUser,
					accumuloPassword,
					TEST_NAMESPACE + batchSize);
			final AccumuloDataStore featureCollectionDataStore = new AccumuloDataStore(
					featureCollectionOperations);
			final AccumuloIndexWriter featureCollectionWriter = new AccumuloIndexWriter(
					IndexType.SPATIAL_VECTOR.createDefaultIndex(),
					featureCollectionOperations,
					featureCollectionDataStore);
			final FeatureCollectionDataAdapter featureCollectionAdapter = new FeatureCollectionDataAdapter(
					TYPE,
					batchSize);
			final Map<String, SimpleFeature> inData = ingestData(
					featureCollectionWriter,
					featureCollectionAdapter);

			ingestedFeatures.put(
					batchSize,
					inData);

			featureCollectionWriter.close();
			log.info("");
		}

		return ingestedFeatures;
	}

	private Map<String, SimpleFeature> ingestData(
			final AccumuloIndexWriter writer,
			final WritableDataAdapter<DefaultFeatureCollection> adapter )
			throws AccumuloException,
			AccumuloSecurityException,
			IOException,
			TableNotFoundException {
		final Map<String, SimpleFeature> ingestedFeatures = new HashMap<String, SimpleFeature>();

		final Random rand = new Random(
				0);

		int numPoints = 0;
		int numColls = 0;

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
							"time",
							new Date());

					// generate the feature
					final SimpleFeature feature = builder.buildFeature(null);

					ingestedFeatures.put(
							feature.getID(),
							feature);

					coll.add(feature);
					numPoints++;
				}

				// write this feature collection to accumulo
				log.info("***     Ingesting feature collection of size " + coll.size());
				writer.write(
						adapter,
						coll);
				numColls++;
			}
		}

		log.info("*** Features Ingested: " + numPoints);
		log.info("*** Collections Ingested: " + numColls);

		return ingestedFeatures;
	}

	private Map<Integer, List<SimpleFeature>> queryCollectionData()
			throws AccumuloException,
			AccumuloSecurityException,
			SchemaException,
			IOException,
			InterruptedException,
			TableNotFoundException {

		log.info("****************************************************************************");
		log.info("                   Testing FeatureCollectionDataAdapter                     ");
		log.info("****************************************************************************");

		final Map<Integer, List<SimpleFeature>> returnedFeatures = new HashMap<Integer, List<SimpleFeature>>();

		for (final int batchSize : pointsPerTile) {
			final BasicAccumuloOperations featureCollectionOperations = new BasicAccumuloOperations(
					zookeeper,
					accumuloInstance,
					accumuloUser,
					accumuloPassword,
					TEST_NAMESPACE + batchSize);
			final AccumuloDataStore featureCollectionDataStore = new AccumuloDataStore(
					featureCollectionOperations);
			final FeatureCollectionDataAdapter featureCollectionAdapter = new FeatureCollectionDataAdapter(
					TYPE,
					batchSize);

			log.info("*** Features per tilespace: " + batchSize);

			log.info("*** World Query");
			final List<SimpleFeature> features = queryData(
					featureCollectionOperations,
					featureCollectionDataStore,
					featureCollectionAdapter,
					worldBBox);

			returnedFeatures.put(
					batchSize,
					features);
		}

		return returnedFeatures;
	}

	private List<SimpleFeature> queryData(
			final BasicAccumuloOperations featureCollectionOperations,
			final AccumuloDataStore featureCollectionDataStore,
			final FeatureCollectionDataAdapter featureCollectionAdapter,
			final Geometry geom )
			throws IOException {

		final List<SimpleFeature> returnedFeatures = new ArrayList<SimpleFeature>();

		CloseableIterator<DefaultFeatureCollection> itr = null;

		itr = featureCollectionDataStore.query(
				featureCollectionAdapter,
				IndexType.SPATIAL_VECTOR.createDefaultIndex(),
				new SpatialQuery(
						geom));

		while (itr.hasNext()) {
			final SimpleFeatureCollection featColl = itr.next();
			final SimpleFeatureIterator featItr = featColl.features();
			while (featItr.hasNext()) {
				returnedFeatures.add(featItr.next());
			}
			featItr.close();
		}
		itr.close();

		return returnedFeatures;
	}

	private boolean compare(
			final Map<Integer, Map<String, SimpleFeature>> ingestedFeatures,
			final Map<Integer, List<SimpleFeature>> returnedFeatures ) {
		for (final int batchSize : pointsPerTile) {
			final Map<String, SimpleFeature> inFeats = ingestedFeatures.get(batchSize);
			final List<SimpleFeature> retFeats = returnedFeatures.get(batchSize);

			if ((inFeats.size() != retFeats.size()) || (inFeats.size() <= 0)) {
				return false;
			}

			for (final SimpleFeature retFeat : retFeats) {
				if (inFeats.containsKey(retFeat.getID())) {
					final SimpleFeature inFeat = inFeats.get(retFeat.getID());
					if (!compareFeatures(
							inFeat,
							retFeat)) {
						return false;
					}
				}
				else {
					return false;
				}
			}
		}
		return true;
	}

	private boolean compareFeatures(
			final SimpleFeature f1,
			final SimpleFeature f2 ) {
		for (final AttributeDescriptor attrib : TYPE.getAttributeDescriptors()) {
			if ((f1.getAttribute(attrib.getName()) == null) || (f2.getAttribute(attrib.getName()) == null) || !f1.getAttribute(
					attrib.getName()).equals(
					f2.getAttribute(attrib.getName()))) {
				return false;
			}
		}
		return true;
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

			final FeatureCollectionRedistributor redistributor = new FeatureCollectionRedistributor(
					new RedistributeConfig().setZookeeperUrl(
							zookeeper).setInstanceName(
							accumuloInstance).setUserName(
							accumuloUser).setPassword(
							accumuloPassword).setTableNamespace(
							TEST_NAMESPACE + tileSize).setIndexType(
							IndexType.SPATIAL_VECTOR).setFeatureType(
							TYPE).setFeaturesPerEntry(
							tileSize).setNumThreads(
							numThreads));
			redistributor.redistribute();
			log.info("");
		}
	}

	private static Geometry createBoundingBox(
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

}