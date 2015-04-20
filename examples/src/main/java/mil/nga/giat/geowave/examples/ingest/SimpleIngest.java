package mil.nga.giat.geowave.examples.ingest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.vividsolutions.jts.geom.Point;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.IndexType;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.index.Index;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.log4j.Logger;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

public class SimpleIngest
{

	static Logger log = Logger.getLogger(SimpleIngest.class);
	public static final String FEATURE_NAME = "GridPoint";

	public static void main(
			final String[] args ) {
		if (args.length != 5) {
			log.error("Invalid arguments, expected: zookeepers, accumuloInstance, accumuloUser, accumuloPass, geowaveNamespace");
			System.exit(1);
		}

		final SimpleIngest si = new SimpleIngest();

		try {
			final BasicAccumuloOperations bao = si.getAccumuloOperationsInstance(
					args[0],
					args[1],
					args[2],
					args[3],
					args[4]);

			final DataStore geowaveDataStore = si.getGeowaveDataStore(bao);
			si.generateGrid(geowaveDataStore);
		}
		catch (final Exception e) {
			log.error(
					"Error creating BasicAccumuloOperations",
					e);
			System.exit(1);
		}

		System.out.println("Finished ingesting data to namespace: " + args[4] + " at accumulo instance: " + args[1]);

	}

	public static List<SimpleFeature> getGriddedFeatures(
			SimpleFeatureBuilder pointBuilder,
			int firstFeatureId ) {

		int featureId = firstFeatureId;
		List<SimpleFeature> feats = new ArrayList<>();
		for (int longitude = -180; longitude <= 180; longitude += 5) {
			for (int latitude = -90; latitude <= 90; latitude += 5) {
				pointBuilder.set(
						"geometry",
						GeometryUtils.GEOMETRY_FACTORY.createPoint(new Coordinate(
								longitude,
								latitude)));
				pointBuilder.set(
						"TimeStamp",
						new Date());
				pointBuilder.set(
						"Latitude",
						latitude);
				pointBuilder.set(
						"Longitude",
						longitude);
				// Note since trajectoryID and comment are marked as nillable we
				// don't need to set them (they default ot null).

				final SimpleFeature sft = pointBuilder.buildFeature(String.valueOf(featureId));
				feats.add(sft);
				featureId++;
			}
		}
		return feats;
	}

	protected void generateGrid(
			final DataStore geowaveDataStore ) {

		// In order to store data we need to determine the type of data store
		final SimpleFeatureType point = createPointFeatureType();

		// This a factory class that builds simple feature objects based on the
		// type passed
		final SimpleFeatureBuilder pointBuilder = new SimpleFeatureBuilder(
				point);

		// This is an adapter, that is needed to describe how to persist the
		// data type passed
		final FeatureDataAdapter adapter = createDataAdapter(point);

		// This describes how to index the data
		final Index index = createSpatialIndex();

		// features require a featureID - this should be unqiue as it's a
		// foreign key on the feature
		// (i.e. sending in a new feature with the same feature id will
		// overwrite the existing feature)
		int featureId = 0;

		// build a grid of points across the globe at each whole
		// lattitude/longitude intersection

		// this loads the data to geowave
		// in practice you probably wouldn't do this in a tight loop -
		// but use a the SimpleIngestIndexWriter, producer/consumer, mapreduce,
		// or some other pattern. But if it matters depends also on the amount
		// of data
		// you are ingesting.

		// Note that the ingest method can take a feature, or an
		// interator on a collection of SimpleFeatures. The latter
		// is the preferred mechanism for non-trivial data sets.
		for (SimpleFeature sft : getGriddedFeatures(
				pointBuilder,
				0)) {
			geowaveDataStore.ingest(
					adapter,
					index,
					sft);
		}
	}

	/***
	 * DataStore is essentially the controller that take the accumulo
	 * information, geowave configuration, and data type, and inserts/queries
	 * from accumulo
	 * 
	 * @param instance
	 *            Accumulo instance configuration
	 * @return DataStore object for the particular accumulo instance
	 */
	protected DataStore getGeowaveDataStore(
			final BasicAccumuloOperations instance ) {

		// GeoWave persists both the index and data adapter to the same accumulo
		// namespace as the data. The intent here
		// is that all data is discoverable without configuration/classes stored
		// outside of the accumulo instance.
		return new AccumuloDataStore(
				new AccumuloIndexStore(
						instance),
				new AccumuloAdapterStore(
						instance),
				new AccumuloDataStatisticsStore(
						instance),
				instance);
	}

	/***
	 * The class tells geowave about the accumulo instance it should connect to,
	 * as well as what tables it should create/store it's data in
	 * 
	 * @param zookeepers
	 *            Zookeepers associated with the accumulo instance, comma
	 *            separate
	 * @param accumuloInstance
	 *            Accumulo instance name
	 * @param accumuloUser
	 *            User geowave should connect to accumulo as
	 * @param accumuloPass
	 *            Password for user to connect to accumulo
	 * @param geowaveNamespace
	 *            Different than an accumulo namespace (unfortunate naming
	 *            usage) - this is basically a prefix on the table names geowave
	 *            uses.
	 * @return Object encapsulating the accumulo connection information
	 * @throws AccumuloException
	 * @throws AccumuloSecurityException
	 */
	protected BasicAccumuloOperations getAccumuloOperationsInstance(
			final String zookeepers,
			final String accumuloInstance,
			final String accumuloUser,
			final String accumuloPass,
			final String geowaveNamespace )
			throws AccumuloException,
			AccumuloSecurityException {
		return new BasicAccumuloOperations(
				zookeepers,
				accumuloInstance,
				accumuloUser,
				accumuloPass,
				geowaveNamespace);
	}

	/***
	 * The dataadapter interface describes how to serialize a data type. Here we
	 * are using an implementation that understands how to serialize OGC
	 * SimpleFeature types.
	 * 
	 * @param sft
	 *            simple feature type you want to generate an adapter from
	 * @return data adapter that handles serialization of the sft simple feature
	 *         type
	 */
	public static FeatureDataAdapter createDataAdapter(
			final SimpleFeatureType sft ) {
		return new FeatureDataAdapter(
				sft);
	}

	/***
	 * We need an index model that tells us how to index the data - the index
	 * determines -What fields are indexed -The precision of the index -The
	 * range of the index (min/max values) -The range type (bounded/unbounded)
	 * -The number of "levels" (different precisions, needed when the values
	 * indexed has ranges on any dimension)
	 * 
	 * @return GeoWave index for a default SPATIAL index
	 */
	public static Index createSpatialIndex() {

		// Reasonable values for spatial and spatio-temporal are provided
		// through static factory methods.
		// They are intended to be a reasonable starting place - though creating
		// a custom index may provide better
		// performance is the distribution/characterization of the data is well
		// known.
		return IndexType.SPATIAL_VECTOR.createDefaultIndex();
	}

	/***
	 * A simple feature is just a mechanism for defining attributes (a feature
	 * is just a collection of attributes + some metadata) We need to describe
	 * what our data looks like so the serializer (FeatureDataAdapter for this
	 * case) can know how to store it. Features/Attributes are also a general
	 * convention of GIS systems in general.
	 * 
	 * @return Simple Feature definition for our demo point feature
	 */
	public static SimpleFeatureType createPointFeatureType() {

		final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		final AttributeTypeBuilder ab = new AttributeTypeBuilder();

		// Names should be unique (at least for a given GeoWave namespace) -
		// think about names in the same sense as a full classname
		// The value you set here will also persist through discovery - so when
		// people are looking at a dataset they will see the
		// type names associated with the data.
		builder.setName(FEATURE_NAME);

		// The data is persisted in a sparse format, so if data is nullable it
		// will not take up any space if no values are persisted.
		// Data which is included in the primary index (in this example
		// lattitude/longtiude) can not be null
		// Calling out latitude an longitude separately is not strictly needed,
		// as the geometry contains that information. But it's
		// convienent in many use cases to get a text representation without
		// having to handle geometries.
		builder.add(ab.binding(
				Geometry.class).nillable(
				false).buildDescriptor(
				"geometry"));
		builder.add(ab.binding(
				Date.class).nillable(
				true).buildDescriptor(
				"TimeStamp"));
		builder.add(ab.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"Latitude"));
		builder.add(ab.binding(
				Double.class).nillable(
				false).buildDescriptor(
				"Longitude"));
		builder.add(ab.binding(
				String.class).nillable(
				true).buildDescriptor(
				"TrajectoryID"));
		builder.add(ab.binding(
				String.class).nillable(
				true).buildDescriptor(
				"Comment"));

		return builder.buildFeatureType();
	}

}
