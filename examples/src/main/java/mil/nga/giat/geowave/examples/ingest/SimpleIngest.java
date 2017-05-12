package mil.nga.giat.geowave.examples.ingest;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.adapter.vector.GeotoolsFeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.ingest.SpatialDimensionalityTypeProvider.SpatialIndexBuilder;
import mil.nga.giat.geowave.core.store.DataStore;
import mil.nga.giat.geowave.core.store.index.PrimaryIndex;
import mil.nga.giat.geowave.datastore.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.datastore.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.datastore.accumulo.index.secondary.AccumuloSecondaryIndexDataStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterIndexMappingStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloDataStatisticsStore;
import mil.nga.giat.geowave.datastore.accumulo.metadata.AccumuloIndexStore;

public class SimpleIngest
{

	static Logger log = LoggerFactory.getLogger(SimpleIngest.class);
	public static final String FEATURE_NAME = "GridPoint";

	public static List<SimpleFeature> getGriddedFeatures(
			final SimpleFeatureBuilder pointBuilder,
			final int firstFeatureId ) {

		int featureId = firstFeatureId;
		final List<SimpleFeature> feats = new ArrayList<>();
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

	/***
	 * DataStore is essentially the controller that take the accumulo
	 * information, geowave configuration, and data type, and inserts/queries
	 * from accumulo
	 *
	 * @param instance
	 *            Accumulo instance configuration
	 * @return DataStore object for the particular accumulo instance
	 */
	protected DataStore getAccumuloGeowaveDataStore(
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
				new AccumuloSecondaryIndexDataStore(
						instance),
				new AccumuloAdapterIndexMappingStore(
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
	public static GeotoolsFeatureDataAdapter createDataAdapter(
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
	public static PrimaryIndex createSpatialIndex() {

		// Reasonable values for spatial and spatial-temporal are provided
		// through index builders.
		// They are intended to be a reasonable starting place - though creating
		// a custom index may provide better
		// performance as the distribution/characterization of the data is well
		// known. There are many such customizations available through setters
		// on the builder.

		// for example to create a spatial-temporal index with 8 randomized
		// partitions (pre-splits on accumulo or hbase) and a temporal bias
		// (giving more precision to time than space) you could do something
		// like this:
		//@formatter:off
		// return new SpatialTemporalIndexBuilder().setBias(Bias.TEMPORAL).setNumPartitions(8);
		//@formatter:on
		return new SpatialIndexBuilder().createIndex();
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
