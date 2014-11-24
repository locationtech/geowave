package mil.nga.giat.geowave.analytics.clustering;

import java.util.LinkedList;
import java.util.List;

import mil.nga.giat.geowave.accumulo.AccumuloOperations;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.mapreduce.input.GeoWaveInputConfigurator;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloAdapterStore;
import mil.nga.giat.geowave.accumulo.metadata.AccumuloIndexStore;
import mil.nga.giat.geowave.analytics.tools.AnalyticFeature;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayRange;
import mil.nga.giat.geowave.index.NumericIndexStrategy;
import mil.nga.giat.geowave.index.NumericIndexStrategyFactory.DataType;
import mil.nga.giat.geowave.index.sfc.data.MultiDimensionalNumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericData;
import mil.nga.giat.geowave.index.sfc.data.NumericRange;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.index.CustomIdIndex;
import mil.nga.giat.geowave.store.index.DimensionalityType;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.DistributableQuery;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.vector.adapter.FeatureDataAdapter;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.iterators.user.RegExFilter;
import org.apache.hadoop.mapreduce.JobContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.LinearRing;
import com.vividsolutions.jts.geom.Polygon;
import com.vividsolutions.jts.geom.PrecisionModel;
import com.vividsolutions.jts.geom.impl.CoordinateArraySequence;
import com.vividsolutions.jts.io.ParseException;
import com.vividsolutions.jts.io.WKTReader;

public class ClusteringUtils
{

	final static Logger LOGGER = LoggerFactory.getLogger(ClusteringUtils.class);

	/*
	 * null field will result in all possible entries in that field
	 */
	public static IteratorSetting createScanIterator(
			final String iterName,
			final String rowRegex,
			final String colfRegex,
			final String colqRegex,
			final String valRegex,
			final boolean orFields ) {
		final IteratorSetting iter = new IteratorSetting(
				15,
				iterName,
				RegExFilter.class);
		RegExFilter.setRegexs(
				iter,
				rowRegex,
				colfRegex,
				colqRegex,
				valRegex,
				orFields);

		return iter;
	}

	public static BasicAccumuloOperations createOperations(
			final String zookeeper,
			final String accumuloInstance,
			final String accumuloUser,
			final String accumuloPassword,
			final String namespace )
			throws Exception {

		return new BasicAccumuloOperations(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				namespace);
	}

	/*
	 * generate a polygon with 4 points defining a boundary of lon: -180 to 180
	 * and lat: -90 to 90 in a counter-clockwise path
	 */
	public static Polygon generateWorldPolygon() {
		final Coordinate[] coordinateArray = new Coordinate[5];
		coordinateArray[0] = new Coordinate(
				-180.0,
				-90.0);
		coordinateArray[1] = new Coordinate(
				-180.0,
				90.0);
		coordinateArray[2] = new Coordinate(
				180.0,
				90.0);
		coordinateArray[3] = new Coordinate(
				180.0,
				-90.0);
		coordinateArray[4] = new Coordinate(
				-180.0,
				-90.0);
		final CoordinateArraySequence cas = new CoordinateArraySequence(
				coordinateArray);
		final LinearRing linearRing = new LinearRing(
				cas,
				new GeometryFactory());
		return new Polygon(
				linearRing,
				null,
				new GeometryFactory());
	}

	public static Index createIndex(
			final String indexId,
			final String zookeeper,
			final String accumuloInstance,
			final String accumuloUser,
			final String accumuloPassword,
			final String namespace )
			throws Exception {

		final AccumuloOperations operations = new BasicAccumuloOperations(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				namespace);

		final AccumuloIndexStore indexStore = new AccumuloIndexStore(
				operations);

		final ByteArrayId dbId = new ByteArrayId(
				indexId);
		if (!indexStore.indexExists(dbId)) {
			if (indexId.equals(IndexType.SPATIAL_VECTOR.getDefaultId())) {
				final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
				indexStore.addIndex(index);
				return index;
			}
			else if (indexId.equals(IndexType.SPATIAL_TEMPORAL_VECTOR.getDefaultId())) {
				final Index index = IndexType.SPATIAL_TEMPORAL_VECTOR.createDefaultIndex();
				indexStore.addIndex(index);
				return index;
			}
			else {
				final Index index = new CustomIdIndex(
						IndexType.SPATIAL_VECTOR.createDefaultIndexStrategy(),
						IndexType.SPATIAL_VECTOR.getDefaultIndexModel(),
						DimensionalityType.SPATIAL,
						DataType.VECTOR,
						new ByteArrayId(
								indexId));
				indexStore.addIndex(index);
				return index;
			}
		}
		else {
			return indexStore.getIndex(dbId);
		}

	}

	public static DataAdapter<?> createAdapter(
			final String sampleDataTypeId,
			final String zookeeper,
			final String accumuloInstance,
			final String accumuloUser,
			final String accumuloPassword,
			final String namespace,
			final String[] dimensionNames )
			throws Exception {

		final FeatureDataAdapter adapter = AnalyticFeature.createGeometryFeatureAdapter(
				sampleDataTypeId,
				dimensionNames,
				4326);

		final AccumuloOperations operations = new BasicAccumuloOperations(
				zookeeper,
				accumuloInstance,
				accumuloUser,
				accumuloPassword,
				namespace);

		final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
				operations);

		final ByteArrayId dbId = new ByteArrayId(
				sampleDataTypeId);
		if (!adapterStore.adapterExists(dbId)) {
			adapterStore.addAdapter(adapter);
			return adapter;
		}
		else {
			return adapterStore.getAdapter(dbId);
		}

	}

	public static DataAdapter[] getAdapters(
			final String zookeeper,
			final String accumuloInstance,
			final String accumuloUser,
			final String accumuloPassword,
			final String namespace ) {
		BasicAccumuloOperations ops;
		try {
			ops = new BasicAccumuloOperations(
					zookeeper,
					accumuloInstance,
					accumuloUser,
					accumuloPassword,
					namespace);

			final AccumuloAdapterStore adapterStore = new AccumuloAdapterStore(
					ops);
			final mil.nga.giat.geowave.store.CloseableIterator<DataAdapter<?>> it = adapterStore.getAdapters();
			final List<DataAdapter> adapters = new LinkedList<DataAdapter>();
			while (it.hasNext()) {
				adapters.add(it.next());
			}

			final DataAdapter[] result = new DataAdapter[adapters.size()];
			adapters.toArray(result);
			return result;
		}
		catch (final AccumuloException e) {
			LOGGER.error(
					"cannot connect to GeoWave ",
					e);
		}
		catch (final AccumuloSecurityException e) {
			LOGGER.error(
					"cannot connect to GeoWave ",
					e);
		}
		return null;
	}

	public static Index[] getIndices(
			final String zookeeper,
			final String accumuloInstance,
			final String accumuloUser,
			final String accumuloPassword,
			final String namespace ) {
		BasicAccumuloOperations ops;
		try {
			ops = new BasicAccumuloOperations(
					zookeeper,
					accumuloInstance,
					accumuloUser,
					accumuloPassword,
					namespace);

			final AccumuloIndexStore indexStore = new AccumuloIndexStore(
					ops);
			final mil.nga.giat.geowave.store.CloseableIterator<Index> it = indexStore.getIndices();
			final List<Index> indices = new LinkedList<Index>();
			while (it.hasNext()) {
				indices.add(it.next());
			}

			final Index[] result = new Index[indices.size()];
			indices.toArray(result);
			return result;
		}
		catch (final AccumuloException e) {
			LOGGER.error(
					"cannot connect to GeoWave ",
					e);
		}
		catch (final AccumuloSecurityException e) {
			LOGGER.error(
					"cannot connect to GeoWave ",
					e);
		}
		return new Index[] {
			IndexType.SPATIAL_VECTOR.createDefaultIndex()
		};
	}

	public static final GeometryFactory geoFactory = new GeometryFactory(
			new PrecisionModel(
					PrecisionModel.FLOATING),
			4326);

	public static Geometry createBoundingRegion(
			final String str )
			throws ParseException {
		final WKTReader wktReader = new WKTReader(
				geoFactory);
		return wktReader.read(str);

	}

	public static String getBoundingRegionForQuery(
			final JobContext context,
			final Class<?> CLASS ) {
		return getBoundingRegionForQuery(
				GeoWaveInputConfigurator.getQuery(
						CLASS,
						context),
				GeoWaveInputConfigurator.searchForIndices(
						CLASS,
						context));
	}

	public static String getBoundingRegionForQuery(
			final DistributableQuery query,
			final Index[] indices ) {
		if (query == null) {
			return generateWorldPolygon().toText();
		}
		if (query instanceof SpatialQuery) {
			final Geometry geo = ((SpatialQuery) query).getQueryGeometry();
			if (geo != null) {
				return geo.toText();
			}
		}
		NumericData[] result = new NumericData[0];
		for (final Index index : indices) {
			final NumericIndexStrategy indexStrategy = index.getIndexStrategy();
			final MultiDimensionalNumericData indexConstraints = query.getIndexConstraints(indexStrategy);
			final NumericData[] newData = new NumericData[indexConstraints.getDimensionCount()];
			for (int i = 0; i < indexConstraints.getDimensionCount(); i++) {
				final NumericData latest = indexConstraints.getDataPerDimension()[i];
				if (result.length < (i + 1)) {
					newData[i] = new NumericRange(
							latest.getMin(),
							latest.getMax());
				}
				else {
					final NumericData old = result[i];
					newData[i] = new NumericRange(
							Math.min(
									latest.getMin(),
									old.getMin()),
							Math.max(
									latest.getMax(),
									old.getMax()));
				}
			}
			result = newData;
		}
		final StringBuffer buf = new StringBuffer();
		buf.append("POLYGON ((");
		buf.append(
				result[0].getMin()).append(
				' ').append(
				result[1].getMin()).append(
				", ");
		buf.append(
				result[0].getMax()).append(
				' ').append(
				result[1].getMin()).append(
				", ");
		buf.append(
				result[0].getMax()).append(
				' ').append(
				result[1].getMax()).append(
				", ");
		buf.append(
				result[0].getMin()).append(
				' ').append(
				result[1].getMax()).append(
				", ");
		buf.append(
				result[0].getMin()).append(
				' ').append(
				result[1].getMin());
		buf.append("))");
		return buf.toString();
	}

	/*
	 * Method takes in a polygon and generates the corresponding ranges in a
	 * GeoWave spatial index
	 */
	protected static List<ByteArrayRange> getGeoWaveRangesForQuery(
			final Polygon polygon ) {

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();
		final List<ByteArrayRange> ranges = index.getIndexStrategy().getQueryRanges(
				new SpatialQuery(
						polygon).getIndexConstraints(index.getIndexStrategy()));

		return ranges;
	}

}
