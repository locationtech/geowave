package mil.nga.giat.geowave.benchmark.utils;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mil.nga.giat.geowave.accumulo.AccumuloDataStore;
import mil.nga.giat.geowave.accumulo.BasicAccumuloOperations;
import mil.nga.giat.geowave.accumulo.query.AccumuloConstraintsQuery;
import mil.nga.giat.geowave.accumulo.query.ArrayToElementsIterator;
import mil.nga.giat.geowave.accumulo.query.ElementsToArrayIterator;
import mil.nga.giat.geowave.accumulo.util.AccumuloUtils;
import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.ByteArrayUtils;
import mil.nga.giat.geowave.index.PersistenceUtils;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.CloseableIterator;
import mil.nga.giat.geowave.store.adapter.DataAdapter;
import mil.nga.giat.geowave.store.adapter.MemoryAdapterStore;
import mil.nga.giat.geowave.store.data.field.ArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.PrimitiveByteArrayReader;
import mil.nga.giat.geowave.store.index.CommonIndexModel;
import mil.nga.giat.geowave.store.index.Index;
import mil.nga.giat.geowave.store.index.IndexType;
import mil.nga.giat.geowave.store.query.SpatialQuery;
import mil.nga.giat.geowave.vector.adapter.FeatureCollectionDataAdapter;
import mil.nga.giat.geowave.vector.util.FitToIndexDefaultFeatureCollection;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.accumulo.core.iterators.user.TransformingIterator;
import org.apache.commons.math.stat.descriptive.moment.Mean;
import org.apache.commons.math.stat.descriptive.moment.StandardDeviation;
import org.apache.log4j.Logger;
import org.geotools.data.DataUtilities;
import org.geotools.feature.DefaultFeatureCollection;
import org.geotools.feature.SchemaException;
import org.opengis.feature.simple.SimpleFeatureType;

import com.google.common.primitives.Doubles;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class HistogramGenerator
{

	private static final Logger LOGGER = Logger.getLogger(HistogramGenerator.class);

	public void generateHistogram(
			final int tileSize,
			final int queryNum,
			final double north,
			final double south,
			final double east,
			final double west )
			throws Exception {

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		final BasicAccumuloOperations accumuloOperations = new BasicAccumuloOperations(
				System.getProperty("zookeeperUrl"),
				System.getProperty("instance"),
				System.getProperty("username"),
				System.getProperty("password"),
				"featureCollectionTest_vector_" + tileSize);

		final AccumuloDataStore dataStore = new AccumuloDataStore(
				accumuloOperations);

		final SimpleFeatureType TYPE = DataUtilities.createType(
				"TestPoint",
				"location:Point:srid=4326,dim1:Double,dim2:Double,dim3:Double,startTime:Date,stopTime:Date,index:String");

		final FeatureCollectionDataAdapter dataAdapter = new FeatureCollectionDataAdapter(
				TYPE,
				tileSize);

		final Coordinate[] coords = new Coordinate[] {
			new Coordinate(
					west,
					south),
			new Coordinate(
					east,
					south),
			new Coordinate(
					east,
					north),
			new Coordinate(
					west,
					north),
			new Coordinate(
					west,
					south)
		};

		final Geometry geom = new GeometryFactory().createPolygon(coords);

		final CloseableIterator<DefaultFeatureCollection> itr = dataStore.query(
				dataAdapter,
				IndexType.SPATIAL_VECTOR.createDefaultIndex(),
				new SpatialQuery(
						geom));

		final List<Double> featsPerTile = new ArrayList<Double>();
		final Map<Integer, Integer> collsPerTier = new HashMap<Integer, Integer>();
		final Map<Integer, Integer> featsPerTier = new HashMap<Integer, Integer>();
		final int[] hist = new int[tileSize / 2];

		int numColls = 0;
		int numFeatures = 0;
		while (itr.hasNext()) {
			final FitToIndexDefaultFeatureCollection coll = (FitToIndexDefaultFeatureCollection) itr.next();

			final ArrayReader reader = new ArrayReader(
					new PrimitiveByteArrayReader());
			final byte[][] keyBytes = (byte[][]) reader.readField(coll.getIndexId().getBytes());
			final Key key = new Key(
					(keyBytes[0] != null) ? keyBytes[0] : new byte[] {},
					(keyBytes[1] != null) ? keyBytes[1] : new byte[] {},
					(keyBytes[2] != null) ? keyBytes[2] : new byte[] {},
					(keyBytes[3] != null) ? keyBytes[3] : new byte[] {},
					ByteBuffer.wrap(
							keyBytes[4]).getLong());

			final int tier = ByteBuffer.wrap(
					key.getRowData().getBackingArray()).get();

			if (collsPerTier.containsKey(tier)) {
				collsPerTier.put(
						tier,
						collsPerTier.get(tier) + 1);
			}
			else {
				collsPerTier.put(
						tier,
						1);
			}

			if (featsPerTier.containsKey(tier)) {
				featsPerTier.put(
						tier,
						featsPerTier.get(tier) + coll.size());
			}
			else {
				featsPerTier.put(
						tier,
						coll.size());
			}

			final int histIdx = Math.min(
					(int) (coll.size() / 2.0),
					(tileSize / 2) - 1);
			hist[histIdx]++;

			numColls++;
			numFeatures += coll.size();
			featsPerTile.add(Double.valueOf(coll.size()));
		}
		itr.close();

		System.out.println("Features: " + numFeatures);
		for (final Map.Entry<Integer, Integer> kvp : featsPerTier.entrySet()) {
			System.out.println("  Features (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Collections: " + numColls);
		for (final Map.Entry<Integer, Integer> kvp : collsPerTier.entrySet()) {
			System.out.println("  Collections (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Features per Tile (MEAN): " + new Mean().evaluate(Doubles.toArray(featsPerTile)));
		System.out.println("Features per Tile (STD): " + new StandardDeviation().evaluate(Doubles.toArray(featsPerTile)));

		detachIterators(
				AccumuloUtils.getQualifiedTableName(
						"featureCollectionTest_vector_" + tileSize,
						StringUtils.stringFromBinary(index.getId().getBytes())),
				accumuloOperations.getConnector());

		final AccumuloConstraintsQuery q = new AccumuloConstraintsQuery(
				Arrays.asList(new ByteArrayId[] {
					dataAdapter.getAdapterId()
				}),
				index,
				new SpatialQuery(
						geom).getIndexConstraints(index.getIndexStrategy()),
				null);
		q.setQueryFiltersEnabled(false);

		final CloseableIterator<DefaultFeatureCollection> itr_full = (CloseableIterator<DefaultFeatureCollection>) q.query(
				accumuloOperations,
				new MemoryAdapterStore(
						new DataAdapter[] {
							dataAdapter
						}),
				null);

		final List<Double> featsPerTile_full = new ArrayList<Double>();
		final Map<Integer, Integer> collsPerTier_full = new HashMap<Integer, Integer>();
		final Map<Integer, Integer> featsPerTier_full = new HashMap<Integer, Integer>();
		final int[] hist_full = new int[tileSize / 2];

		numColls = 0;
		numFeatures = 0;
		while (itr_full.hasNext()) {
			final FitToIndexDefaultFeatureCollection coll = (FitToIndexDefaultFeatureCollection) itr_full.next();

			final ArrayReader reader = new ArrayReader(
					new PrimitiveByteArrayReader());
			final byte[][] keyBytes = (byte[][]) reader.readField(coll.getIndexId().getBytes());
			final Key key = new Key(
					(keyBytes[0] != null) ? keyBytes[0] : new byte[] {},
					(keyBytes[1] != null) ? keyBytes[1] : new byte[] {},
					(keyBytes[2] != null) ? keyBytes[2] : new byte[] {},
					(keyBytes[3] != null) ? keyBytes[3] : new byte[] {},
					ByteBuffer.wrap(
							keyBytes[4]).getLong());

			final int tier = ByteBuffer.wrap(
					key.getRowData().getBackingArray()).get();

			if (collsPerTier_full.containsKey(tier)) {
				collsPerTier_full.put(
						tier,
						collsPerTier_full.get(tier) + 1);
			}
			else {
				collsPerTier_full.put(
						tier,
						1);
			}

			if (featsPerTier_full.containsKey(tier)) {
				featsPerTier_full.put(
						tier,
						featsPerTier_full.get(tier) + coll.size());
			}
			else {
				featsPerTier_full.put(
						tier,
						coll.size());
			}

			final int histIdx = Math.min(
					(int) (coll.size() / 2.0),
					(tileSize / 2) - 1);
			hist_full[histIdx]++;

			numColls++;
			numFeatures += coll.size();
			featsPerTile_full.add(new Double(
					coll.size()));
		}
		itr_full.close();

		attachIterators(
				index.getIndexModel(),
				AccumuloUtils.getQualifiedTableName(
						"featureCollectionTest_vector_" + tileSize,
						StringUtils.stringFromBinary(index.getId().getBytes())),
				accumuloOperations.getConnector());

		System.out.println("Features: " + numFeatures);
		for (final Map.Entry<Integer, Integer> kvp : featsPerTier.entrySet()) {
			System.out.println("  Features (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Collections: " + numColls);
		for (final Map.Entry<Integer, Integer> kvp : collsPerTier_full.entrySet()) {
			System.out.println("  Collections (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Features per Tile (MEAN): " + new Mean().evaluate(Doubles.toArray(featsPerTile_full)));
		System.out.println("Features per Tile (STD): " + new StandardDeviation().evaluate(Doubles.toArray(featsPerTile_full)));

		// write the histograms
		PrintWriter writer = new PrintWriter(
				"hist_" + queryNum + "_" + tileSize,
				StringUtils.UTF8_CHAR_SET.toString());
		for (final int v : hist) {
			writer.println(v);
		}
		writer.close();

		writer = new PrintWriter(
				"hist_full_" + queryNum + "_" + tileSize,
				StringUtils.UTF8_CHAR_SET.toString());
		for (final int v : hist_full) {
			writer.println(v);
		}
		writer.close();
	}

	public void originalTest(
			final String[] args )
			throws SchemaException,
			AccumuloException,
			AccumuloSecurityException,
			TableNotFoundException,
			IOException {

		final Index index = IndexType.SPATIAL_VECTOR.createDefaultIndex();

		final BasicAccumuloOperations accumuloOperations = new BasicAccumuloOperations(
				System.getProperty("zookeeperUrl"),
				System.getProperty("instance"),
				System.getProperty("username"),
				System.getProperty("password"),
				"featureCollectionTest_vector_1000");

		final AccumuloDataStore dataStore = new AccumuloDataStore(
				accumuloOperations);

		final SimpleFeatureType TYPE = DataUtilities.createType(
				"TestPoint",
				"location:Point:srid=4326,dim1:Double,dim2:Double,dim3:Double,startTime:Date,stopTime:Date,index:String");

		final FeatureCollectionDataAdapter dataAdapter = new FeatureCollectionDataAdapter(
				TYPE,
				1000);

		final Coordinate[] coords10 = new Coordinate[5];
		coords10[0] = new Coordinate(
				78.06386544649773,
				-59.85236814516062);
		coords10[1] = new Coordinate(
				85.55750025087512,
				-59.85236814516062);
		coords10[2] = new Coordinate(
				85.55750025087512,
				-53.730289324274004);
		coords10[3] = new Coordinate(
				78.06386544649773,
				-53.730289324274004);
		coords10[4] = new Coordinate(
				78.06386544649773,
				-59.85236814516062);

		final Geometry geom10 = new GeometryFactory().createPolygon(coords10);

		final CloseableIterator<DefaultFeatureCollection> itr10 = dataStore.query(
				dataAdapter,
				IndexType.SPATIAL_VECTOR.createDefaultIndex(),
				new SpatialQuery(
						geom10));

		final List<Double> featsPerTile10 = new ArrayList<Double>();
		final Map<Integer, Integer> collsPerTier10 = new HashMap<Integer, Integer>();
		final Map<Integer, Integer> featsPerTier10 = new HashMap<Integer, Integer>();
		System.out.println("\nQuery 10");
		int numColls = 0;
		int numFeatures = 0;
		while (itr10.hasNext()) {
			final FitToIndexDefaultFeatureCollection coll = (FitToIndexDefaultFeatureCollection) itr10.next();

			final ArrayReader reader = new ArrayReader(
					new PrimitiveByteArrayReader());
			final byte[][] keyBytes = (byte[][]) reader.readField(coll.getIndexId().getBytes());
			final Key key = new Key(
					(keyBytes[0] != null) ? keyBytes[0] : new byte[] {},
					(keyBytes[1] != null) ? keyBytes[1] : new byte[] {},
					(keyBytes[2] != null) ? keyBytes[2] : new byte[] {},
					(keyBytes[3] != null) ? keyBytes[3] : new byte[] {},
					ByteBuffer.wrap(
							keyBytes[4]).getLong());

			final int tier = ByteBuffer.wrap(
					key.getRowData().getBackingArray()).get();

			if (collsPerTier10.containsKey(tier)) {
				collsPerTier10.put(
						tier,
						collsPerTier10.get(tier) + 1);
			}
			else {
				collsPerTier10.put(
						tier,
						1);
			}

			if (featsPerTier10.containsKey(tier)) {
				featsPerTier10.put(
						tier,
						featsPerTier10.get(tier) + coll.size());
			}
			else {
				featsPerTier10.put(
						tier,
						coll.size());
			}

			numColls++;
			numFeatures += coll.size();
			featsPerTile10.add(new Double(
					coll.size()));
		}
		itr10.close();

		System.out.println("Features: " + numFeatures);
		for (final Map.Entry<Integer, Integer> kvp : featsPerTier10.entrySet()) {
			System.out.println("  Features (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Collections: " + numColls);
		for (final Map.Entry<Integer, Integer> kvp : collsPerTier10.entrySet()) {
			System.out.println("  Collections (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Features per Tile (MEAN): " + new Mean().evaluate(Doubles.toArray(featsPerTile10)));
		System.out.println("Features per Tile (STD): " + new StandardDeviation().evaluate(Doubles.toArray(featsPerTile10)));

		detachIterators(
				AccumuloUtils.getQualifiedTableName(
						"featureCollectionTest_vector_1000",
						StringUtils.stringFromBinary(index.getId().getBytes())),
				accumuloOperations.getConnector());

		final AccumuloConstraintsQuery q10 = new AccumuloConstraintsQuery(
				Arrays.asList(new ByteArrayId[] {
					dataAdapter.getAdapterId()
				}),
				index,
				new SpatialQuery(
						geom10).getIndexConstraints(index.getIndexStrategy()),
				null);
		q10.setQueryFiltersEnabled(false);

		final CloseableIterator<DefaultFeatureCollection> itr10_full = (CloseableIterator<DefaultFeatureCollection>) q10.query(
				accumuloOperations,
				new MemoryAdapterStore(
						new DataAdapter[] {
							dataAdapter
						}),
				null);

		final List<Double> featsPerTile10_full = new ArrayList<Double>();
		final Map<Integer, Integer> collsPerTier10_full = new HashMap<Integer, Integer>();
		final Map<Integer, Integer> featsPerTier10_full = new HashMap<Integer, Integer>();
		System.out.println("\nQuery 10 - Full");
		numColls = 0;
		numFeatures = 0;
		while (itr10_full.hasNext()) {
			final FitToIndexDefaultFeatureCollection coll = (FitToIndexDefaultFeatureCollection) itr10_full.next();

			final ArrayReader reader = new ArrayReader(
					new PrimitiveByteArrayReader());
			final byte[][] keyBytes = (byte[][]) reader.readField(coll.getIndexId().getBytes());
			final Key key = new Key(
					(keyBytes[0] != null) ? keyBytes[0] : new byte[] {},
					(keyBytes[1] != null) ? keyBytes[1] : new byte[] {},
					(keyBytes[2] != null) ? keyBytes[2] : new byte[] {},
					(keyBytes[3] != null) ? keyBytes[3] : new byte[] {},
					ByteBuffer.wrap(
							keyBytes[4]).getLong());

			final int tier = ByteBuffer.wrap(
					key.getRowData().getBackingArray()).get();

			if (collsPerTier10_full.containsKey(tier)) {
				collsPerTier10_full.put(
						tier,
						collsPerTier10_full.get(tier) + 1);
			}
			else {
				collsPerTier10_full.put(
						tier,
						1);
			}

			if (featsPerTier10_full.containsKey(tier)) {
				featsPerTier10_full.put(
						tier,
						featsPerTier10_full.get(tier) + coll.size());
			}
			else {
				featsPerTier10_full.put(
						tier,
						coll.size());
			}

			numColls++;
			numFeatures += coll.size();
			featsPerTile10_full.add(new Double(
					coll.size()));
		}
		itr10_full.close();

		attachIterators(
				index.getIndexModel(),
				AccumuloUtils.getQualifiedTableName(
						"featureCollectionTest_vector_1000",
						StringUtils.stringFromBinary(index.getId().getBytes())),
				accumuloOperations.getConnector());

		System.out.println("Features: " + numFeatures);
		for (final Map.Entry<Integer, Integer> kvp : featsPerTier10_full.entrySet()) {
			System.out.println("  Features (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Collections: " + numColls);
		for (final Map.Entry<Integer, Integer> kvp : collsPerTier10_full.entrySet()) {
			System.out.println("  Collections (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Features per Tile (MEAN): " + new Mean().evaluate(Doubles.toArray(featsPerTile10_full)));
		System.out.println("Features per Tile (STD): " + new StandardDeviation().evaluate(Doubles.toArray(featsPerTile10_full)));

		// ================================================================================================
		coords10[0] = new Coordinate(
				-119.29923973537691,
				-66.02765731285959);
		coords10[1] = new Coordinate(
				-113.91325572756969,
				-66.02765731285959);
		coords10[2] = new Coordinate(
				-113.91325572756969,
				-58.786695540035225);
		coords10[3] = new Coordinate(
				-119.29923973537691,
				-58.786695540035225);
		coords10[4] = new Coordinate(
				-119.29923973537691,
				-66.02765731285959);

		final Geometry geom11 = new GeometryFactory().createPolygon(coords10);

		final CloseableIterator<DefaultFeatureCollection> itr11 = dataStore.query(
				dataAdapter,
				IndexType.SPATIAL_VECTOR.createDefaultIndex(),
				new SpatialQuery(
						geom11));

		final List<Double> featsPerTile11 = new ArrayList<Double>();
		final Map<Integer, Integer> collsPerTier11 = new HashMap<Integer, Integer>();
		final Map<Integer, Integer> featsPerTier11 = new HashMap<Integer, Integer>();
		System.out.println("\nQuery 11");
		numColls = 0;
		numFeatures = 0;
		while (itr11.hasNext()) {
			final FitToIndexDefaultFeatureCollection coll = (FitToIndexDefaultFeatureCollection) itr11.next();

			final ArrayReader reader = new ArrayReader(
					new PrimitiveByteArrayReader());
			final byte[][] keyBytes = (byte[][]) reader.readField(coll.getIndexId().getBytes());
			final Key key = new Key(
					(keyBytes[0] != null) ? keyBytes[0] : new byte[] {},
					(keyBytes[1] != null) ? keyBytes[1] : new byte[] {},
					(keyBytes[2] != null) ? keyBytes[2] : new byte[] {},
					(keyBytes[3] != null) ? keyBytes[3] : new byte[] {},
					ByteBuffer.wrap(
							keyBytes[4]).getLong());

			final int tier = ByteBuffer.wrap(
					key.getRowData().getBackingArray()).get();

			if (collsPerTier11.containsKey(tier)) {
				collsPerTier11.put(
						tier,
						collsPerTier11.get(tier) + 1);
			}
			else {
				collsPerTier11.put(
						tier,
						1);
			}

			if (featsPerTier11.containsKey(tier)) {
				featsPerTier11.put(
						tier,
						featsPerTier11.get(tier) + coll.size());
			}
			else {
				featsPerTier11.put(
						tier,
						coll.size());
			}

			numColls++;
			numFeatures += coll.size();
			featsPerTile11.add(new Double(
					coll.size()));
		}
		itr11.close();

		System.out.println("Features: " + numFeatures);
		for (final Map.Entry<Integer, Integer> kvp : featsPerTier11.entrySet()) {
			System.out.println("  Features (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Collections: " + numColls);
		for (final Map.Entry<Integer, Integer> kvp : collsPerTier11.entrySet()) {
			System.out.println("  Collections (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Features per Tile (MEAN): " + new Mean().evaluate(Doubles.toArray(featsPerTile11)));
		System.out.println("Features per Tile (STD): " + new StandardDeviation().evaluate(Doubles.toArray(featsPerTile11)));

		detachIterators(
				AccumuloUtils.getQualifiedTableName(
						"featureCollectionTest_vector_1000",
						StringUtils.stringFromBinary(index.getId().getBytes())),
				accumuloOperations.getConnector());

		final AccumuloConstraintsQuery q11 = new AccumuloConstraintsQuery(
				Arrays.asList(new ByteArrayId[] {
					dataAdapter.getAdapterId()
				}),
				index,
				new SpatialQuery(
						geom11).getIndexConstraints(index.getIndexStrategy()),
				null);
		q11.setQueryFiltersEnabled(false);

		final CloseableIterator<DefaultFeatureCollection> itr11_full = (CloseableIterator<DefaultFeatureCollection>) q11.query(
				accumuloOperations,
				new MemoryAdapterStore(
						new DataAdapter[] {
							dataAdapter
						}),
				null);

		final List<Double> featsPerTile11_full = new ArrayList<Double>();
		final Map<Integer, Integer> collsPerTier11_full = new HashMap<Integer, Integer>();
		final Map<Integer, Integer> featsPerTier11_full = new HashMap<Integer, Integer>();
		System.out.println("\nQuery 11 - Full");
		numColls = 0;
		numFeatures = 0;
		while (itr11_full.hasNext()) {
			final FitToIndexDefaultFeatureCollection coll = (FitToIndexDefaultFeatureCollection) itr11_full.next();

			final ArrayReader reader = new ArrayReader(
					new PrimitiveByteArrayReader());
			final byte[][] keyBytes = (byte[][]) reader.readField(coll.getIndexId().getBytes());
			final Key key = new Key(
					(keyBytes[0] != null) ? keyBytes[0] : new byte[] {},
					(keyBytes[1] != null) ? keyBytes[1] : new byte[] {},
					(keyBytes[2] != null) ? keyBytes[2] : new byte[] {},
					(keyBytes[3] != null) ? keyBytes[3] : new byte[] {},
					ByteBuffer.wrap(
							keyBytes[4]).getLong());

			final int tier = ByteBuffer.wrap(
					key.getRowData().getBackingArray()).get();

			if (collsPerTier11_full.containsKey(tier)) {
				collsPerTier11_full.put(
						tier,
						collsPerTier11_full.get(tier) + 1);
			}
			else {
				collsPerTier11_full.put(
						tier,
						1);
			}

			if (featsPerTier11_full.containsKey(tier)) {
				featsPerTier11_full.put(
						tier,
						featsPerTier11_full.get(tier) + coll.size());
			}
			else {
				featsPerTier11_full.put(
						tier,
						coll.size());
			}

			numColls++;
			numFeatures += coll.size();
			featsPerTile11_full.add(new Double(
					coll.size()));
		}
		itr11_full.close();

		attachIterators(
				index.getIndexModel(),
				AccumuloUtils.getQualifiedTableName(
						"featureCollectionTest_vector_1000",
						StringUtils.stringFromBinary(index.getId().getBytes())),
				accumuloOperations.getConnector());

		System.out.println("Features: " + numFeatures);
		for (final Map.Entry<Integer, Integer> kvp : featsPerTier11_full.entrySet()) {
			System.out.println("  Features (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Collections: " + numColls);
		for (final Map.Entry<Integer, Integer> kvp : collsPerTier11_full.entrySet()) {
			System.out.println("  Collections (Tier " + kvp.getKey() + "): " + kvp.getValue());
		}
		System.out.println("Features per Tile (MEAN): " + new Mean().evaluate(Doubles.toArray(featsPerTile11_full)));
		System.out.println("Features per Tile (STD): " + new StandardDeviation().evaluate(Doubles.toArray(featsPerTile11_full)));
	}

	public static void detachIterators(
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

	public static void attachIterators(
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

	public static void main(
			final String[] args )
			throws Exception {
		final HistogramGenerator qt = new HistogramGenerator();

		final int[] tileSizes = new int[] {
			100,
			500,
			1000,
			5000,
			10000,
			50000
		};

		final BufferedReader br = new BufferedReader(
				new InputStreamReader(
						new FileInputStream(
								"large-queries.txt"),
						StringUtils.UTF8_CHAR_SET));

		int queryNum = 0;

		String curLine;
		try {
			while ((curLine = br.readLine()) != null) {
				queryNum++;

				// left off at 7...
				if (queryNum >= 7) {
					final String[] coords = curLine.split(" ");
					final double north = Double.parseDouble(coords[0]);
					final double south = Double.parseDouble(coords[1]);
					final double east = Double.parseDouble(coords[2]);
					final double west = Double.parseDouble(coords[3]);

					// perform this query for each tileSize
					for (final int tileSize : tileSizes) {
						System.out.println("\nQuery " + queryNum + " - Tile Size " + tileSize);

						qt.generateHistogram(
								tileSize,
								queryNum,
								north,
								south,
								east,
								west);
					}

				}
			}
		}
		catch (Exception e) {
			LOGGER.error(
					"Unable to generate histogram",
					e);
		}
		finally {
			br.close();
		}
	}
}
