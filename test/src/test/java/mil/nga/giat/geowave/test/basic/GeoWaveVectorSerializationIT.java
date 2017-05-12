package mil.nga.giat.geowave.test.basic;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.geotools.feature.AttributeTypeBuilder;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.feature.simple.SimpleFeatureTypeBuilder;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.opengis.feature.Property;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;

import mil.nga.giat.geowave.adapter.vector.FeatureDataAdapter;
import mil.nga.giat.geowave.core.geotime.GeometryUtils;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.CloseableIterator;
import mil.nga.giat.geowave.core.store.IndexWriter;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;
import mil.nga.giat.geowave.core.store.query.QueryOptions;
import mil.nga.giat.geowave.test.GeoWaveITRunner;
import mil.nga.giat.geowave.test.TestUtils;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore;
import mil.nga.giat.geowave.test.annotation.GeoWaveTestStore.GeoWaveStoreType;

@RunWith(GeoWaveITRunner.class)
public class GeoWaveVectorSerializationIT
{

	private final static Logger LOGGER = LoggerFactory.getLogger(GeoWaveVectorSerializationIT.class);
	@GeoWaveTestStore({
		GeoWaveStoreType.ACCUMULO,
		GeoWaveStoreType.BIGTABLE,
		GeoWaveStoreType.HBASE
	})
	protected DataStorePluginOptions dataStore;
	private static long startMillis;

	@BeforeClass
	public static void reportTestStart() {
		startMillis = System.currentTimeMillis();
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("*  RUNNING GeoWaveVectorSerializationIT *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@AfterClass
	public static void reportTestFinish() {
		LOGGER.warn("-----------------------------------------");
		LOGGER.warn("*                                       *");
		LOGGER.warn("* FINISHED GeoWaveVectorSerializationIT *");
		LOGGER
				.warn("*         " + ((System.currentTimeMillis() - startMillis) / 1000)
						+ "s elapsed.                 *");
		LOGGER.warn("*                                       *");
		LOGGER.warn("-----------------------------------------");
	}

	@Test
	public void testFeatureSerialization()
			throws IOException {

		final Map<Class, Object> args = new HashMap<>();
		args.put(
				Geometry.class,
				GeometryUtils.GEOMETRY_FACTORY.createPoint(
						new Coordinate(
								123.4,
								567.8)).buffer(
						1));
		args.put(
				Integer.class,
				23);
		args.put(
				Long.class,
				473874387l);
		args.put(
				Boolean.class,
				Boolean.TRUE);
		args.put(
				Byte.class,
				(byte) 0xa);
		args.put(
				Short.class,
				Short.valueOf("2"));
		args.put(
				Float.class,
				34.23434f);
		args.put(
				Double.class,
				85.3498394839d);
		args.put(
				byte[].class,
				new byte[] {
					(byte) 1,
					(byte) 2,
					(byte) 3
				});
		args.put(
				Byte[].class,
				new Byte[] {
					(byte) 4,
					(byte) 5,
					(byte) 6
				});
		args.put(
				Date.class,
				new Date(
						8675309l));
		args.put(
				BigInteger.class,
				BigInteger.valueOf(893489348343423l));
		args.put(
				BigDecimal.class,
				new BigDecimal(
						"939384.93840238409237483617837483"));
		args.put(
				Calendar.class,
				Calendar.getInstance());
		args.put(
				String.class,
				"This is my string. There are many like it, but this one is mine.\n"
						+ "My string is my best friend. It is my life. I must master it as I must master my life.");
		args.put(
				long[].class,
				new long[] {
					12345l,
					6789l,
					1011l,
					1213111111111111l
				});
		args.put(
				int[].class,
				new int[] {
					-55,
					-44,
					-33,
					-934839,
					55
				});
		args.put(
				double[].class,
				new double[] {
					1.125d,
					2.25d
				});
		args.put(
				float[].class,
				new float[] {
					1.5f,
					1.75f
				});
		args.put(
				short[].class,
				new short[] {
					(short) 8,
					(short) 9,
					(short) 10
				});

		final SimpleFeatureTypeBuilder builder = new SimpleFeatureTypeBuilder();
		final AttributeTypeBuilder ab = new AttributeTypeBuilder();
		builder.setName("featureserializationtest");

		for (final Map.Entry<Class, Object> arg : args.entrySet()) {
			builder.add(ab.binding(
					arg.getKey()).nillable(
					false).buildDescriptor(
					arg.getKey().getName().toString()));
		}

		final SimpleFeatureType serTestType = builder.buildFeatureType();
		final SimpleFeatureBuilder serBuilder = new SimpleFeatureBuilder(
				serTestType);
		final FeatureDataAdapter serAdapter = new FeatureDataAdapter(
				serTestType);

		for (final Map.Entry<Class, Object> arg : args.entrySet()) {
			serBuilder.set(
					arg.getKey().getName(),
					arg.getValue());
		}

		final mil.nga.giat.geowave.core.store.DataStore geowaveStore = dataStore.createDataStore();

		final SimpleFeature sf = serBuilder.buildFeature("343");
		try (IndexWriter writer = geowaveStore.createWriter(
				serAdapter,
				TestUtils.DEFAULT_SPATIAL_INDEX)) {
			writer.write(sf);
		}
		final DistributableQuery q = new SpatialQuery(
				((Geometry) args.get(Geometry.class)).buffer(0.5d));
		try (final CloseableIterator<?> iter = geowaveStore.query(
				new QueryOptions(/* TODO do I need to pass 'index'? */),
				q)) {
			boolean foundFeat = false;
			while (iter.hasNext()) {
				final Object maybeFeat = iter.next();
				Assert.assertTrue(
						"Iterator should return simple feature in this test",
						maybeFeat instanceof SimpleFeature);
				foundFeat = true;
				final SimpleFeature isFeat = (SimpleFeature) maybeFeat;
				for (final Property p : isFeat.getProperties()) {
					final Object before = args.get(p.getType().getBinding());
					final Object after = isFeat.getAttribute(p.getType().getName().toString());

					if (before instanceof double[]) {
						Assert.assertTrue(Arrays.equals(
								(double[]) before,
								(double[]) after));
					}
					else if (before instanceof boolean[]) {
						final boolean[] b = (boolean[]) before;
						final boolean[] a = (boolean[]) after;
						Assert.assertTrue(a.length == b.length);
						for (int i = 0; i < b.length; i++) {
							Assert.assertTrue(b[i] == a[i]);
						}
					}
					else if (before instanceof byte[]) {
						Assert.assertArrayEquals(
								(byte[]) before,
								(byte[]) after);
					}
					else if (before instanceof char[]) {
						Assert.assertArrayEquals(
								(char[]) before,
								(char[]) after);
					}
					else if (before instanceof float[]) {
						Assert.assertTrue(Arrays.equals(
								(float[]) before,
								(float[]) after));
					}
					else if (before instanceof int[]) {
						Assert.assertArrayEquals(
								(int[]) before,
								(int[]) after);
					}
					else if (before instanceof long[]) {
						Assert.assertArrayEquals(
								(long[]) before,
								(long[]) after);
					}
					else if (before instanceof short[]) {
						Assert.assertArrayEquals(
								(short[]) before,
								(short[]) after);
					}
					else if (before.getClass().isArray()) {
						Assert.assertArrayEquals(
								returnArray(
										p.getType().getBinding(),
										before),
								returnArray(
										p.getType().getBinding(),
										after));
					}
					else {
						Assert.assertTrue(before.equals(after));
					}
				}
			}
			Assert.assertTrue(
					"One feature should be found",
					foundFeat);
		}

		TestUtils.deleteAll(dataStore);
	}

	public <T> T[] returnArray(
			final Class<T> clazz,
			final Object o ) {
		return (T[]) o;
	}
}
