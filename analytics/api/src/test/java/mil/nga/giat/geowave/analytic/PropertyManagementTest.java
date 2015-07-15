package mil.nga.giat.geowave.analytic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import mil.nga.giat.geowave.analytic.PropertyManagement;
import mil.nga.giat.geowave.analytic.extract.EmptyDimensionExtractor;
import mil.nga.giat.geowave.analytic.param.ExtractParameters;
import mil.nga.giat.geowave.analytic.param.GlobalParameters;
import mil.nga.giat.geowave.analytic.param.ParameterEnum;
import mil.nga.giat.geowave.analytic.param.InputParameters.Input;
import mil.nga.giat.geowave.core.geotime.store.query.SpatialQuery;
import mil.nga.giat.geowave.core.store.query.DistributableQuery;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class PropertyManagementTest
{
	final GeometryFactory factory = new GeometryFactory();

	@Test
	public void testBulk()
			throws Exception {
		final PropertyManagement pm = new PropertyManagement();

		pm.store(
				new ParameterEnum[] {
					ExtractParameters.Extract.DATA_NAMESPACE_URI
				},
				new Serializable[] {
					"file:///foo"
				});

	}

	@Test
	public void testClass()
			throws Exception {
		final PropertyManagement pm = new PropertyManagement();

		pm.store(
				new ParameterEnum[] {
					ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS
				},
				new Serializable[] {
					"mil.nga.giat.geowave.analytic.extract.EmptyDimensionExtractor"
				});

		assertEquals(
				EmptyDimensionExtractor.class,
				pm.getPropertyAsClass(ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testClassFailure() {
		final PropertyManagement pm = new PropertyManagement();
		pm.store(

				new ParameterEnum[] {
					ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS
				},
				new Serializable[] {
					"mil.nga.giat.geowave.analytic.distance.CoordinateCircleDistanceFn"
				});
		pm.getPropertyAsClass(ExtractParameters.Extract.DIMENSION_EXTRACT_CLASS);
	}

	@Test
	public void testQuery()
			throws Exception {

		final Geometry testGeoFilter = factory.createPolygon(new Coordinate[] {
			new Coordinate(
					24,
					33),
			new Coordinate(
					28,
					33),
			new Coordinate(
					28,
					31),
			new Coordinate(
					24,
					31),
			new Coordinate(
					24,
					33)
		});
		final SpatialQuery sq = new SpatialQuery(
				testGeoFilter);
		final PropertyManagement pm = new PropertyManagement();
		pm.store(
				ExtractParameters.Extract.QUERY,
				sq);
		final DistributableQuery q = pm.getPropertyAsQuery(ExtractParameters.Extract.QUERY);
		assertNotNull(q);
		assertNotNull(((SpatialQuery) q).getQueryGeometry());
		assertEquals(
				"POLYGON ((24 33, 28 33, 28 31, 24 31, 24 33))",
				((SpatialQuery) q).getQueryGeometry().toText());

		pm.store(
				ExtractParameters.Extract.QUERY,
				q);
		final DistributableQuery q1 = (DistributableQuery) pm.getPropertyAsPersistable(ExtractParameters.Extract.QUERY);
		assertNotNull(((SpatialQuery) q1).getQueryGeometry());
		assertEquals(
				"POLYGON ((24 33, 28 33, 28 31, 24 31, 24 33))",
				((SpatialQuery) q1).getQueryGeometry().toText());
	}

	@Test
	public void testPath()
			throws Exception {
		final PropertyManagement pm = new PropertyManagement();
		final Path path1 = new Path(
				"http://java.sun.com/j2se/1.3/foo");
		pm.store(
				Input.HDFS_INPUT_PATH,
				path1);
		final Path path2 = pm.getPropertyAsPath(Input.HDFS_INPUT_PATH);
		assertEquals(
				path1,
				path2);
		pm.store(
				Input.HDFS_INPUT_PATH,
				"x/y/z");
		assertEquals(
				new Path(
						"x/y/z"),
				pm.getPropertyAsPath(Input.HDFS_INPUT_PATH));
	}

	@Test
	public void testOption() {
		final Set<Option> options = new HashSet<Option>();

		GlobalParameters.fillOptions(
				options,
				new GlobalParameters.Global[] {
					GlobalParameters.Global.ACCUMULO_INSTANCE
				});
		assertEquals(
				4,
				options.size());
		PropertyManagement.removeOption(
				options,
				GlobalParameters.Global.ACCUMULO_INSTANCE);
		assertEquals(
				3,
				options.size());
	}

	public static class NonSerializableExample
	{
		int v = 1;
	}

	enum MyLocalNSEnum
			implements
			ParameterEnum {

		ARG1;

		@Override
		public Class<?> getBaseClass() {
			return NonSerializableExample.class;
		}

		@Override
		public Enum<?> self() {
			return this;
		}
	}

	@Test
	public void testOtherConverter()
			throws Exception {
		final PropertyManagement.PropertyConverter<NonSerializableExample> converter = new PropertyManagement.PropertyConverter<NonSerializableExample>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Serializable convert(
					final NonSerializableExample ob )
					throws Exception {
				return Integer.valueOf(1);
			}

			@Override
			public NonSerializableExample convert(
					final Serializable ob )
					throws Exception {
				assertTrue(ob instanceof Integer);
				return new NonSerializableExample();
			}

			@Override
			public Class<NonSerializableExample> baseClass() {
				return NonSerializableExample.class;
			}

		};
		final PropertyManagement pm = new PropertyManagement(
				new PropertyManagement.PropertyConverter[] {
					converter
				},
				new ParameterEnum[] {
					MyLocalNSEnum.ARG1
				},
				new Object[] {
					new NonSerializableExample()
				});
		assertTrue(pm.getProperty(
				MyLocalNSEnum.ARG1,
				converter) instanceof NonSerializableExample);
	}

	@Test
	public void testStore()
			throws Exception {
		final PropertyManagement pm = new PropertyManagement();
		pm.store(
				ExtractParameters.Extract.ADAPTER_ID,
				"bar");
		assertEquals(
				"bar",
				pm.getPropertyAsString(ExtractParameters.Extract.ADAPTER_ID));

		final Path path1 = new Path(
				"http://java.sun.com/j2se/1.3/foo");
		pm.store(
				Input.HDFS_INPUT_PATH,
				path1);

		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (ObjectOutputStream os = new ObjectOutputStream(
				bos)) {
			os.writeObject(pm);
		}
		final ByteArrayInputStream bis = new ByteArrayInputStream(
				bos.toByteArray());
		try (ObjectInputStream is = new ObjectInputStream(
				bis)) {
			final PropertyManagement pm2 = (PropertyManagement) is.readObject();
			assertEquals(
					"bar",
					pm2.getPropertyAsString(ExtractParameters.Extract.ADAPTER_ID));
			assertEquals(
					path1,
					pm2.getPropertyAsPath(Input.HDFS_INPUT_PATH));
		}
	}

	enum MyLocalBoolEnum
			implements
			ParameterEnum {

		BOOLEAN_ARG1,
		BOOLEAN_ARG2;

		@Override
		public Class<?> getBaseClass() {
			return Boolean.class;
		}

		@Override
		public Enum<?> self() {
			return this;
		}
	}

	@Test
	public void testCommandLine()
			throws ParseException {
		final PropertyManagement pm = new PropertyManagement();
		final Options options = new Options();
		options.addOption(PropertyManagement.newOption(
				ExtractParameters.Extract.ADAPTER_ID,
				"id",
				"test id",
				true));
		options.addOption(PropertyManagement.newOption(
				MyLocalBoolEnum.BOOLEAN_ARG1,
				"mi",
				"test id",
				false));
		options.addOption(PropertyManagement.newOption(
				MyLocalBoolEnum.BOOLEAN_ARG2,
				"rd",
				"test id",
				false));
		final BasicParser parser = new BasicParser();
		final CommandLine commandLine = parser.parse(
				options,
				new String[] {
					"-id",
					"y",
					"-rd"
				});
		pm.buildFromOptions(commandLine);

		assertTrue(pm.getPropertyAsBoolean(
				MyLocalBoolEnum.BOOLEAN_ARG2,
				false));
		assertFalse(pm.getPropertyAsBoolean(
				MyLocalBoolEnum.BOOLEAN_ARG1,
				false));
		assertEquals(
				"y",
				pm.getPropertyAsString(ExtractParameters.Extract.ADAPTER_ID));
	}

	@Test
	public void testIO()
			throws Exception {
		final PropertyManagement pm = new PropertyManagement();
		pm.store(
				ExtractParameters.Extract.QUERY,
				"POLYGON ((24 33, 28 33, 28 31, 24 31, 24 33))");
		pm.store(
				Input.HDFS_INPUT_PATH,
				"file:///foo");
		byte[] result = null;
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream()) {
			pm.toOutput(bos);
			bos.flush();
			result = bos.toByteArray();
		}

		try (ByteArrayInputStream bis = new ByteArrayInputStream(
				result)) {
			pm.fromInput(bis);
		}
		final DistributableQuery q = pm.getPropertyAsQuery(ExtractParameters.Extract.QUERY);
		assertEquals(
				"POLYGON ((24 33, 28 33, 28 31, 24 31, 24 33))",
				((SpatialQuery) q).getQueryGeometry().toText());

		final Path path1 = new Path(
				"http://java.sun.com/j2se/1.3/foo");

		final Properties props = new Properties();
		props.put(
				"input-hdfs-input-path",
				path1.toUri().toString());
		pm.fromProperties(props);
		final Path path2 = pm.getPropertyAsPath(Input.HDFS_INPUT_PATH);
		assertEquals(
				path1,
				path2);
	}

	@Test
	public void testStoreWithEmbedded()
			throws Exception {
		final PropertyManagement pm1 = new PropertyManagement();
		pm1.store(
				ExtractParameters.Extract.ADAPTER_ID,
				"bar");
		assertEquals(
				"bar",
				pm1.getPropertyAsString(ExtractParameters.Extract.ADAPTER_ID));

		final PropertyManagement pm2 = new PropertyManagement(
				pm1);

		assertTrue(pm2.hasProperty(ExtractParameters.Extract.ADAPTER_ID));

		final Path path1 = new Path(
				"http://java.sun.com/j2se/1.3/foo");
		pm2.store(
				Input.HDFS_INPUT_PATH,
				path1);

		final ByteArrayOutputStream bos = new ByteArrayOutputStream();
		try (ObjectOutputStream os = new ObjectOutputStream(
				bos)) {
			os.writeObject(pm2);
		}
		final ByteArrayInputStream bis = new ByteArrayInputStream(
				bos.toByteArray());
		try (ObjectInputStream is = new ObjectInputStream(
				bis)) {
			final PropertyManagement pm3 = (PropertyManagement) is.readObject();
			assertEquals(
					"bar",
					pm3.getPropertyAsString(ExtractParameters.Extract.ADAPTER_ID));
			assertEquals(
					path1,
					pm3.getPropertyAsPath(Input.HDFS_INPUT_PATH));
		}
	}
}
