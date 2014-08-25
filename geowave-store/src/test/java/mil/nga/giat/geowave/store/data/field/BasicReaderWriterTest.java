package mil.nga.giat.geowave.store.data.field;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import junit.framework.Assert;
import mil.nga.giat.geowave.store.data.field.ArrayReader.CalendarArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayReader.DateArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayReader.DoubleArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayReader.FloatArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayReader.GeometryArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayReader.IntArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayReader.LongArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayReader.ShortArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayReader.StringArrayReader;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.CalendarArrayWriter;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.DateArrayWriter;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.DoubleArrayWriter;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.FloatArrayWriter;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.GeometryArrayWriter;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.IntArrayWriter;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.LongArrayWriter;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.ShortArrayWriter;
import mil.nga.giat.geowave.store.data.field.ArrayWriter.StringArrayWriter;
import mil.nga.giat.geowave.store.data.field.BasicReader.BigDecimalReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.BigIntegerReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.BooleanReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.ByteArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.ByteReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.CalendarReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.DateReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.DoubleReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.FloatReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.GeometryReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.IntReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.LongReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.PrimitiveByteArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.PrimitiveDoubleArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.PrimitiveFloatArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.PrimitiveIntArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.PrimitiveLongArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.PrimitiveShortArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.ShortReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.StringReader;
import mil.nga.giat.geowave.store.data.field.BasicWriter.BigDecimalWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.BigIntegerWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.BooleanWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.ByteArrayWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.ByteWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.CalendarWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.DateWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.DoubleWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.FloatWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.GeometryWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.IntWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.LongWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.PrimitiveByteArrayWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.PrimitiveDoubleArrayWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.PrimitiveFloatArrayWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.PrimitiveIntArrayWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.PrimitiveLongArrayWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.PrimitiveShortArrayWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.ShortWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.StringWriter;

import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class BasicReaderWriterTest
{

	public static void main(
			final String[] args ) {
		final BasicReaderWriterTest tester = new BasicReaderWriterTest();
		tester.init();
		tester.testBasicReadWrite();
	}

	@Before
	public void init() {
		booleanExpected = Boolean.TRUE;
		byteExpected = Byte.MIN_VALUE;
		shortExpected = Short.MIN_VALUE;
		shortArrayExpected = new Short[] {
			Short.MIN_VALUE,
			null,
			Short.MAX_VALUE,
			null
		};
		primShortArrayExpected = new short[] {
			Short.MIN_VALUE,
			Short.MAX_VALUE
		};
		floatExpected = Float.MIN_VALUE;
		floatArrayExpected = new Float[] {
			null,
			Float.MIN_VALUE,
			null,
			Float.MAX_VALUE
		};
		primFloatArrayExpected = new float[] {
			Float.MIN_VALUE,
			Float.MAX_VALUE
		};
		doubleExpected = Double.MIN_VALUE;
		doubleArrayExpected = new Double[] {
			Double.MIN_VALUE,
			null,
			Double.MAX_VALUE,
			null
		};
		primDoubleArrayExpected = new double[] {
			Double.MIN_VALUE,
			Double.MAX_VALUE
		};
		bigDecimalExpected = BigDecimal.TEN;
		integerExpected = Integer.MIN_VALUE;
		intArrayExpected = new Integer[] {
			null,
			Integer.MIN_VALUE,
			null,
			Integer.MAX_VALUE
		};
		primIntArrayExpected = new int[] {
			Integer.MIN_VALUE,
			Integer.MAX_VALUE
		};
		longExpected = Long.MIN_VALUE;
		longArrayExpected = new Long[] {
			Long.MIN_VALUE,
			null,
			Long.MAX_VALUE,
			null
		};
		primLongArrayExpected = new long[] {
			Long.MIN_VALUE,
			Long.MAX_VALUE
		};
		bigIntegerExpected = BigInteger.valueOf(Long.MAX_VALUE);
		stringExpected = this.getClass().getName();
		stringArrayExpected = new String[] {
			null,
			this.getClass().getName(),
			null,
			String.class.getName()
		};
		geometryExpected = new GeometryFactory().createPoint(new Coordinate(
				25,
				32));
		geometryArrayExpected = new Geometry[] {
			new GeometryFactory().createPoint(new Coordinate(
					25,
					32)),
			new GeometryFactory().createPoint(new Coordinate(
					26,
					33)),
			new GeometryFactory().createPoint(new Coordinate(
					27,
					34)),
			new GeometryFactory().createPoint(new Coordinate(
					28,
					35))
		};
		dateExpected = new Date();
		dateArrayExpected = new Date[] {
			new Date(),
			null,
			new Date(
					0),
			null
		};
		calendarExpected = new GregorianCalendar();
		calendarExpected.setTimeZone(TimeZone.getTimeZone("GMT"));
		final Calendar cal1 = new GregorianCalendar();
		cal1.setTimeZone(TimeZone.getTimeZone("GMT"));
		final Calendar cal2 = new GregorianCalendar();
		cal2.setTimeZone(TimeZone.getTimeZone("GMT"));
		calendarArrayExpected = new Calendar[] {
			cal1,
			null,
			cal2,
			null
		};
		byteArrayExpected = new Byte[] {
			Byte.MIN_VALUE,
			new Byte(
					(byte) 55),
			Byte.MAX_VALUE
		};
		primByteArrayExpected = new byte[] {
			Byte.MIN_VALUE,
			(byte) 33,
			Byte.MAX_VALUE
		};
	}

	@Test
	public void testBasicReadWrite() {

		byte[] value;

		// test Boolean reader/writer
		value = new BooleanWriter().writeField(booleanExpected);
		final Boolean booleanActual = new BooleanReader().readField(value);
		Assert.assertEquals(
				"FAILED test of Boolean reader/writer",
				booleanExpected.booleanValue(),
				booleanActual.booleanValue());

		// test Byte reader/writer
		value = new ByteWriter().writeField(byteExpected);
		final Byte byteActual = new ByteReader().readField(value);
		Assert.assertEquals(
				"FAILED test of Byte reader/writer",
				byteExpected.byteValue(),
				byteActual.byteValue());

		// test Short reader/writer
		value = new ShortWriter().writeField(shortExpected);
		final Short shortActual = new ShortReader().readField(value);
		Assert.assertEquals(
				"FAILED test of Short reader/writer",
				shortExpected.shortValue(),
				shortActual.shortValue());

		// test Short Array reader/writer
		value = new ShortArrayWriter().writeField(shortArrayExpected);
		final Short[] shortArrayActual = new ShortArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of Short Array reader/writer",
				Arrays.deepEquals(
						shortArrayExpected,
						shortArrayActual));

		// test short Array reader/writer
		value = new PrimitiveShortArrayWriter().writeField(primShortArrayExpected);
		final short[] primShortArrayActual = new PrimitiveShortArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of short Array reader/writer",
				Arrays.equals(
						primShortArrayExpected,
						primShortArrayActual));

		// test Float reader/writer
		value = new FloatWriter().writeField(floatExpected);
		final Float floatActual = new FloatReader().readField(value);
		Assert.assertEquals(
				"FAILED test of Float reader/writer",
				floatExpected,
				floatActual);

		// test Float Array reader/writer
		value = new FloatArrayWriter().writeField(floatArrayExpected);
		final Float[] floatArrayActual = new FloatArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of Float Array reader/writer",
				Arrays.deepEquals(
						floatArrayExpected,
						floatArrayActual));

		// test float Array reader/writer
		value = new PrimitiveFloatArrayWriter().writeField(primFloatArrayExpected);
		final float[] primFloatArrayActual = new PrimitiveFloatArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of float Array reader/writer",
				Arrays.equals(
						primFloatArrayExpected,
						primFloatArrayActual));

		// test Double reader/writer
		value = new DoubleWriter().writeField(doubleExpected);
		final Double doubleActual = new DoubleReader().readField(value);
		Assert.assertEquals(
				"FAILED test of Double reader/writer",
				doubleExpected,
				doubleActual);

		// test Double Array reader/writer
		value = new DoubleArrayWriter().writeField(doubleArrayExpected);
		final Double[] doubleArrayActual = new DoubleArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of Double Array reader/writer",
				Arrays.deepEquals(
						doubleArrayExpected,
						doubleArrayActual));

		// test double Array reader/writer
		value = new PrimitiveDoubleArrayWriter().writeField(primDoubleArrayExpected);
		final double[] primDoubleArrayActual = new PrimitiveDoubleArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of double Array reader/writer",
				Arrays.equals(
						primDoubleArrayExpected,
						primDoubleArrayActual));

		// test BigDecimal reader/writer
		value = new BigDecimalWriter().writeField(bigDecimalExpected);
		final BigDecimal bigDecimalActual = new BigDecimalReader().readField(value);
		Assert.assertEquals(
				"FAILED test of BigDecimal reader/writer",
				bigDecimalExpected,
				bigDecimalActual);

		// test Integer reader/writer
		value = new IntWriter().writeField(integerExpected);
		final Integer integerActual = new IntReader().readField(value);
		Assert.assertEquals(
				"FAILED test of Integer reader/writer",
				integerExpected,
				integerActual);

		// test Integer Array reader/writer
		value = new IntArrayWriter().writeField(intArrayExpected);
		final Integer[] intArrayActual = new IntArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of Integer Array reader/writer",
				Arrays.deepEquals(
						intArrayExpected,
						intArrayActual));

		// test int Array reader/writer
		value = new PrimitiveIntArrayWriter().writeField(primIntArrayExpected);
		final int[] primIntArrayActual = new PrimitiveIntArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of int Array reader/writer",
				Arrays.equals(
						primIntArrayExpected,
						primIntArrayActual));

		// test Long reader/writer
		value = new LongWriter().writeField(longExpected);
		final Long longActual = new LongReader().readField(value);
		Assert.assertEquals(
				"FAILED test of Long reader/writer",
				longExpected,
				longActual);

		// test Long Array reader/writer
		value = new LongArrayWriter().writeField(longArrayExpected);
		final Long[] longArrayActual = new LongArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of Long Array reader/writer",
				Arrays.deepEquals(
						longArrayExpected,
						longArrayActual));

		// test long Array reader/writer
		value = new PrimitiveLongArrayWriter().writeField(primLongArrayExpected);
		final long[] primLongArrayActual = new PrimitiveLongArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of long Array reader/writer",
				Arrays.equals(
						primLongArrayExpected,
						primLongArrayActual));

		// test BigInteger reader/writer
		value = new BigIntegerWriter().writeField(bigIntegerExpected);
		final BigInteger bigIntegerActual = new BigIntegerReader().readField(value);
		Assert.assertEquals(
				"FAILED test of BigInteger reader/writer",
				bigIntegerExpected,
				bigIntegerActual);

		// test String reader/writer
		value = new StringWriter().writeField(stringExpected);
		final String stringActual = new StringReader().readField(value);
		Assert.assertEquals(
				"FAILED test of String reader/writer",
				stringExpected,
				stringActual);

		// test String Array reader/writer
		value = new StringArrayWriter().writeField(stringArrayExpected);
		final String[] stringArrayActual = new StringArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of String Array reader/writer",
				Arrays.deepEquals(
						stringArrayExpected,
						stringArrayActual));

		// test Geometry reader/writer
		value = new GeometryWriter().writeField(geometryExpected);
		final Geometry geometryActual = new GeometryReader().readField(value);
		// TODO develop the "equals" test for Geometry
		Assert.assertEquals(
				"FAILED test of Geometry reader/writer",
				geometryExpected,
				geometryActual);

		// test Geometry Array reader/writer
		value = new GeometryArrayWriter().writeField(geometryArrayExpected);
		final Geometry[] geometryArrayActual = new GeometryArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of String Array reader/writer",
				Arrays.deepEquals(
						geometryArrayExpected,
						geometryArrayActual));

		// test Date reader/writer
		value = new DateWriter().writeField(dateExpected);
		final Date dateActual = new DateReader().readField(value);
		Assert.assertEquals(
				"FAILED test of Date reader/writer",
				dateExpected,
				dateActual);

		// test Date Array reader/writer
		value = new DateArrayWriter().writeField(dateArrayExpected);
		final Date[] dateArrayActual = new DateArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of Date Array reader/writer",
				Arrays.deepEquals(
						dateArrayExpected,
						dateArrayActual));

		// test Calendar reader/writer
		value = new CalendarWriter().writeField(calendarExpected);
		final Calendar calendarActual = new CalendarReader().readField(value);
		Assert.assertEquals(
				"FAILED test of Calendar reader/writer",
				calendarExpected,
				calendarActual);

		// test Calendar Array reader/writer
		value = new CalendarArrayWriter().writeField(calendarArrayExpected);
		final Calendar[] calendarArrayActual = new CalendarArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of Calendar Array reader/writer",
				Arrays.deepEquals(
						calendarArrayExpected,
						calendarArrayActual));

		// test Byte [] reader/writer
		value = new ByteArrayWriter().writeField(byteArrayExpected);
		final Byte[] byteArrayActual = new ByteArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of Byte [] reader/writer",
				Arrays.deepEquals(
						byteArrayExpected,
						byteArrayActual));

		// test byte [] reader/writer
		value = new PrimitiveByteArrayWriter().writeField(primByteArrayExpected);
		final byte[] primByteArrayActual = new PrimitiveByteArrayReader().readField(value);
		Assert.assertTrue(
				"FAILED test of byte [] reader/writer",
				Arrays.equals(
						primByteArrayExpected,
						primByteArrayActual));
	}

	private Boolean booleanExpected;
	private Byte byteExpected;
	private Short shortExpected;
	private Short[] shortArrayExpected;
	private short[] primShortArrayExpected;
	private Float floatExpected;
	private Float[] floatArrayExpected;
	private float[] primFloatArrayExpected;
	private Double doubleExpected;
	private Double[] doubleArrayExpected;
	private double[] primDoubleArrayExpected;
	private BigDecimal bigDecimalExpected;
	private Integer integerExpected;
	private Integer[] intArrayExpected;
	private int[] primIntArrayExpected;
	private Long longExpected;
	private Long[] longArrayExpected;
	private long[] primLongArrayExpected;
	private BigInteger bigIntegerExpected;
	private String stringExpected;
	private String[] stringArrayExpected;
	private Geometry geometryExpected;
	private Geometry[] geometryArrayExpected;
	private Date dateExpected;
	private Date[] dateArrayExpected;
	private Calendar calendarExpected;
	private Calendar[] calendarArrayExpected;
	private Byte[] byteArrayExpected;
	private byte[] primByteArrayExpected;
}
