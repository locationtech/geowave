package mil.nga.giat.geowave.store.data.field;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryFactory;

public class BasicReaderWriterTest
{

	public static void main(
			String[] args ) {
		BasicReaderWriterTest tester = new BasicReaderWriterTest();
		tester.init();
		tester.testBasicReadWrite();
	}
	
	@Before
	public void init() {
		booleanExpected = Boolean.TRUE;
		byteExpected = Byte.MIN_VALUE;
		shortExpected = Short.MIN_VALUE;
		floatExpected = Float.MIN_VALUE;
		doubleExpected = Double.MIN_VALUE;
		bigDecimalExpected = BigDecimal.TEN;
		integerExpected = Integer.MIN_VALUE;
		longExpected = Long.MIN_VALUE;
		bigIntegerExpected = BigInteger.valueOf(Long.MAX_VALUE);
		stringExpected = this.getClass().getName();
		geometryExpected = new GeometryFactory().createPoint(new Coordinate(25,32));
		dateExpected = new Date();
		calendarExpected = new GregorianCalendar();
		calendarExpected.setTimeZone(TimeZone.getTimeZone("GMT"));
		byteArrayExpected = new Byte [] {Byte.MIN_VALUE, new Byte((byte)55), Byte.MAX_VALUE};
		primByteArrayExpected = new byte [] {Byte.MIN_VALUE, (byte) 33, Byte.MAX_VALUE};
	}

	@Test
	public void testBasicReadWrite() {
		
		byte[] value;
		
		// test Boolean reader/writer
		value = new BasicWriter.BooleanWriter().writeField(booleanExpected);
		Boolean booleanActual = new BasicReader.BooleanReader().readField(value);
		Assert.assertEquals("FAILED test of Boolean reader/writer", booleanExpected.booleanValue(), booleanActual.booleanValue());
		
		// test Byte reader/writer
		value = new BasicWriter.ByteWriter().writeField(byteExpected);
		Byte byteActual = new BasicReader.ByteReader().readField(value);
		Assert.assertEquals("FAILED test of Byte reader/writer", byteExpected.byteValue(), byteActual.byteValue());
		
		// test Short reader/writer
		value = new BasicWriter.ShortWriter().writeField(shortExpected);
		Short shortActual = new BasicReader.ShortReader().readField(value);
		Assert.assertEquals("FAILED test of Short reader/writer", shortExpected.shortValue(), shortActual.shortValue());
		
		// test Float reader/writer
		value = new BasicWriter.FloatWriter().writeField(floatExpected);
		Float floatActual = new BasicReader.FloatReader().readField(value);
		Assert.assertEquals("FAILED test of Float reader/writer", floatExpected, floatActual);
		
		// test Double reader/writer
		value = new BasicWriter.DoubleWriter().writeField(doubleExpected);
		Double doubleActual = new BasicReader.DoubleReader().readField(value);
		Assert.assertEquals("FAILED test of Double reader/writer", doubleExpected, doubleActual);
		
		// test BigDecimal reader/writer
		value = new BasicWriter.BigDecimalWriter().writeField(bigDecimalExpected);
		BigDecimal bigDecimalActual = new BasicReader.BigDecimalReader().readField(value);
		Assert.assertEquals("FAILED test of BigDecimal reader/writer", bigDecimalExpected, bigDecimalActual);
		
		// test Integer reader/writer
		value = new BasicWriter.IntWriter().writeField(integerExpected);
		Integer integerActual = new BasicReader.IntReader().readField(value);
		Assert.assertEquals("FAILED test of Integer reader/writer", integerExpected, integerActual);
		
		// test Long reader/writer
		value = new BasicWriter.LongWriter().writeField(longExpected);
		Long longActual = new BasicReader.LongReader().readField(value);
		Assert.assertEquals("FAILED test of Long reader/writer", longExpected, longActual);
		
		// test BigInteger reader/writer
		value = new BasicWriter.BigIntegerWriter().writeField(bigIntegerExpected);
		BigInteger bigIntegerActual = new BasicReader.BigIntegerReader().readField(value);
		Assert.assertEquals("FAILED test of BigInteger reader/writer", bigIntegerExpected, bigIntegerActual);
		
		// test String reader/writer
		value = new BasicWriter.StringWriter().writeField(stringExpected);
		String stringActual = new BasicReader.StringReader().readField(value);
		Assert.assertEquals("FAILED test of String reader/writer", stringExpected, stringActual);
		
		// test Geometry reader/writer
		value = new BasicWriter.GeometryWriter().writeField(geometryExpected);
		Geometry geometryActual = new BasicReader.GeometryReader().readField(value);
		// TODO develop the "equals" test for Geometry
		Assert.assertEquals("FAILED test of Geometry reader/writer", geometryExpected, geometryActual);
		
		// test Date reader/writer
		value = new BasicWriter.DateWriter().writeField(dateExpected);
		Date dateActual = new BasicReader.DateReader().readField(value);
		Assert.assertEquals("FAILED test of Date reader/writer", dateExpected, dateActual);
		
		// test Calendar reader/writer
		value = new BasicWriter.CalendarWriter().writeField(calendarExpected);
		Calendar calendarActual = new BasicReader.CalendarReader().readField(value);
		Assert.assertEquals("FAILED test of Calendar reader/writer", calendarExpected, calendarActual);
		
		// test Byte [] reader/writer
		value = new BasicWriter.ByteArrayWriter().writeField(byteArrayExpected);
		Byte [] byteArrayActual = new BasicReader.ByteArrayReader().readField(value);		
		Assert.assertTrue("FAILED test of Byte [] reader/writer", Arrays.deepEquals(byteArrayExpected, byteArrayActual));
				
		// test byte [] reader/writer
		value = new BasicWriter.PrimitiveByteArrayWriter().writeField(primByteArrayExpected);
		byte [] primByteArrayActual = new BasicReader.PrimitiveByteArrayReader().readField(value);
		Assert.assertEquals("FAILED test of byte [] reader/writer", primByteArrayExpected, primByteArrayActual);
	}
	
	private Boolean booleanExpected;
	private Byte byteExpected;
	private Short shortExpected;
	private Float floatExpected;
	private Double doubleExpected;
	private BigDecimal bigDecimalExpected;
	private Integer integerExpected;
	private Long longExpected;
	private BigInteger bigIntegerExpected;
	private String stringExpected;
	private Geometry geometryExpected;
	private Date dateExpected;
	private Calendar calendarExpected;
	private Byte [] byteArrayExpected;
	private byte [] primByteArrayExpected;
}
