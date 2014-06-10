package mil.nga.giat.geowave.store.data.field;

import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.store.data.field.BasicReader.ByteArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.CalendarReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.DateReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.DoubleReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.FloatReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.GeometryReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.IntReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.LongReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.PrimitiveByteArrayReader;
import mil.nga.giat.geowave.store.data.field.BasicReader.StringReader;
import mil.nga.giat.geowave.store.data.field.BasicWriter.ByteArrayWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.CalendarWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.DateWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.DoubleWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.FloatWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.GeometryWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.IntWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.LongWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.PrimitiveByteArrayWriter;
import mil.nga.giat.geowave.store.data.field.BasicWriter.StringWriter;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This class has a set of convenience methods to determine the appropriate
 * field reader and writer for a given field type (Class)
 * 
 */
public class FieldUtils
{
	private static final Map<Class<?>, FieldReader<?>> DEFAULT_READERS = new HashMap<Class<?>, FieldReader<?>>();
	private static final Map<Class<?>, FieldWriter<?, ?>> DEFAULT_WRITERS = new HashMap<Class<?>, FieldWriter<?, ?>>();
	static {
		DEFAULT_READERS.put(
				Float.class,
				new FloatReader());
		DEFAULT_READERS.put(
				Double.class,
				new DoubleReader());
		DEFAULT_READERS.put(
				Integer.class,
				new IntReader());
		DEFAULT_READERS.put(
				Long.class,
				new LongReader());
		DEFAULT_READERS.put(
				Date.class,
				new DateReader());
		DEFAULT_READERS.put(
				String.class,
				new StringReader());
		DEFAULT_READERS.put(
				Geometry.class,
				new GeometryReader());
		DEFAULT_READERS.put(
				Calendar.class,
				new CalendarReader());
		DEFAULT_READERS.put(
				Byte[].class,
				new ByteArrayReader());
		DEFAULT_READERS.put(
				byte[].class,
				new PrimitiveByteArrayReader());

		DEFAULT_WRITERS.put(
				Float.class,
				new FloatWriter());
		DEFAULT_WRITERS.put(
				Double.class,
				new DoubleWriter());
		DEFAULT_WRITERS.put(
				Integer.class,
				new IntWriter());
		DEFAULT_WRITERS.put(
				Long.class,
				new LongWriter());
		DEFAULT_WRITERS.put(
				Date.class,
				new DateWriter());
		DEFAULT_WRITERS.put(
				String.class,
				new StringWriter());
		DEFAULT_WRITERS.put(
				Geometry.class,
				new GeometryWriter());
		DEFAULT_WRITERS.put(
				Calendar.class,
				new CalendarWriter());
		DEFAULT_WRITERS.put(
				Byte[].class,
				new ByteArrayWriter());
		DEFAULT_WRITERS.put(
				byte[].class,
				new PrimitiveByteArrayWriter());

	}

	@SuppressWarnings("unchecked")
	public static <T> FieldReader<T> getDefaultReaderForClass(
			final Class<T> myClass ) {
		// try concrete class
		final FieldReader<T> reader = (FieldReader<T>) DEFAULT_READERS.get(myClass);
		if (reader != null) {
			return reader;
		}
		// if the concrete class lookup failed, try inheritance
		for (final Entry<Class<?>, FieldReader<?>> candidate : DEFAULT_READERS.entrySet()) {
			if (candidate.getKey().isAssignableFrom(
					myClass)) {
				return (FieldReader<T>) candidate.getValue();
			}
		}
		return null;
	}

	@SuppressWarnings("unchecked")
	public static <T> FieldWriter<?, T> getDefaultWriterForClass(
			final Class<T> myClass ) {

		// try concrete class
		final FieldWriter<?, T> writer = (FieldWriter<?, T>) DEFAULT_WRITERS.get(myClass);
		if (writer != null) {
			return writer;
		}// if the concrete class lookup failed, try inheritance
		return (FieldWriter<?, T>) getAssignableValueFromClassMap(
				myClass,
				DEFAULT_WRITERS);
	}

	public static <T> T getAssignableValueFromClassMap(
			final Class<?> myClass,
			final Map<Class<?>, T> classToAssignableValueMap ) {
		// loop through the map to discover the first class that is assignable
		// from myClass
		for (final Entry<Class<?>, T> candidate : classToAssignableValueMap.entrySet()) {
			if (candidate.getKey().isAssignableFrom(
					myClass)) {
				return candidate.getValue();
			}
		}
		return null;
	}

	public static <RowType, FieldType> FieldWriter<RowType, FieldType> getDefaultWriterForClass(
			final Class<FieldType> myClass,
			final FieldVisibilityHandler<RowType, Object> visibilityHandler ) {
		return new BasicWriter<RowType, FieldType>(
				getDefaultWriterForClass(myClass),
				visibilityHandler);
	}
}
