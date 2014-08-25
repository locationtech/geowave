package mil.nga.giat.geowave.store.data.field;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Calendar;
import java.util.Date;

import mil.nga.giat.geowave.index.ByteArrayId;
import mil.nga.giat.geowave.index.StringUtils;
import mil.nga.giat.geowave.store.GeometryUtils;
import mil.nga.giat.geowave.store.TimeUtils;

import org.apache.commons.lang3.ArrayUtils;

import com.vividsolutions.jts.geom.Geometry;

/**
 * This class contains all of the primitive writer field types supported
 *
 */
public class BasicWriter<RowType, FieldType> implements
		FieldWriter<RowType, FieldType>
{
	private FieldVisibilityHandler<RowType, Object> visibilityHandler;
	private FieldWriter<?, FieldType> writer;

	public BasicWriter(
			final FieldWriter<?, FieldType> writer ) {
		this(
				writer,
				null);
	}

	public BasicWriter(
			final FieldWriter<?, FieldType> writer,
			final FieldVisibilityHandler<RowType, Object> visibilityHandler ) {
		this.writer = writer;
		this.visibilityHandler = visibilityHandler;
	}

	@Override
	public byte[] getVisibility(
			final RowType rowValue,
			final ByteArrayId fieldId,
			final FieldType fieldValue ) {
		if (visibilityHandler != null) {
			return visibilityHandler.getVisibility(
					rowValue,
					fieldId,
					fieldValue);
		}
		return new byte[] {};
	}

	@Override
	public byte[] writeField(
			final FieldType fieldValue ) {
		return writer.writeField(fieldValue);
	}

	public static class BooleanWriter implements
			FieldWriter<Object, Boolean>
	{

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Boolean fieldValue ) {
			return new byte[] {};
		}

		@Override
		public byte[] writeField(
				final Boolean fieldValue ) {
			return new byte[] {
				((fieldValue == null) || !fieldValue) ? (byte) 0 : (byte) 1
			};
		}
	}

	public static class ByteWriter implements
			FieldWriter<Object, Byte>
	{

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Byte fieldValue ) {
			return new byte[] {};
		}

		@Override
		public byte[] writeField(
				final Byte fieldValue ) {
			return new byte[] {
				fieldValue
			};
		}

	}

	public static class ShortWriter implements
			FieldWriter<Object, Short>
	{
		@Override
		public byte[] writeField(
				final Short fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(2);
			buf.putShort(fieldValue);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Short fieldValue ) {
			return new byte[] {};
		}
	}

	public static class PrimitiveShortArrayWriter implements
			FieldWriter<Object, short[]>
	{
		@Override
		public byte[] writeField(
				final short[] fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(2 * fieldValue.length);
			for (final short value : fieldValue) {
				buf.putShort(value);
			}
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final short[] fieldValue ) {
			return new byte[] {};
		}
	}

	public static class FloatWriter implements
			FieldWriter<Object, Float>
	{

		@Override
		public byte[] writeField(
				final Float fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(4);
			buf.putFloat(fieldValue);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Float fieldValue ) {
			return new byte[] {};
		}
	}

	public static class PrimitiveFloatArrayWriter implements
			FieldWriter<Object, float[]>
	{

		@Override
		public byte[] writeField(
				final float[] fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(4 * fieldValue.length);
			for (final float value : fieldValue) {
				buf.putFloat(value);
			}
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final float[] fieldValue ) {
			return new byte[] {};
		}
	}

	public static class DoubleWriter implements
			FieldWriter<Object, Double>
	{
		@Override
		public byte[] writeField(
				final Double fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(8);
			buf.putDouble(fieldValue);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Double fieldValue ) {
			return new byte[] {};
		}
	}

	public static class PrimitiveDoubleArrayWriter implements
			FieldWriter<Object, double[]>
	{
		@Override
		public byte[] writeField(
				final double[] fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(8 * fieldValue.length);
			for (final double value : fieldValue) {
				buf.putDouble(value);
			}
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final double[] fieldValue ) {
			return new byte[] {};
		}
	}

	public static class BigDecimalWriter implements
			FieldWriter<Object, BigDecimal>
	{

		@Override
		public byte[] writeField(
				final BigDecimal fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(8);
			buf.putDouble(fieldValue.doubleValue());
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final BigDecimal fieldValue ) {
			return new byte[] {};
		}
	}

	public static class IntWriter implements
			FieldWriter<Object, Integer>
	{
		@Override
		public byte[] writeField(
				final Integer fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(4);
			buf.putInt(fieldValue);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Integer fieldValue ) {
			return new byte[] {};
		}
	}

	public static class PrimitiveIntArrayWriter implements
			FieldWriter<Object, int[]>
	{
		@Override
		public byte[] writeField(
				final int[] fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(4 * fieldValue.length);
			for (final int value : fieldValue) {
				buf.putInt(value);
			}
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final int[] fieldValue ) {
			return new byte[] {};
		}
	}

	public static class LongWriter implements
			FieldWriter<Object, Long>
	{
		public LongWriter() {
			super();
		}

		@Override
		public byte[] writeField(
				final Long fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(8);
			buf.putLong(fieldValue);
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Long fieldValue ) {
			return new byte[] {};
		}
	}

	public static class PrimitiveLongArrayWriter implements
			FieldWriter<Object, long[]>
	{
		public PrimitiveLongArrayWriter() {
			super();
		}

		@Override
		public byte[] writeField(
				final long[] fieldValue ) {
			final ByteBuffer buf = ByteBuffer.allocate(8 * fieldValue.length);
			for (final long value : fieldValue) {
				buf.putLong(value);
			}
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final long[] fieldValue ) {
			return new byte[] {};
		}
	}

	public static class BigIntegerWriter implements
			FieldWriter<Object, BigInteger>
	{

		@Override
		public byte[] writeField(
				final BigInteger fieldValue ) {
			return fieldValue.toByteArray();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final BigInteger fieldValue ) {
			return new byte[] {};
		}
	}

	public static class StringWriter implements
			FieldWriter<Object, String>
	{
		@Override
		public byte[] writeField(
				final String fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return StringUtils.stringToBinary(fieldValue);
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final String fieldValue ) {
			return new byte[] {};
		}
	}

	public static class GeometryWriter implements
			FieldWriter<Object, Geometry>
	{
		@Override
		public byte[] writeField(
				final Geometry fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return GeometryUtils.geometryToBinary(fieldValue);
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Geometry fieldValue ) {
			return new byte[] {};
		}
	}

	public static class DateWriter implements
			FieldWriter<Object, Date>
	{
		@Override
		public byte[] writeField(
				final Date fieldData ) {
			if (fieldData == null) {
				return new byte[] {};
			}

			final ByteBuffer buf = ByteBuffer.allocate(8);
			buf.putLong(fieldData.getTime());
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Date fieldValue ) {
			return new byte[] {};
		}
	}

	public static class CalendarWriter implements
			FieldWriter<Object, Calendar>
	{
		@Override
		public byte[] writeField(
				final Calendar cal ) {
			if (cal == null) {
				return new byte[] {};
			}
			final ByteBuffer buf = ByteBuffer.allocate(8);
			buf.putLong(TimeUtils.calendarToGMTMillis(cal));
			return buf.array();
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Calendar fieldValue ) {
			return new byte[] {};
		}
	}

	public static class ByteArrayWriter implements
			FieldWriter<Object, Byte[]>
	{
		@Override
		public byte[] writeField(
				final Byte[] fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return ArrayUtils.toPrimitive(fieldValue);
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final Byte[] fieldValue ) {
			return new byte[] {};
		}
	}

	public static class PrimitiveByteArrayWriter implements
			FieldWriter<Object, byte[]>
	{
		@Override
		public byte[] writeField(
				final byte[] fieldValue ) {
			if (fieldValue == null) {
				return new byte[] {};
			}
			return fieldValue;
		}

		@Override
		public byte[] getVisibility(
				final Object rowValue,
				final ByteArrayId fieldId,
				final byte[] fieldValue ) {
			return new byte[] {};
		}
	}
}
