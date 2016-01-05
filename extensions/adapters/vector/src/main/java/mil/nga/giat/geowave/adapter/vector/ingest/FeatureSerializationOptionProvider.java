package mil.nga.giat.geowave.adapter.vector.ingest;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.ingest.index.BaseIndexOptions.PartitionStrategyConverter;

import org.apache.commons.lang3.StringUtils;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

public class FeatureSerializationOptionProvider implements
		Persistable
{
	@Parameter(names = {
		"-serializer"
	}, required = false, description = "The serialization format to use.  Default will be serialization per attribute.", converter = PartitionStrategyConverter.class)
	protected SerializationFormat serializationFormat = SerializationFormat.ATTRIBUTE;

	public SerializationFormat getSerializationFormat() {
		return serializationFormat;
	}

	@Override
	public byte[] toBinary() {
		byte singleByteVal;
		switch (serializationFormat) {
			case AVRO:
				singleByteVal = (byte) 2;
				break;
			case FEATURE:
				singleByteVal = (byte) 1;
				break;
			default:
			case ATTRIBUTE:
				singleByteVal = (byte) 0;
				break;
		}
		return new byte[] {
			singleByteVal
		};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if ((bytes != null) && (bytes.length > 0)) {
			if (bytes[0] == 1) {
				serializationFormat = SerializationFormat.FEATURE;
			}
			if (bytes[0] == 2) {
				serializationFormat = SerializationFormat.AVRO;
			}
		}
		serializationFormat = SerializationFormat.ATTRIBUTE;
	}

	protected static enum SerializationFormat {
		ATTRIBUTE,
		FEATURE,
		AVRO;
		// converter that will be used later
		public static SerializationFormat fromString(
				final String code ) {

			for (final SerializationFormat output : SerializationFormat.values()) {
				if (output.toString().equalsIgnoreCase(
						code)) {
					return output;
				}
			}

			return null;
		}
	}

	public static class SerializationFormatConverter implements
			IStringConverter<SerializationFormat>
	{
		@Override
		public SerializationFormat convert(
				final String value ) {
			final SerializationFormat convertedValue = SerializationFormat.fromString(value);

			if (convertedValue == null) {
				throw new ParameterException(
						"Value '" + value + "' can not be converted to a serialization format. " + "Available values are: " + StringUtils.join(
								SerializationFormat.values(),
								", ").toLowerCase());
			}
			return convertedValue;
		}

	}
}
