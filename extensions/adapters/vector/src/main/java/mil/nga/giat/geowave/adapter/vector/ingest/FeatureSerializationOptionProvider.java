package mil.nga.giat.geowave.adapter.vector.ingest;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.index.Persistable;

public class FeatureSerializationOptionProvider implements
		Persistable
{
	@Parameter(names = "--avro", description = "A flag to indicate whether avro feature serialization should be used")
	private boolean avro = false;

	public boolean isAvro() {
		return avro;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {
			avro ? (byte) 1 : (byte) 0
		};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if ((bytes != null) && (bytes.length > 0)) {
			if (bytes[0] == 1) {
				avro = true;
			}
		}
	}
}
