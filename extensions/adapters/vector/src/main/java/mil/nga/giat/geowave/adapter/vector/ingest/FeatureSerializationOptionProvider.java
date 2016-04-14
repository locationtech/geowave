package mil.nga.giat.geowave.adapter.vector.ingest;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.index.Persistable;

public class FeatureSerializationOptionProvider implements
		Persistable
{
	@Parameter(names = "--whole", description = "A flag to indicate whether whole feature serialization should be used")
	private boolean whole = false;

	@Parameter(names = "--avro", description = "A flag to indicate whether avro feature serialization should be used")
	private boolean avro = false;

	public boolean isWhole() {
		return whole;
	}

	public boolean isAvro() {
		return avro;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {
			whole ? (byte) 1 : avro ? (byte) 2 : (byte) 0
		};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if ((bytes != null) && (bytes.length > 0)) {
			if (bytes[0] == 1) {
				whole = true;
			}
			if (bytes[0] == 2) {
				avro = true;
			}
		}
	}
}
