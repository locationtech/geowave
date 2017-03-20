package mil.nga.giat.geowave.adapter.vector.ingest;

import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.ingest.spi.IngestFormatOptionProvider;

public class DataSchemaOptionProvider implements
		Persistable,
		IngestFormatOptionProvider
{
	@Parameter(names = "--extended", description = "A flag to indicate whether extended data format should be used")
	private boolean includeSupplementalFields = false;

	public boolean includeSupplementalFields() {
		return includeSupplementalFields;
	}

	@Override
	public byte[] toBinary() {
		return new byte[] {
			includeSupplementalFields ? (byte) 1 : (byte) 0
		};
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if ((bytes != null) && (bytes.length > 0)) {
			if (bytes[0] == 1) {
				includeSupplementalFields = true;
			}
		}
	}

	/**
	 * 
	 */
	public void setSupplementalFields(
			boolean supplementalFields ) {
		includeSupplementalFields = supplementalFields;
	}

}
