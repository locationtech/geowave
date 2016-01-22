package mil.nga.giat.geowave.adapter.vector.ingest;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;

public class DataSchemaOptionProvider implements
		IngestFormatOptionProvider,
		Persistable
{

	private boolean includeSupplementalFields = false;
	private final static String SUPPLEMENTAL_OPTION = "extended";

	@Override
	public void applyOptions(
			final Options allOptions ) {
		allOptions.addOption(
				SUPPLEMENTAL_OPTION,
				false,
				"A flag to indicate whether extended data format should be used");
	}

	@Override
	public void parseOptions(
			final CommandLine commandLine ) {
		if (commandLine.hasOption(SUPPLEMENTAL_OPTION)) {
			includeSupplementalFields = true;
		}
	}

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

}
