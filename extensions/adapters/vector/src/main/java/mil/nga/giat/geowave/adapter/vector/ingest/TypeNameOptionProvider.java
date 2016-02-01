package mil.nga.giat.geowave.adapter.vector.ingest;

import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.ingest.IngestFormatOptionProvider;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

public class TypeNameOptionProvider implements
		IngestFormatOptionProvider,
		Persistable
{
	private String typename = null;
	private String[] typenames = null;

	@Override
	public void applyOptions(
			final Options allOptions ) {
		allOptions.addOption(
				"typename",
				true,
				"A comma-delimitted set of typenames to ingest, feature types matching the specified typenames will be ingested (optional, by default all types will be ingested)");
	}

	public String getTypeName() {
		return typename;
	}

	public boolean typeNameMatches(
			final String typeName ) {
		String[] internalTypenames;
		synchronized (this) {
			if (typenames == null) {
				typenames = typename.split(",");
			}
			internalTypenames = typenames;
		}
		for (final String t : internalTypenames) {
			if (t.equalsIgnoreCase(typeName)) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void parseOptions(
			final CommandLine commandLine ) {
		if (commandLine.hasOption("typename")) {
			typename = commandLine.getOptionValue("typename");
		}
	}

	@Override
	public byte[] toBinary() {
		if (typename == null) {
			return new byte[] {};
		}
		return StringUtils.stringToBinary(typename);
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		if (bytes.length > 0) {
			typename = StringUtils.stringFromBinary(bytes);
		}
		else {
			typename = null;
		}
	}

}
