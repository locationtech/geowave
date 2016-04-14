package mil.nga.giat.geowave.adapter.vector.export;

import java.io.File;

public class VectorLocalExportOptions extends
		VectorExportOptions
{
	// TODO annotate appropriately when new commandline tools is merged
	private File outputFile = new File(
			"");

	public File getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(
			File outputFile ) {
		this.outputFile = outputFile;
	}

}
