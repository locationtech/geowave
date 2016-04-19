package mil.nga.giat.geowave.adapter.vector.export;

import java.io.File;

import com.beust.jcommander.Parameter;

public class VectorLocalExportOptions extends
		VectorExportOptions
{
	@Parameter(names = "--outputFile", required = true)
	private File outputFile;

	public File getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(
			File outputFile ) {
		this.outputFile = outputFile;
	}

}
