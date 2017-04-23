package mil.nga.giat.geowave.cli.osm.operations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.ParametersDelegate;

import mil.nga.giat.geowave.cli.osm.parser.OsmPbfParser;
import mil.nga.giat.geowave.cli.osm.parser.OsmPbfParserOptions;
import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;

@GeowaveOperation(name = "stage", parentOperation = OSMSection.class)
@Parameters(commandDescription = "Stage OSM data to HDFS")
public class StageOSMToHDFSCommand extends
		DefaultOperation implements
		Command
{

	@Parameter(description = "<file or directory> <hdfs host:port> <path to base directory to write to>")
	private List<String> parameters = new ArrayList<String>();

	@ParametersDelegate
	private OsmPbfParserOptions parserOptions = new OsmPbfParserOptions();

	@Override
	public void execute(
			OperationParams params )
			throws Exception {

		// Ensure we have all the required arguments
		if (parameters.size() != 3) {
			throw new ParameterException(
					"Requires arguments: <file or directory> <hdfs host:port> <path to base directory to write to>");
		}

		String inputPath = parameters.get(0);
		String hdfsHostPort = parameters.get(1);
		String basePath = parameters.get(2);

		// Ensures that the url starts with hdfs://
		if (!hdfsHostPort.contains("://")) {
			hdfsHostPort = "hdfs://" + hdfsHostPort;
		}

		if (!basePath.startsWith("/")) {
			throw new ParameterException(
					"HDFS Base path must start with forward slash /");
		}

		// These are set as main parameter arguments, to keep consistency with
		// GeoWave.
		parserOptions.setIngestDirectory(inputPath);
		parserOptions.setHdfsBasePath(basePath);
		parserOptions.setNameNode(hdfsHostPort);

		OsmPbfParser osmPbfParser = new OsmPbfParser();
		Configuration conf = osmPbfParser.stageData(parserOptions);

		ContentSummary cs = getHDFSFileSummary(
				conf,
				basePath);
		System.out.println("**************************************************");
		System.out.println("Directories: " + cs.getDirectoryCount());
		System.out.println("Files: " + cs.getFileCount());
		System.out.println("Nodes size: " + getHDFSFileSummary(
				conf,
				parserOptions.getNodesBasePath()).getLength());
		System.out.println("Ways size: " + getHDFSFileSummary(
				conf,
				parserOptions.getWaysBasePath()).getLength());
		System.out.println("Relations size: " + getHDFSFileSummary(
				conf,
				parserOptions.getRelationsBasePath()).getLength());
		System.out.println("**************************************************");
		System.out.println("finished osmpbf ingest");
	}

	public List<String> getParameters() {
		return parameters;
	}

	public void setParameters(
			String fileOrDirectory,
			String hdfsHostPort,
			String hdfsPath ) {
		parameters = new ArrayList<String>();
		parameters.add(fileOrDirectory);
		parameters.add(hdfsHostPort);
		parameters.add(hdfsPath);
	}

	public OsmPbfParserOptions getParserOptions() {
		return parserOptions;
	}

	public void setParserOptions(
			OsmPbfParserOptions parserOptions ) {
		this.parserOptions = parserOptions;
	}

	private static ContentSummary getHDFSFileSummary(
			Configuration conf,
			String filename )
			throws IOException {
		org.apache.hadoop.fs.Path path = new org.apache.hadoop.fs.Path(
				filename);
		FileSystem file = path.getFileSystem(conf);
		ContentSummary cs = file.getContentSummary(path);
		file.close();
		return cs;
	}

	@Override
	protected Object computeResults(
			OperationParams params ) {
		// TODO Auto-generated method stub
		return null;
	}
}
