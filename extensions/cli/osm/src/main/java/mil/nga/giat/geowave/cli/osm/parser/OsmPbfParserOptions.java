package mil.nga.giat.geowave.cli.osm.parser;

import com.beust.jcommander.Parameter;

public class OsmPbfParserOptions
{

	@Parameter(names = "--extension", description = "PBF File extension")
	private String extension = ".pbf";

	private String ingestDirectory;

	private String hdfsBasePath;

	private String nameNode;

	public OsmPbfParserOptions() {
		super();
	}

	public String getExtension() {
		return extension;
	}

	public void setExtension(
			String extension ) {
		this.extension = extension;
	}

	public String getIngestDirectory() {
		return ingestDirectory;
	}

	public void setIngestDirectory(
			String ingestDirectory ) {
		this.ingestDirectory = ingestDirectory;
	}

	public String getHdfsBasePath() {
		return hdfsBasePath;
	}

	public void setHdfsBasePath(
			String hdfsBasePath ) {
		this.hdfsBasePath = hdfsBasePath;
	}

	public String getNameNode() {
		return nameNode;
	}

	public void setNameNode(
			String nameNode ) {
		this.nameNode = nameNode;
	}

	public String getNodesBasePath() {
		return hdfsBasePath + "/nodes";
	}

	public String getWaysBasePath() {
		return hdfsBasePath + "/ways";
	}

	public String getRelationsBasePath() {
		return hdfsBasePath + "/relations";
	}
}
