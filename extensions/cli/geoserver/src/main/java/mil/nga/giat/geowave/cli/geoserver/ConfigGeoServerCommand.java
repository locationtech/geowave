package mil.nga.giat.geowave.cli.geoserver;

import java.io.File;
import java.util.Properties;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

@GeowaveOperation(name = "geoserver", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create a local configuration for GeoServer")
public class ConfigGeoServerCommand implements
		Command
{
	@Parameter(names = {
		"-u",
		"--url"
	}, required = false, description = "GeoServer URL (for example http://localhost:8080/geoserver), or simply host:port and appropriate assumptions are made")
	private String url;

	@Parameter(names = {
		"-n",
		"--name"
	}, required = false, description = "GeoServer User")
	private String name;

	@Parameter(names = {
		"-p",
		"--pass"
	}, required = false, description = "GeoServer Password")
	private String pass;

	@Parameter(names = {
		"-ws",
		"--workspace"
	}, required = false, description = "GeoServer Default Workspace")
	private String workspace;

	@Override
	public boolean prepare(
			OperationParams params ) {
		// Successfully prepared (none needed).
		return true;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		File propFile = (File) params.getContext().get(
				ConfigOptions.PROPERTIES_FILE_CONTEXT);
		Properties existingProps = ConfigOptions.loadProperties(
				propFile,
				null);

		// all switches are optional
		if (getUrl() != null) {
			existingProps.setProperty(
					GeoServerConfig.GEOSERVER_URL,
					getUrl());
		}

		if (getName() != null) {
			existingProps.setProperty(
					GeoServerConfig.GEOSERVER_USER,
					getName());
		}

		if (getPass() != null) {
			existingProps.setProperty(
					GeoServerConfig.GEOSERVER_PASS,
					getPass());
		}

		if (getWorkspace() != null) {
			existingProps.setProperty(
					GeoServerConfig.GEOSERVER_WORKSPACE,
					getWorkspace());
		}

		// Write properties file
		ConfigOptions.writeProperties(
				propFile,
				existingProps);
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(
			String url ) {
		this.url = url;
	}

	public String getName() {
		return name;
	}

	public void setName(
			String name ) {
		this.name = name;
	}

	public String getPass() {
		return pass;
	}

	public void setPass(
			String pass ) {
		this.pass = pass;
	}

	public String getWorkspace() {
		return workspace;
	}

	public void setWorkspace(
			String workspace ) {
		this.workspace = workspace;
	}
}
