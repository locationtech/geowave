package mil.nga.giat.geowave.core.ingest.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.operations.config.ConfigSection;
import mil.nga.giat.geowave.core.cli.operations.config.options.ConfigOptions;
import mil.nga.giat.geowave.mapreduce.operations.ConfigHDFSCommand;

@GeowaveOperation(name = "aws", parentOperation = ConfigSection.class)
@Parameters(commandDescription = "Create a local configuration for aws s3")
public class ConfigAWSCommand extends
		DefaultOperation implements
		Command
{

	public static final String AWS_S3_ENDPOINT_PREFIX = "s3.endpoint";
	public static final String AWS_S3_ENDPOINT_URL = AWS_S3_ENDPOINT_PREFIX + ".url";

	@Parameter(description = "<AWS S3 endpoint URL> (for example s3.amazonaws.com)")
	private List<String> parameters = new ArrayList<String>();
	private String url = null;

	@Override
	public boolean prepare(
			OperationParams params ) {
		boolean retval = true;
		retval |= super.prepare(params);

		return retval;
	}

	@Override
	public void execute(
			OperationParams params )
			throws Exception {
		if (parameters.size() != 1) {
			throw new ParameterException(
					"Requires argument: <AWS S3 endpoint URL>");
		}
		url = parameters.get(0);
		Properties existingProps = getGeoWaveConfigProperties(params);

		// all switches are optional
		if (url != null) {
			existingProps.setProperty(
					AWS_S3_ENDPOINT_URL,
					url);
		}

		// Write properties file
		ConfigOptions.writeProperties(
				getGeoWaveConfigFile(params),
				existingProps,
				this.getClass(),
				AWS_S3_ENDPOINT_PREFIX);
	}

	public static String getS3Url(
			Properties configProperties ) {

		String s3EndpointUrl = configProperties.getProperty(ConfigAWSCommand.AWS_S3_ENDPOINT_URL);
		if (s3EndpointUrl == null) {
			throw new ParameterException(
					"S3 endpoint URL is empty. Config using \"geowave config aws <s3 endpoint url>\"");
		}

		if (!s3EndpointUrl.contains("://")) {
			s3EndpointUrl = "s3://" + s3EndpointUrl;
		}

		return s3EndpointUrl;
	}

	public void setS3UrlParameter(
			String s3EndpointUrl ) {
		parameters = new ArrayList<String>();
		parameters.add(s3EndpointUrl);
	}
}
