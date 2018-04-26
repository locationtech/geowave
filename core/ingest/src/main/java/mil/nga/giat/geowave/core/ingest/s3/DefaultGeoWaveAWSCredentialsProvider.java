package mil.nga.giat.geowave.core.ingest.s3;

import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AnonymousAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;

class DefaultGeoWaveAWSCredentialsProvider extends
		DefaultAWSCredentialsProviderChain
{

	@Override
	public AWSCredentials getCredentials() {
		try {
			return super.getCredentials();
		}
		catch (final SdkClientException exception) {

		}
		// fall back to anonymous credentials
		return new AnonymousAWSCredentials();
	}

}
