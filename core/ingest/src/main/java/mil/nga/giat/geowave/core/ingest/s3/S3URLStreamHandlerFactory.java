package mil.nga.giat.geowave.core.ingest.s3;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.Optional;

import mil.nga.giat.geowave.core.ingest.local.Handler;

public class S3URLStreamHandlerFactory implements
		URLStreamHandlerFactory
{

	// The wrapped URLStreamHandlerFactory's instance
	private final Optional<URLStreamHandlerFactory> delegate;

	/**
	 * Used in case there is no existing URLStreamHandlerFactory defined
	 */
	public S3URLStreamHandlerFactory() {
		this(
				null);
	}

	/**
	 * Used in case there is an existing URLStreamHandlerFactory defined
	 */
	public S3URLStreamHandlerFactory(
			final URLStreamHandlerFactory delegate ) {
		this.delegate = Optional.ofNullable(delegate);
	}

	@Override
    public URLStreamHandler createURLStreamHandler(final String protocol) {
        if ("s3".equals(protocol)) {
            return new Handler();// my S3 URLStreamHandler;
        }
        // It is not the s3 protocol so we delegate it to the wrapped 
        // URLStreamHandlerFactory
        return delegate.map(factory -> factory.createURLStreamHandler(protocol))
            .orElse(null);
    }
}