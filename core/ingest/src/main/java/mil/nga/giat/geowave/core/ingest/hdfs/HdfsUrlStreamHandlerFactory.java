package mil.nga.giat.geowave.core.ingest.hdfs;

import java.net.URLStreamHandler;
import java.net.URLStreamHandlerFactory;
import java.util.Optional;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;

public class HdfsUrlStreamHandlerFactory extends
		FsUrlStreamHandlerFactory
{
	// The wrapped URLStreamHandlerFactory's instance
	private final Optional<URLStreamHandlerFactory> delegate;

	/**
	 * Used in case there is no existing URLStreamHandlerFactory defined
	 */
	public HdfsUrlStreamHandlerFactory() {
		this(
				null);
	}

	/**
	 * Used in case there is an existing URLStreamHandlerFactory defined
	 */
	public HdfsUrlStreamHandlerFactory(
			final URLStreamHandlerFactory delegate ) {
		this.delegate = Optional.ofNullable(delegate);
	}

	@Override
    public URLStreamHandler createURLStreamHandler(final String protocol) {
		
		// FsUrlStreamHandlerFactory impl
		URLStreamHandler urlStreamHandler = super.createURLStreamHandler(protocol);
		
		// See if hadoop handled it
        if (urlStreamHandler != null) {
            return urlStreamHandler; 
        }
        
        // It is not the hdfs protocol so we delegate it to the wrapped URLStreamHandlerFactory
        return delegate.map(factory -> factory.createURLStreamHandler(protocol))
            .orElse(null);
    }
}