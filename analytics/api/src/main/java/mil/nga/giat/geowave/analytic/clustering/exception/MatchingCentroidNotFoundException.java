package mil.nga.giat.geowave.analytic.clustering.exception;

public class MatchingCentroidNotFoundException extends
		Exception
{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public MatchingCentroidNotFoundException() {
		super();
	}

	public MatchingCentroidNotFoundException(
			final String arg0,
			final Throwable arg1,
			final boolean arg2,
			final boolean arg3 ) {
		super(
				arg0,
				arg1,
				arg2,
				arg3);
	}

	public MatchingCentroidNotFoundException(
			final String arg0,
			final Throwable arg1 ) {
		super(
				arg0,
				arg1);
	}

	public MatchingCentroidNotFoundException(
			final String arg0 ) {
		super(
				arg0);
	}

	public MatchingCentroidNotFoundException(
			final Throwable arg0 ) {
		super(
				arg0);
	}

}
