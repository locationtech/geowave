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
			String arg0,
			Throwable arg1,
			boolean arg2,
			boolean arg3 ) {
		super(
				arg0,
				arg1,
				arg2,
				arg3);
	}

	public MatchingCentroidNotFoundException(
			String arg0,
			Throwable arg1 ) {
		super(
				arg0,
				arg1);
	}

	public MatchingCentroidNotFoundException(
			String arg0 ) {
		super(
				arg0);
	}

	public MatchingCentroidNotFoundException(
			Throwable arg0 ) {
		super(
				arg0);
	}

}
