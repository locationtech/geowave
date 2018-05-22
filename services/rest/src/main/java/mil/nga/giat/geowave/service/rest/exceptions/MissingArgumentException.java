package mil.nga.giat.geowave.service.rest.exceptions;

public class MissingArgumentException extends
		Exception
{

	private static final long serialVersionUID = 1L;

	public MissingArgumentException(
			final String argumentName ) {
		super(
				"Missing argument: " + argumentName);
	}
}
