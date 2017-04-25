/**
 * 
 */
package mil.nga.giat.geowave.core.cli.converters;

/**
 * This converter does nothing other than ensure that a required field is setup.
 * Using this - over the standard JCommander 'required=true' - allows a user to
 * be prompted for the field, rather than always throwing an error (i.e. a more
 * gracious way of reporting the error)
 *
 */
public class RequiredFieldConverter extends
		GeoWaveBaseConverter<String>
{

	public RequiredFieldConverter(
			String optionName ) {
		super(
				optionName);
	}

	@Override
	public String convert(
			String value ) {
		return value;
	}

	@Override
	public boolean isRequired() {
		return true;
	}
}