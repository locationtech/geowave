/**
 * 
 */
package mil.nga.giat.geowave.core.cli.converters;

/**
 * Extends the password converter class to force required=false
 * 
 * This class will allow support for user's passing in passwords through a
 * variety of ways. Current supported options for passwords include standard
 * password input (pass), an environment variable (env), a file containing the
 * password text (file), a properties file containing the password associated
 * with a specific key (propfile), and the user being prompted to enter the
 * password at command line (stdin). <br/>
 * <br/>
 * Required notation for specifying varying inputs are:
 * <ul>
 * <li><b>pass</b>:&lt;password&gt;</li>
 * <li><b>env</b>:&lt;variable containing the password&gt;</li>
 * <li><b>file</b>:&lt;local file containing the password&gt;</li>
 * <li><b>propfile</b>:&lt;local properties file containing the
 * password&gt;<b>:</b>&lt;property file key&gt;</li>
 * <li><b>stdin</b></li>
 * </ul>
 */
public class OptionalPasswordConverter extends
		PasswordConverter
{
	public OptionalPasswordConverter(
			String optionName ) {
		super(
				optionName);
	}

	@Override
	public String convert(
			String value ) {
		return super.convert(value);
	}

	@Override
	public boolean isPassword() {
		return true;
	}

	@Override
	public boolean isRequired() {
		return false;
	}
}