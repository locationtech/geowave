/**
 * 
 */
package mil.nga.giat.geowave.core.cli.operations.config.security;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import mil.nga.giat.geowave.core.cli.annotations.GeowaveOperation;
import mil.nga.giat.geowave.core.cli.api.Command;
import mil.nga.giat.geowave.core.cli.api.DefaultOperation;
import mil.nga.giat.geowave.core.cli.api.OperationParams;
import mil.nga.giat.geowave.core.cli.converters.DecryptionConverter;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

@GeowaveOperation(name = "decrypt", parentOperation = SecuritySection.class)
@Parameters(commandDescription = "Decrypts a hex-encoded value. Value can be specified as either "
		+ "-value <clear text value>, or -secure <be prompted for value to avoid it showing in terminal history>")
public class DecryptValueCommand extends
		DefaultOperation implements
		Command
{
	private final static Logger sLog = LoggerFactory.getLogger(DecryptValueCommand.class);

	@Parameter(names = {
		"-value"
	}, description = "Value to decrypt", required = true, converter = DecryptionConverter.class)
	private String value;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {

		sLog.info("Decrypting hex-encoded value");
		if (getValue() != null && !"".equals(getValue().trim())) {
			String decryptedValue = new SecurityUtils().decryptHexEncodedValue(getValue());
			System.out.println("decrypted: " + decryptedValue);
		}
	}

	public String getValue() {
		return value;
	}

	public void setValue(
			final String value ) {
		this.value = value;
	}
}