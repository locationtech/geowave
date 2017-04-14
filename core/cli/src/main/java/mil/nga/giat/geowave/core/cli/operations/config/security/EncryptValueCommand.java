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
import mil.nga.giat.geowave.core.cli.converters.EncryptionConverter;
import mil.nga.giat.geowave.core.cli.operations.config.security.utils.SecurityUtils;

@GeowaveOperation(name = "encrypt", parentOperation = SecuritySection.class)
@Parameters(commandDescription = "Encrypts and hex-encodes value. Value can be specified as either "
		+ "-value <clear text value>, or -secure <be prompted for value, to avoid it showing in terminal history>")
public class EncryptValueCommand extends
		DefaultOperation implements
		Command
{
	private final static Logger sLog = LoggerFactory.getLogger(EncryptValueCommand.class);

	@Parameter(names = {
		"-value"
	}, description = "Value to encrypt", converter = EncryptionConverter.class)
	private String value;

	@Override
	public void execute(
			OperationParams params )
			throws Exception {

		sLog.info("Encrypting and hex-encoding value");
		if (getValue() != null && !"".equals(getValue().trim())) {
			String encryptedValue = new SecurityUtils().encryptAndHexEncodeValue(getValue());
			System.out.println("encrypted: " + encryptedValue);
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