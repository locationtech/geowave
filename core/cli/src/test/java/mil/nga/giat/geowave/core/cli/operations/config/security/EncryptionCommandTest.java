/**
 * 
 */
package mil.nga.giat.geowave.core.cli.operations.config.security;

import com.beust.jcommander.JCommander;

/**
 * @author mcarrier
 *
 */
public class EncryptionCommandTest
{

	public static void main(
			String[] args ) {
		EncryptValueCommand command = new EncryptValueCommand();
		String[] arguments = new String[] {
			"-value"
		};
		new JCommander(
				command,
				arguments);
		System.out.println("value: " + command.getValue());
	}
}
