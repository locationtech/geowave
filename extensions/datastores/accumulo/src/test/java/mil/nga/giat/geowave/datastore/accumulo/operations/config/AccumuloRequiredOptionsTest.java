/**
 * 
 */
package mil.nga.giat.geowave.datastore.accumulo.operations.config;

import mil.nga.giat.geowave.datastore.accumulo.operations.config.AccumuloRequiredOptions;

import com.beust.jcommander.JCommander;

/**
 * @author mcarrier
 *
 */
public class AccumuloRequiredOptionsTest
{

	/**
	 * @param args
	 */
	public static void main(
			String[] args ) {
		AccumuloRequiredOptions test = new AccumuloRequiredOptions();
		String[] arguments = new String[] {
			"-z",
			"zook123",
			"-i",
			"instance",
			"-u",
			"user",
			"-p",
			"pass:pass1234",
		};
		new JCommander(
				test,
				arguments);
		System.out.println("password: " + test.getPassword());
	}
}