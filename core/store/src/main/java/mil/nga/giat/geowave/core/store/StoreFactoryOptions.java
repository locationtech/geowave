package mil.nga.giat.geowave.core.store;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import mil.nga.giat.geowave.core.cli.utils.JCommanderParameterUtils;
import mil.nga.giat.geowave.core.store.operations.remote.options.DataStorePluginOptions;

/**
 * This interface doesn't actually do anything, is just used for tracking during
 * development.
 */
abstract public class StoreFactoryOptions
{
	private final static Logger LOGGER = LoggerFactory.getLogger(StoreFactoryOptions.class);

	public final static String GEOWAVE_NAMESPACE_OPTION = "gwNamespace";
	public final static String GEOWAVE_NAMESPACE_DESCRIPTION = "The geowave namespace (optional; default is no namespace)";
	@Parameter(names = "--" + GEOWAVE_NAMESPACE_OPTION, description = GEOWAVE_NAMESPACE_DESCRIPTION)
	private String geowaveNamespace;

	public String getGeowaveNamespace() {
		return geowaveNamespace;
	}

	public void setGeowaveNamespace(
			final String geowaveNamespace ) {
		this.geowaveNamespace = geowaveNamespace;
	}

	public abstract StoreFactoryFamilySpi getStoreFactory();

	public DataStorePluginOptions createPluginOptions() {
		return new DataStorePluginOptions(
				this);
	}

	/**
	 * Method to perform global validation for all plugin options
	 * 
	 * @throws Exception
	 */
	public void validatePluginOptions()
			throws ParameterException {
		LOGGER.trace("ENTER :: validatePluginOptions()");
		for (Field field : this.getClass().getDeclaredFields()) {
			for (Annotation annotation : field.getAnnotations()) {
				if (annotation.annotationType() == Parameter.class) {
					Parameter parameter = (Parameter) annotation;
					if (JCommanderParameterUtils.isRequired(parameter)) {
						field.setAccessible(true);
						Object value = null;
						try {
							value = field.get(this);
							if (value == null) {
								JCommander.getConsole().println(
										"Field [" + field.getName() + "] is required: "
												+ Arrays.toString(parameter.names()) + ": " + parameter.description());
								JCommander.getConsole().print(
										"Enter value for [" + field.getName() + "]: ");
								char[] password = JCommander.getConsole().readPassword(
										true);
								String strPassword = new String(
										password);
								if (!"".equals(strPassword.trim())) {
									value = (strPassword != null && !"".equals(strPassword.trim())) ? strPassword
											.trim() : null;
								}
								if (value == null) {
									throw new ParameterException(
											"Value for [" + field.getName() + "] cannot be null");
								}
								else {
									field.set(
											this,
											value);
								}
							}
						}
						catch (Exception ex) {
							LOGGER.error(
									"An error occurred validating plugin options for [" + this.getClass().getName()
											+ "]: " + ex.getLocalizedMessage(),
									ex);
						}
					}
				}
			}
		}
	}
}
