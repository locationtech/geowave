package mil.nga.giat.geowave.core.cli.api;

import java.util.Properties;

import mil.nga.giat.geowave.core.cli.prefix.JCommanderPropertiesTransformer;

/**
 * This class has some default implementations for the PluginOptions interface,
 * such as saving and loading plugin options.
 */
public abstract class DefaultPluginOptions
{

	/**
	 * This is implemented by the PluginOptions interface by child classes
	 * 
	 * @param qualifier
	 */
	public abstract void selectPlugin(
			String qualifier );

	/**
	 * This is implemented by the PluginOptions interface by child classes
	 * 
	 * @param qualifier
	 */
	public abstract String getType();

	/**
	 * Transform to a map, making all option values live in the "opts"
	 * namespace.
	 * 
	 * @return
	 */
	public void save(
			Properties properties,
			String namespace ) {
		JCommanderPropertiesTransformer jcpt = new JCommanderPropertiesTransformer(
				String.format(
						"%s.opts",
						namespace));
		jcpt.addObject(this);
		jcpt.transformToProperties(properties);
		// Add the entry for the type property.
		String typeProperty = String.format(
				"%s.type",
				namespace);
		properties.setProperty(
				typeProperty,
				getType());
	}

	/**
	 * Transform from a map, reading values that live in the "opts" namespace.
	 * 
	 * @param options
	 */
	public boolean load(
			Properties properties,
			String namespace ) {
		// Get the qualifier.
		String typeProperty = String.format(
				"%s.type",
				namespace);
		String typeValue = properties.getProperty(typeProperty);
		if (typeValue == null) {
			return false;
		}

		if (getType() == null) {
			selectPlugin(typeValue);
		}
		JCommanderPropertiesTransformer jcpt = new JCommanderPropertiesTransformer(
				String.format(
						"%s.opts",
						namespace));
		jcpt.addObject(this);
		jcpt.transformFromProperties(properties);

		return true;
	}

}
