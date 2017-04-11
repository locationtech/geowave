package mil.nga.giat.geowave.core.cli.prefix;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.Locale;
import java.util.ResourceBundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameterized;

import mil.nga.giat.geowave.core.cli.converters.GeoWaveBaseConverter;


/**
 * This helper class is just a tuple that allows us to keep track of the
 * parameters, their translated field names, and the original object they map
 * to.
 */
public class TranslationEntry
{
	private static Logger LOGGER = LoggerFactory.getLogger(TranslationEntry.class);
	
	private final Parameterized param;
	private final Object object;
	private final String prefix;
	private final String[] prefixedNames;
	private final AnnotatedElement member;

	protected TranslationEntry(
			Parameterized param,
			Object object,
			String prefix,
			AnnotatedElement member ) {
		this.param = param;
		this.object = object;
		this.prefix = prefix;
		this.member = member;
		this.prefixedNames = addPrefixToNames();
	}

	public Parameterized getParam() {
		return param;
	}

	public Object getObject() {
		return object;
	}

	public String getPrefix() {
		return prefix;
	}

	public boolean isMethod() {
		return member instanceof Method;
	}

	public AnnotatedElement getMember() {
		return member;
	}

	public String[] getPrefixedNames() {
		return prefixedNames;
	}

	public String getDescription() {
		String description = null;
		// check to see if a description key is specified. If so, perform a lookup in the GeoWave labels 
		// properties for a description to use in place of the command line instance
		if (getParam().getParameter() != null && getParam().getParameter().descriptionKey() != null) {
			String descriptionKey = getParam().getParameter().descriptionKey();
			if (descriptionKey != null && !"".equals(descriptionKey.trim())) {
				descriptionKey = descriptionKey.trim();
				description = getDescriptionFromResourceBundle(descriptionKey);
			}
		}
		else if (getParam().isDynamicParameter() && getParam().getWrappedParameter() != null
				&& getParam().getWrappedParameter().getDynamicParameter() != null) {
			String descriptionKey = getParam().getWrappedParameter().getDynamicParameter().descriptionKey();
			if (descriptionKey != null && !"".equals(descriptionKey.trim())) {
				descriptionKey = descriptionKey.trim();
				description = getDescriptionFromResourceBundle(descriptionKey);
			}
		}
		
		// if no description is set from GeoWave labels properties, use the one set from the field parameter annotation definition
		if (description == null || "".equals(description.trim())) {
			if (getParam().getParameter() != null && getParam().getParameter().description() != null) {
				description = getParam().getParameter().description();
			}
			else if (getParam().isDynamicParameter()) {
				description = getParam().getWrappedParameter().getDynamicParameter().description();
			}
		}
		return description == null ? "<no description>" : description;
	}

	/**
	 * 
	 * @param descriptionKey
	 * @return
	 */
	private String getDescriptionFromResourceBundle(
			final String descriptionKey ) {
		String description = "";
		final String bundleName = "GeoWaveLabels";
		Locale locale=Locale.getDefault();
		final String defaultResourcePath=bundleName+".properties";
		final String localeResourcePath=bundleName+"_"+locale.toString()+".properties";
		if (this.getClass().getResource("/"+defaultResourcePath) != null ||
				this.getClass().getResource("/"+localeResourcePath) != null) {
			
			// associate the default locale to the base properties, rather than the standard resource bundle requiring a separate base 
			// properties (GeoWaveLabels.properties) and a default-locale-specific properties (GeoWaveLabels_en_US.properties)
			ResourceBundle resourceBundle = ResourceBundle.getBundle(bundleName, locale, 
					ResourceBundle.Control.getNoFallbackControl(ResourceBundle.Control.FORMAT_PROPERTIES));
			if (resourceBundle != null) {
				if (resourceBundle.containsKey(descriptionKey)) {
					description = resourceBundle.getString(descriptionKey);
				}
			}
		}
		return description;
	}

	public boolean isPassword() {
		boolean isPassword=false;
		// check if a converter was specified. If so, if the converter is a GeoWaveBaseConverter instance, check the isPassword value of the converter 
		if (getParam().getParameter() != null && getParam().getParameter().converter() != null &&
				getParam().getParameter().converter().getSuperclass().equals(GeoWaveBaseConverter.class)) {
			isPassword=isPassword || getIsPasswordFromGeoWaveBaseConverter(getParam().getParameter().converter());
		}
		else if (getParam().getWrappedParameter() != null && getParam().getWrappedParameter().getParameter().converter().getSuperclass().equals(GeoWaveBaseConverter.class)) {
			isPassword=isPassword || getIsPasswordFromGeoWaveBaseConverter(getParam().getWrappedParameter().getParameter().converter());
		}
		
		if (getParam().getParameter() != null) {
			isPassword=isPassword || getParam().getParameter().password();
		}
		else if (getParam().getWrappedParameter() != null) {
			isPassword=isPassword || getParam().getWrappedParameter().password();
		}
		return isPassword;
	}

	private boolean getIsPasswordFromGeoWaveBaseConverter(Class<? extends IStringConverter<?>> converterClazz) {
		boolean isPassword=false;
		try {
			Constructor<?> ctor = converterClazz.getConstructor(String.class);
			GeoWaveBaseConverter<?> converter = (GeoWaveBaseConverter<?>) ctor.newInstance(new Object[] { "" });
			if (converter!=null) {
				isPassword=isPassword || converter.isPassword();
			}
		} catch (Exception e) {
			LOGGER.error("An error occurred getting isPassword() field from GeoWaveBaseConverter instance: "+e.getLocalizedMessage(), e);
		}
		return isPassword;
	}
	
	public boolean isHidden() {
		if (getParam().getParameter() != null) {
			return getParam().getParameter().hidden();
		}
		else if (getParam().getWrappedParameter() != null) {
			return getParam().getWrappedParameter().hidden();
		}
		return false;
	}

	public boolean isRequired() {
		if (getParam().getParameter() != null) {
			return getParam().getParameter().required();
		}
		else if (getParam().getWrappedParameter() != null) {
			return getParam().getWrappedParameter().required();
		}
		return false;
	}

	/**
	 * Whether the given object has a value specified. If the current value is
	 * non null, then return true.
	 * 
	 * @return
	 */
	public boolean hasValue() {
		Object value = getParam().get(
				getObject());
		return value != null;
	}

	/**
	 * Property name is used to write to properties files, but also to report
	 * option names to Geoserver.
	 * 
	 * @return
	 */
	public String getAsPropertyName() {
		return trimNonAlphabetic(getLongestParam(getPrefixedNames()));
	}

	/**
	 * This function will take the configured prefix (a member variable) and add
	 * it to all the names list.
	 * 
	 * @return
	 */
	private String[] addPrefixToNames() {
		String[] names = null;
		if (param.getParameter() != null) {
			names = param.getParameter().names();
		}
		else {
			names = param.getWrappedParameter().names();
		}
		String[] newNames = new String[names.length];
		for (int i = 0; i < names.length; i++) {
			String item = names[i];
			char subPrefix = item.charAt(0);
			int j = 0;
			while (j < item.length() && item.charAt(j) == subPrefix) {
				j++;
			}
			String prePrefix = item.substring(
					0,
					j);
			item = item.substring(j);
			newNames[i] = String.format(
					"%s%s%s%s",
					prePrefix,
					prefix,
					JCommanderTranslationMap.PREFIX_SEPARATOR,
					item);
		}
		return newNames;
	}

	/**
	 * For all the entries in names(), look for the largest one
	 * 
	 * @param names
	 * @return
	 */
	private String getLongestParam(
			String[] names ) {
		String longest = null;
		for (String name : names) {
			if (longest == null || name.length() > longest.length()) {
				longest = name;
			}
		}
		return longest;
	}

	/**
	 * Remove any non alphabetic character from the beginning of the string. For
	 * example, '--version' will become 'version'.
	 * 
	 * @param str
	 * @return
	 */
	private String trimNonAlphabetic(
			String str ) {
		int i = 0;
		for (i = 0; i < str.length(); i++) {
			if (Character.isAlphabetic(str.charAt(i))) {
				break;
			}
		}
		return str.substring(i);
	}
}
