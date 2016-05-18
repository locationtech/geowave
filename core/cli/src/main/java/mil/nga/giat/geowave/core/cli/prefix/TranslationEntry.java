package mil.nga.giat.geowave.core.cli.prefix;

import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Method;

import com.beust.jcommander.Parameterized;

/**
 * This helper class is just a tuple that allows us to keep track of the
 * parameters, their translated field names, and the original object they map
 * to.
 */
public class TranslationEntry
{
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
		if (getParam().getParameter() != null && getParam().getParameter().description() != null) {
			description = getParam().getParameter().description();
		}
		else if (getParam().isDynamicParameter()) {
			description = getParam().getWrappedParameter().getDynamicParameter().description();
		}
		return description == null ? "<no description>" : description;
	}

	public boolean isPassword() {
		if (getParam().getParameter() != null) {
			return getParam().getParameter().password();
		}
		else if (getParam().getWrappedParameter() != null) {
			return getParam().getWrappedParameter().password();
		}
		return false;
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
		try {
			Object value = getParam().get(
					getObject());
			return value != null;
		}
		catch (Exception e) {
			return false;
		}
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
