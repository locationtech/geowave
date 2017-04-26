/**
 * 
 */
package mil.nga.giat.geowave.core.cli.utils;

import java.lang.reflect.Constructor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.Parameter;

import mil.nga.giat.geowave.core.cli.converters.GeoWaveBaseConverter;

/**
 *
 */
public class JCommanderParameterUtils
{
	private static Logger LOGGER = LoggerFactory.getLogger(JCommanderParameterUtils.class);

	public static boolean isPassword(
			Parameter parameter ) {
		boolean isPassword = false;
		if (parameter != null) {
			Class<?> superClass = null;
			Class<? extends IStringConverter<?>> converterClass = parameter.converter();
			if (converterClass != null) {
				superClass = converterClass.getSuperclass();
				while (superClass != null && superClass != GeoWaveBaseConverter.class) {
					superClass = superClass.getSuperclass();
				}
			}

			if (superClass != null && superClass.equals(GeoWaveBaseConverter.class)) {
				GeoWaveBaseConverter<?> converter = getParameterBaseConverter(parameter);
				if (converter != null) {
					isPassword = isPassword || converter.isPassword();
				}
			}
			isPassword = isPassword || parameter.password();
		}
		return isPassword;
	}

	public static boolean isRequired(
			Parameter parameter ) {
		boolean isRequired = false;
		if (parameter != null) {
			if (parameter.converter() != null && parameter.converter().getSuperclass().equals(
					GeoWaveBaseConverter.class)) {
				GeoWaveBaseConverter<?> converter = getParameterBaseConverter(parameter);
				if (converter != null) {
					isRequired = isRequired || converter.isRequired();
				}
			}
			isRequired = isRequired || parameter.required();
		}
		return isRequired;
	}

	private static GeoWaveBaseConverter<?> getParameterBaseConverter(
			Parameter parameter ) {
		GeoWaveBaseConverter<?> converter = null;
		try {
			Constructor<?> ctor = parameter.converter().getConstructor(
					String.class);
			if (ctor != null) {
				converter = (GeoWaveBaseConverter<?>) ctor.newInstance(new Object[] {
					""
				});
			}
		}
		catch (Exception e) {
			LOGGER.error(
					"An error occurred getting converter from parameter: " + e.getLocalizedMessage(),
					e);
		}
		return converter;
	}
}