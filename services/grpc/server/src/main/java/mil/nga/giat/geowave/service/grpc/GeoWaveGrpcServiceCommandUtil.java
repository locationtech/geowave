package mil.nga.giat.geowave.service.grpc;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParametersDelegate;
import com.google.protoshadebuf3.Descriptors.FieldDescriptor;

import mil.nga.giat.geowave.analytic.mapreduce.operations.KdeCommand;
import mil.nga.giat.geowave.core.cli.api.ServiceEnabledCommand;

public class GeoWaveGrpcServiceCommandUtil
{

	private static final Logger LOGGER = LoggerFactory.getLogger(GeoWaveGrpcServiceCommandUtil.class.getName());

	static void SetGrpcToCommandFields(
			Map<FieldDescriptor, Object> m,
			ServiceEnabledCommand cmd ) {
		for (Map.Entry<FieldDescriptor, Object> entry : m.entrySet()) {
			try {
				mapFieldValue(
						cmd,
						cmd.getClass(),
						entry);
			}
			catch (final IOException e) {
				LOGGER.error(
						"Exception encountered setting fields on command",
						e);
			}
			catch (IllegalArgumentException e) {
				LOGGER.error(
						"Exception encountered setting fields on command",
						e);
			}
			catch (IllegalAccessException e) {
				LOGGER.error(
						"Exception encountered setting fields on command",
						e);
			}
		}
	}

	private static void mapFieldValue(
			Object cmd,
			final Class<?> cmdClass,
			Map.Entry<FieldDescriptor, Object> entry )
			throws IOException,
			IllegalArgumentException,
			IllegalAccessException {

		try {
			System.out.println("Class = " + cmdClass);
			Field currField = cmdClass.getDeclaredField(entry.getKey().getName());
			currField.setAccessible(true);
			currField.set(
					cmd,
					entry.getValue());
		}
		catch (final NoSuchFieldException e) {
			// scan the parameters delegates for the field if it could not be
			// found
			// as a stand-alone member
			Field[] fields = cmdClass.getDeclaredFields();
			for (int i = 0; i < fields.length; i++) {
				if (fields[i].isAnnotationPresent(ParametersDelegate.class)) {
					fields[i].setAccessible(true);
					mapFieldValue(
							(fields[i].get(cmd)),
							fields[i].getType(),
							entry);
				}
			}
		}
		catch (final IllegalAccessException e) {
			LOGGER.error(
					"Exception encountered setting fields on command",
					e);
		}

	}
}
