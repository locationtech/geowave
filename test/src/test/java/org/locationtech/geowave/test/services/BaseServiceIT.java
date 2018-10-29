package org.locationtech.geowave.test.services;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public abstract class BaseServiceIT
{

	private final Map<String, Level> loggerMap = new HashMap<>();

	/*
	 * Utility method for dynamically altering the logger level for loggers
	 * 
	 * Note: Slf4j does not expose the setLevel API, so this is using Log4j
	 * directly.
	 */
	void muteLogging() {

		loggerMap.clear();

		@SuppressWarnings("unchecked")
		final List<Logger> currentLoggers = Collections.<Logger> list(LogManager.getCurrentLoggers());
		currentLoggers.add(LogManager.getRootLogger());

		currentLoggers.forEach(logger -> {
			loggerMap.put(logger.getName(), logger.getEffectiveLevel());
			logger.setLevel(Level.OFF);
		});
	}

	void unmuteLogging() {

		loggerMap.entrySet().forEach(entry -> {
			LogManager.getLogger(entry.getKey()).setLevel(entry.getValue());
		});
	}
}
