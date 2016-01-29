package mil.nga.giat.geowave.analytic.store;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import mil.nga.giat.geowave.core.cli.GenericStoreCommandLineOptions;
import mil.nga.giat.geowave.core.index.Persistable;
import mil.nga.giat.geowave.core.index.StringUtils;
import mil.nga.giat.geowave.core.store.GenericStoreFactory;
import mil.nga.giat.geowave.core.store.config.AbstractConfigOption;
import mil.nga.giat.geowave.core.store.config.ConfigUtils;
import mil.nga.giat.geowave.core.store.filter.GenericTypeResolver;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class PersistableStore<T> implements
		Persistable
{
	final static Logger LOGGER = LoggerFactory.getLogger(PersistableStore.class);
	protected GenericStoreCommandLineOptions<T> cliOptions;

	protected PersistableStore() {}

	public PersistableStore(
			final GenericStoreCommandLineOptions<T> cliOptions ) {
		this.cliOptions = cliOptions;
	}

	public GenericStoreCommandLineOptions<T> getCliOptions() {
		return cliOptions;
	}

	@Override
	public byte[] toBinary() {
		final GenericStoreFactory<T> factory = cliOptions.getFactory();
		final Map<String, String> strOptions = ConfigUtils.valuesToStrings(
				cliOptions.getConfigOptions(),
				factory.getOptions());
		final List<byte[]> strOptionsBinary = new ArrayList<byte[]>(
				strOptions.size());
		int optionsLength = 4;// for the size of the config options
		for (final Entry<String, String> e : strOptions.entrySet()) {
			final byte[] keyBinary = StringUtils.stringToBinary(e.getKey());
			final byte[] valueBinary = StringUtils.stringToBinary(e.getValue());
			final int entryLength = keyBinary.length + valueBinary.length + 8;
			final ByteBuffer buf = ByteBuffer.allocate(entryLength);
			buf.putInt(keyBinary.length);
			buf.put(keyBinary);
			buf.putInt(valueBinary.length);
			buf.put(valueBinary);
			strOptionsBinary.add(buf.array());
			optionsLength += entryLength;
		}
		final byte[] namespace = StringUtils.stringToBinary(cliOptions.getNamespace());
		final byte[] factoryName = StringUtils.stringToBinary(factory.getName());
		optionsLength += (8 + namespace.length + factoryName.length);
		final ByteBuffer buf = ByteBuffer.allocate(optionsLength);
		buf.putInt(namespace.length);
		buf.put(namespace);
		buf.putInt(factoryName.length);
		buf.put(factoryName);
		buf.putInt(strOptionsBinary.size());
		for (final byte[] strOption : strOptionsBinary) {
			buf.put(strOption);
		}

		return buf.array();
	}

	@Override
	public void fromBinary(
			final byte[] bytes ) {
		final ByteBuffer buf = ByteBuffer.wrap(bytes);
		final byte[] namespaceBinary = new byte[buf.getInt()];
		buf.get(namespaceBinary);
		final byte[] factoryNameBinary = new byte[buf.getInt()];
		buf.get(factoryNameBinary);
		final int configOptionLength = buf.getInt();
		final Map<String, String> configOptions = new HashMap<String, String>(
				configOptionLength);
		for (int i = 0; i < configOptionLength; i++) {
			final int keyLength = buf.getInt();
			final byte[] keyBinary = new byte[keyLength];
			buf.get(keyBinary);
			final int valueLength = buf.getInt();
			final byte[] valueBinary = new byte[valueLength];
			buf.get(valueBinary);
			configOptions.put(
					StringUtils.stringFromBinary(keyBinary),
					StringUtils.stringFromBinary(valueBinary));
		}
		final String factoryName = StringUtils.stringFromBinary(factoryNameBinary);
		final String namespace = StringUtils.stringFromBinary(namespaceBinary);
		cliOptions = getCLIOptions(
				configOptions,
				namespace,
				factoryName);
	}

	abstract protected GenericStoreCommandLineOptions<T> getCLIOptions(
			Map<String, String> configOptions,
			String namespace,
			String factoryName );
}
