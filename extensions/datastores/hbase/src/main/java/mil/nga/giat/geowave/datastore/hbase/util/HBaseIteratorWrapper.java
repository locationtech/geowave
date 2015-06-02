package mil.nga.giat.geowave.datastore.hbase.util;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * @author viggy Functionality similar to <code> IteratorWrapper </code>
 */
public class HBaseIteratorWrapper<InputType, ConvertedType> implements
		Iterator<ConvertedType>
{
	public static interface Converter<InputType, ConvertedType>
	{
		public Iterator<ConvertedType> convert(
				InputType entry );
	}

	public static interface Callback<InputType, ConvertedType>
	{
		public void notifyIterationComplete(
				InputType entry );
	}

	final private Iterator<InputType> inputIterator;
	final private Converter<InputType, ConvertedType> converter;
	private Iterator<ConvertedType> conversionQueue = new LinkedList<ConvertedType>().iterator();
	private final Callback<InputType, ConvertedType> conversionCallback;
	private InputType lastInput;

	public HBaseIteratorWrapper(
			final Iterator<InputType> inputIterator,
			final Converter<InputType, ConvertedType> converter ) {
		this(
				inputIterator,
				converter,
				null);
	}

	public HBaseIteratorWrapper(
			final Iterator<InputType> inputIterator,
			final Converter<InputType, ConvertedType> converter,
			final Callback<InputType, ConvertedType> conversionCallback ) {
		this.inputIterator = inputIterator;
		this.converter = converter;
		this.conversionCallback = conversionCallback;
	}

	@Override
	public synchronized boolean hasNext() {
		if (conversionQueue.hasNext()) {
			return true;
		}
		return inputIterator.hasNext();
	}

	@Override
	public synchronized ConvertedType next() {
		while (!conversionQueue.hasNext() && inputIterator.hasNext()) {
			// fill conversion queue with converted objects from the next input
			final InputType input = inputIterator.next();
			final Iterator<ConvertedType> conversions = converter.convert(input);

			lastInput = input;
			conversionQueue = conversions;
		}
		final ConvertedType retVal = conversionQueue.next();
		if (!conversionQueue.hasNext() && (conversionCallback != null)) {
			// if the queue is empty, then notify that the last input had been
			// converted and iterated on
			notifyIterationComplete();
		}
		return retVal;
	}

	private synchronized void notifyIterationComplete() {
		if (lastInput != null) {
			if (conversionCallback != null) {
				conversionCallback.notifyIterationComplete(lastInput);
			}
			lastInput = null;
		}
	}

	@Override
	public synchronized void remove() {
		conversionQueue.remove();
		inputIterator.remove();
	}
}
