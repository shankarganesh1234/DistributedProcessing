package com.flink.bucketer;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.hadoop.fs.Path;

import com.flink.models.ItemModel;

/**
 * Bucketer to enhance DateTimeBucketer functionality , to include more
 * partitioning options
 * 
 * @author shankarganesh
 *
 */
public class CustomBucketer implements Bucketer<ItemModel> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static final String DEFAULT_FORMAT_STRING = "yyyy-MM-dd--HH";

	private final String formatString;

	private transient SimpleDateFormat dateFormatter;

	/**
	 * Creates a new {@code DateTimeBucketer} with format string
	 * {@code "yyyy-MM-dd--HH"}.
	 */
	public CustomBucketer() {
		this(DEFAULT_FORMAT_STRING);
	}

	/**
	 * Creates a new {@code DateTimeBucketer} with the given date/time format
	 * string.
	 *
	 * @param formatString
	 *            The format string that will be given to {@code SimpleDateFormat}
	 *            to determine the bucket path.
	 */
	public CustomBucketer(String formatString) {
		this.formatString = formatString;

		this.dateFormatter = new SimpleDateFormat(formatString);
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		in.defaultReadObject();

		this.dateFormatter = new SimpleDateFormat(formatString);
	}

	@Override
	public Path getBucketPath(Clock clock, Path basePath, ItemModel element) {
		String newDateTimeString = dateFormatter.format(new Date(clock.currentTimeMillis()));
		return new Path(basePath + "/" + newDateTimeString + "/" + element.getCategoryId());
	}

}
