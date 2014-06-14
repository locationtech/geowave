package mil.nga.giat.geowave.ingest;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

public class FileExtensionVisitor implements FileVisitor<Path> {

	
	protected final static Logger log = Logger.getLogger(FileExtensionVisitor.class);
	protected Pattern pattern;
	protected Matcher matcher;

	public FileExtensionVisitor( final String[] extensions ) {
		String extStr = String.format("([^\\s]+(\\.(?i)(%s))$)", StringUtils.join(extensions, "|"));
		pattern = Pattern.compile(extStr);
	}

	@Override
	public FileVisitResult postVisitDirectory( final Path path, final IOException e )
			throws IOException {
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult preVisitDirectory( final Path path, final BasicFileAttributes bfa )
			throws IOException {
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFile( final Path path, final BasicFileAttributes bfa )	throws IOException {
		return FileVisitResult.CONTINUE;
	}

	@Override
	public FileVisitResult visitFileFailed( final Path path, final IOException bfa )
			throws IOException {
		log.error("Cannot visit path: " + path);
		return FileVisitResult.CONTINUE;
	}

}
