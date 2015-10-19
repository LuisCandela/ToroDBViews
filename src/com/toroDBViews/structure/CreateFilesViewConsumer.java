package com.toroDBViews.structure;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.jooq.CreateViewFinalStep;

import com.google.common.base.Charsets;
import com.toroDBViews.connection.ConfigureConnection;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;

public class CreateFilesViewConsumer implements ViewConsumer {

	String relativePath;
	String filename;

	public CreateFilesViewConsumer(ConfigureConnection config)
			throws ClassNotFoundException, ImplementationDbException {

		relativePath = config.getFilePath();
		try {
			createDir(relativePath);
		} catch (IOException e) {

			e.printStackTrace();
		}
	}

	private void createDir(String relativePath) throws IOException {
		
		final File dir = new File(relativePath);
		if (dir.getParentFile() != null) {
			
			if (!dir.exists() && !dir.mkdirs()) {
				throw new IOException("Unable to create " + dir.getAbsolutePath());
			}
		}
	}

	@Override
	public void dropView(String schemaName, String viewName) {
		filename = schemaName + "." + viewName + ".sql";
		Path path = Paths.get(relativePath + filename);
		try {
			Files.deleteIfExists(path);
		} catch (NoSuchFileException x) {
			System.err.format("%s: no such" + " file or directory%n", path);
		} catch (DirectoryNotEmptyException x) {
			System.err.format("%s not empty%n", path);
		} catch (IOException x) {
			// File permission problems are caught here.
			System.err.println(x);
		}
	}

	@Override
	public void createView(String schemaName, String viewName, CreateViewFinalStep view) {

		filename = schemaName + "." + viewName + ".sql";

		try {
			PrintWriter writer = new PrintWriter(relativePath + filename, Charsets.UTF_8.name());

			writer.print(view);
			writer.flush();
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
