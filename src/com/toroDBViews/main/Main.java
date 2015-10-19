package com.toroDBViews.main;

import java.io.BufferedReader;
import java.io.Console;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.sql.SQLException;

import com.beust.jcommander.JCommander;

import com.toroDBViews.connection.ConfigureConnection;
import com.toroDBViews.connection.PostgreSQLConnection;
import com.toroDBViews.exceptions.InstanceOfArrayStructureException;
import com.toroDBViews.exceptions.TypeAttributeException;
import com.toroDBViews.structure.CreateFilesViewConsumer;
import com.toroDBViews.structure.CreateViewViewConsumer;
import com.toroDBViews.structure.TableCreator;
import com.toroDBViews.structure.ViewConsumer;
import com.torodb.torod.core.dbWrapper.exceptions.ImplementationDbException;
import com.torodb.torod.db.exceptions.InvalidDatabaseException;

public class Main {

	public static void main(String[] args) throws IOException, ClassNotFoundException,
			SQLException, InstantiationException, IllegalAccessException, InvalidDatabaseException,
			InstanceOfArrayStructureException, TypeAttributeException, ImplementationDbException {

		final ConfigureConnection config = new ConfigureConnection();

		JCommander jCommander = new JCommander(config, args);

		if (config.help()) {
			jCommander.usage();
			System.exit(0);
		}

		File toroPass = new File(System.getProperty("user.home") + "/.toropass");
		if (toroPass.exists() && toroPass.canRead() && toroPass.isFile()) {
			BufferedReader br = new BufferedReader(
					new InputStreamReader(new FileInputStream(toroPass), Charset.forName("UTF-8")));
			String line;
			while ((line = br.readLine()) != null) {
				String[] toroPassChunks = line.split(":");
				if (toroPassChunks.length != 5) {
					continue;
				}

				if ((toroPassChunks[0].equals("*") || toroPassChunks[0].equals(config.getDbHost()))
						&& (toroPassChunks[1].equals("*")
								|| toroPassChunks[1].equals(String.valueOf(config.getDbPort())))
						&& (toroPassChunks[2].equals("*") || toroPassChunks[2].equals(config.getDbName()))
						&& (toroPassChunks[3].equals("*") || toroPassChunks[3].equals(config.getUsername()))) {
					config.setPassword(toroPassChunks[4]);
				}
			}
			br.close();
		}

		if (!config.hasPassword() || config.askForPassword()) {
			config.setPassword(readPwd("PostgreSQL's database user password:"));
		}

		PostgreSQLConnection connection = new PostgreSQLConnection(config);
		connection.initialize();
		TableCreator tableCreator;
		if (config.isExecute()) {
			ViewConsumer viewConsumer;
			viewConsumer = new CreateViewViewConsumer(connection);
			tableCreator = new TableCreator(viewConsumer, connection);
			tableCreator.execute();
		}
		if(config.isCreateFiles()){
			ViewConsumer viewConsumer2;
			viewConsumer2 = new CreateFilesViewConsumer(config);
			tableCreator = new TableCreator(viewConsumer2, connection);
			tableCreator.execute();
		}
		
	}

	private static String readPwd(String text) throws IOException {
		Console c = System.console();
		if (c == null) { // IN ECLIPSE IDE
			System.out.print(text);
			InputStream in = System.in;
			int max = 50;
			byte[] b = new byte[max];

			int l = in.read(b);
			l--;// last character is \n
			if (l > 0) {
				byte[] e = new byte[l];
				System.arraycopy(b, 0, e, 0, l);
				return new String(e, Charset.forName("UTF-8"));
			} else {
				return null;
			}
		} else { // Outside Eclipse IDE
			return new String(c.readPassword(text));
		}
	}

}
