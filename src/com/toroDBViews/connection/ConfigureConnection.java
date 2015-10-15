package com.toroDBViews.connection;

public class ConfigureConnection {
	
	private String user = "toro";
	private String password = "toro";
	private String url = "jdbc:postgresql://localhost:5432/";
	private String databaseName = "torodb";
	private final int DB_SUPPORT_MAJOR = 9;
	private final int DB_SUPPORT_MINOR = 4;
	
	public String getUser() {
		return user;
	}
	public void setUser(String user) {
		this.user = user;
	}
	public String getPassword() {
		return password;
	}
	public void setPassword(String password) {
		this.password = password;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getDatabaseName() {
		return databaseName;
	}
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}
	public int getDbSupportMajor() {
		return DB_SUPPORT_MAJOR;
	}
	public int getDbSupportMinor() {
		return DB_SUPPORT_MINOR;
	}

	
}
