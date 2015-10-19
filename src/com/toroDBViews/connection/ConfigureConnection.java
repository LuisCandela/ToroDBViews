package com.toroDBViews.connection;

import com.beust.jcommander.Parameter;

public class ConfigureConnection {
	
	@Parameter(names={"--help","--usage"}, description="Print this usage guide")
	private boolean help = false;
	
	@Parameter(names={"--ask-for-password"}, description="Force input of PostgreSQL's database user password.")
	private boolean askForPassword = false;
	private String dbpass = null;
	
	@Parameter(names={"-h","--host"}, description="PostgreSQL's server host (hostname or IP address)")
	private String dbhost = "localhost";
	@Parameter(names={"-p","--dbport"}, description="PostgreSQL's server port")
	private Integer dbport = 5432;
	@Parameter(names={"-d","--dbname"}, description="PostgreSQL's database name to connect to (must exist)")
	private String dbname = "torodb";
	@Parameter(names={"-u","--username"}, description="PostgreSQL's database user name. Must be a superuser")
	private String dbuser = "toro";
	@Parameter(names={"-f","--filepath"}, description="Path where to save the generated SQL files")
	private String filePath = "/torodb/";
	
	@Parameter(names={"-e", "--execute"}, description="Execute the program creating the views on the database", arity = 1)
	private boolean execute = true;
	
	@Parameter(names={"-c","--createfiles"}, description="Execute the program creating the SQL files of each view", arity = 1 )
	private boolean createFiles = true;
	
	private static final int DB_SUPPORT_MAJOR = 9;
	private static final int DB_SUPPORT_MINOR = 4;
	
	public boolean askForPassword() {
    	return askForPassword;
    }

	public boolean hasPassword() {
    	return dbpass != null;
    }
	
	public void setPassword(String password) {
    	this.dbpass = password;
    }
	
    public String getPassword() {
        return dbpass;
    }
    
    public boolean help() {
    	return help;
    }

    public String getUsername() {
        return dbuser;
    }

    public String getDbHost() {
        return dbhost;
    }

    public String getDbName() {
        return dbname;
    }

    public int getDbPort() {
        return dbport;
    }
    
    public boolean isExecute() {
		return execute;
	}
    
    public boolean isCreateFiles() {
		return createFiles;
	}

	public int getDbSupportMajor() {
		return DB_SUPPORT_MAJOR;
	}
	public int getDbSupportMinor() {
		return DB_SUPPORT_MINOR;
	}

	public String getUrl() {
		
		return "jdbc:postgresql://" + getDbHost() + ":" + getDbPort() + "/";
	}

	public String getFilePath() {
		return filePath;
	}
	
	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
}
