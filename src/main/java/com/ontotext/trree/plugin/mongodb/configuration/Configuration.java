package com.ontotext.trree.plugin.mongodb.configuration;

import static org.apache.commons.lang3.StringUtils.isAnyBlank;
import static org.apache.commons.lang3.StringUtils.isNoneBlank;

import com.mongodb.MongoCredential;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InvalidObjectException;
import java.util.Optional;
import java.util.Properties;

public class Configuration {

  public static final String CONNECTION_STRING_PROPERTY = "connectionString";
  public static final String DATABASE_PROPERTY = "database";
  public static final String COLLECTION_PROPERTY = "collection";
  public static final String USER_PROPERTY = "user";
  public static final String PASSWORD_PROPERTY = "password";
  public static final String AUTH_DB_PROPERTY = "authDb";

  private String connectionString;
  private String database;
  private String collection;
  private String user;
  private String password;
  private String authDb;

  private File propertiesFile;
  private boolean dirty = false;

  public Configuration(File propertiesFile) {
    this.propertiesFile = propertiesFile;
  }

  public static Configuration fromFile(File propertiesFile) throws IOException {
    Configuration conf = new Configuration(propertiesFile);

    Properties props = new Properties();
    try (FileReader reader = new FileReader(propertiesFile)) {
      props.load(reader);
    }

    conf.connectionString = props.getProperty(CONNECTION_STRING_PROPERTY);
    conf.database = props.getProperty(DATABASE_PROPERTY);
    conf.collection = props.getProperty(COLLECTION_PROPERTY);

    if (props.getProperty(USER_PROPERTY) != null) {
      conf.user = props.getProperty(USER_PROPERTY);
      conf.authDb = props.getProperty(AUTH_DB_PROPERTY);
      conf.password = ConfigurationUtils.decrypt(props.getProperty(PASSWORD_PROPERTY), conf.user);
    }

    return conf;
  }

  /**
   * Creates credentials object when the configurations are provided.
   *
   * @return credentials if they are available, otherwise empty optional
   */
  public Optional<MongoCredential> getMongoCredential() {
    if (user != null) {
      return Optional.of(MongoCredential.createCredential(user, authDb, password.toCharArray()));
    }
    return Optional.empty();
  }

  /**
   * Deletes the configurations file.
   */
  public void delete() {
    propertiesFile.delete();
  }

  /**
   * Persists the current configurations.
   *
   * @throws IOException
   */
  public void persist() throws IOException {
    validate();

    Properties props = new Properties();

    props.setProperty(CONNECTION_STRING_PROPERTY, connectionString);
    props.setProperty(DATABASE_PROPERTY, database);
    props.setProperty(COLLECTION_PROPERTY, collection);

    if (user != null) {
      props.setProperty(USER_PROPERTY, user);
      props.setProperty(AUTH_DB_PROPERTY, authDb);
      props.setProperty(PASSWORD_PROPERTY, ConfigurationUtils.encrypt(password, user));
    }

    try (FileWriter writer = new FileWriter(propertiesFile)) {
      props.store(writer, "Mongo properties");
    }
  }

  private void validate() throws InvalidObjectException {
    if (isAnyBlank(connectionString, database, collection)) {
      throw new InvalidObjectException(
          "Invalid mongo configuration. Service, database and collection are mandatory fields");
    }

    // if any of the values is provided, then none of them should be blank
    if (isNoneBlank(user, password, authDb) && isAnyBlank(user, password, authDb)) {
      throw new InvalidObjectException(
          "Invalid mongo configuration. If setting credentials user, password and source are mandatory");
    }
  }

  public String getConnectionString() {
    return connectionString;
  }

  public void setConnectionString(String connectionString) {
    dirty = true;
    this.connectionString = connectionString;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    dirty = true;
    this.database = database;
  }

  public String getCollection() {
    return collection;
  }

  public void setCollection(String collection) {
    dirty = true;
    this.collection = collection;
  }

  public void setUser(String user) {
    dirty = true;
    this.user = user;
  }

  public void setPassword(String password) {
    dirty = true;
    this.password = password;
  }

  public void setAuthDb(String authDb) {
    dirty = true;
    this.authDb = authDb;
  }

  public String getPropertiesFilePath() {
    return propertiesFile.getAbsolutePath();
  }

  public boolean isDirty() {
    return dirty;
  }
}
