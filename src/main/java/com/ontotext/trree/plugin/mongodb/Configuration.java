package com.ontotext.trree.plugin.mongodb;

import com.mongodb.MongoCredential;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import java.io.*;
import java.security.Key;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;

public class Configuration {

	private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

	protected static final String CONNECTION_STRING_PROPERTY = "connectionString";
	protected static final String DATABASE_PROPERTY = "database";
	protected static final String COLLECTION_PROPERTY = "collection";

	protected static final String USER_PROPERTY = "user";
	protected static final String PASSWORD_PROPERTY = "password";
	protected static final String AUTH_DB_PROPERTY = "authDb";

	protected static final String ENC_ALG = "AES";
	protected static final String KEY_HASH_ALG = "SHA-1";
	protected static final int KEY_LENGTH = 16;

	protected Base64 encoder;
	protected String connectionString;
	protected String database;
	protected String collection;
	protected String user;
	protected String password;
	protected String authDb;

	protected File propertiesFile;

	protected boolean dirty = false;

	public Configuration(File propertiesFile) {
		this.propertiesFile = propertiesFile;
		this.encoder = new Base64();
	}

	static Configuration fromFile(File propertiesFile) throws IOException {
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
			conf.password = conf.decrypt(props.getProperty(PASSWORD_PROPERTY), conf.user);
		}

		return conf;
	}

	void delete() {
		propertiesFile.delete();
	}

	void persist() throws IOException {
		validate();

		Properties props = new Properties();

		props.setProperty(CONNECTION_STRING_PROPERTY, connectionString);
		props.setProperty(DATABASE_PROPERTY, database);
		props.setProperty(COLLECTION_PROPERTY, collection);

		if (user != null) {
			props.setProperty(USER_PROPERTY, user);
			props.setProperty(AUTH_DB_PROPERTY, authDb);
			props.setProperty(PASSWORD_PROPERTY, encrypt(password, user));
		}

		try (FileWriter writer = new FileWriter(propertiesFile)) {
			props.store(writer, "Mongo properties");
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

	public Optional<MongoCredential> getMongoCredential() {
		if (user != null && password != null && authDb != null) {
			return Optional.of(MongoCredential.createCredential(user, authDb, password.toCharArray()));
		} else {
			return Optional.empty();
		}
	}

	public boolean isDirty() {
		return dirty;
	}

	public void validate() throws InvalidObjectException {
		if (StringUtils.isEmpty(connectionString) || StringUtils.isEmpty(database) || StringUtils.isEmpty(collection))
			throw new InvalidObjectException("Invalid mongo configuration. Service, database and collection are mandatory fields");

		if (!StringUtils.isEmpty(user) || !StringUtils.isEmpty(password) || !StringUtils.isEmpty(authDb)) {
			if (StringUtils.isEmpty(user) || StringUtils.isEmpty(password) || StringUtils.isEmpty(authDb))
				throw new InvalidObjectException("Invalid mongo configuration. If setting credentials user, password and source are mandatory");
		}

	}

	private String encrypt(String text, String key) {
		if (text == null || key == null) {
			throw new RuntimeException("Text or key is null!");
		}
		try {
			byte[] hash = Arrays.copyOf(MessageDigest.getInstance(KEY_HASH_ALG).digest(key.getBytes()), KEY_LENGTH);
			Key aesKey = new SecretKeySpec(hash, ENC_ALG);
			Cipher cipher = Cipher.getInstance(ENC_ALG);
			cipher.init(Cipher.ENCRYPT_MODE, aesKey);

			return new String(encoder.encode(cipher.doFinal(text.getBytes())));
		} catch (Exception e) {
			LOGGER.error("Could not encrypt mongo password", e);
			throw new RuntimeException("Could not encrypt text", e);
		}
	}

	private String decrypt(String text, String key) {
		if (text == null || key == null) {
			throw new RuntimeException("Text or key is null!");
		}
		try {
			byte[] hash = Arrays.copyOf(MessageDigest.getInstance(KEY_HASH_ALG).digest(key.getBytes()), KEY_LENGTH);
			Key aesKey = new SecretKeySpec(hash, ENC_ALG);
			Cipher cipher = Cipher.getInstance(ENC_ALG);
			cipher.init(Cipher.DECRYPT_MODE, aesKey);
			byte[] decrypted = cipher.doFinal(encoder.decode(text));

			return new String(decrypted);
		} catch (Exception e) {
			LOGGER.error("Could not decrypt mongo password", e);
			throw new RuntimeException("Could not decrypt text", e);
		}
	}
}
