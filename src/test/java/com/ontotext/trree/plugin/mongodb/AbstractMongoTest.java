package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.functional.base.SingleRepositoryFunctionalTest;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

/**
 * Class that all Mongo classed should extend in order to get an instance of {@link MongodProcess}
 */
public abstract class AbstractMongoTest extends SingleRepositoryFunctionalTest {

	protected MongodProcess mongoProcess;

	@Before
	public void setup() {
		this.mongoProcess = startMongod();
	}

	@After
	public void cleanup() {
		if (mongoProcess.isProcessRunning()) {
			mongoProcess.stop();
		}
	}

	protected static MongodProcess startMongod() {
		IMongodConfig mongoConfigConfig;
		MongodProcess mongod = null;
		try {
			mongoConfigConfig = new MongodConfigBuilder()
					.version(Version.V3_5_5)
					.net(new Net(Network.getFreeServerPort(), Network.localhostIsIPv6()))
					.build();

			MongodExecutable mongodExecutable = MongodStarter.getDefaultInstance().prepare(mongoConfigConfig);
			mongod = mongodExecutable.start();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return mongod;
	}

}
