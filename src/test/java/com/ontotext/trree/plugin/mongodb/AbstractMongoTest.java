package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.functional.base.SingleRepositoryFunctionalTest;
import de.flapdoodle.embed.mongo.transitions.Mongod;
import de.flapdoodle.embed.mongo.transitions.RunningMongodProcess;
import de.flapdoodle.reverse.TransitionWalker;
import org.junit.After;
import org.junit.Before;

/**
 * Class that all Mongo classes should extend in order to get an instance of running MongoDB
 */
public abstract class AbstractMongoTest extends SingleRepositoryFunctionalTest {

	protected RunningMongodProcess mongoProcess;
	protected TransitionWalker.ReachedState<RunningMongodProcess> running;

	@Before
	public void setup() {
		this.running = startMongod();
		this.mongoProcess = running.current();
	}

	@After
	public void cleanup() {
		if (running != null) {
			running.close();
		}
	}

	protected static TransitionWalker.ReachedState<RunningMongodProcess> startMongod() {
		try {
			return Mongod.instance().start(de.flapdoodle.embed.mongo.distribution.Version.Main.V4_4);
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Failed to start MongoDB", e);
		}
	}

}
