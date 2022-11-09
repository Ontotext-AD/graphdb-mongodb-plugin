package com.ontotext.trree.plugin.mongodb;

import com.ontotext.test.utils.StandardUtils;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.impl.bootstrap.HttpServer;
import org.apache.http.impl.bootstrap.ServerBootstrap;
import org.apache.http.protocol.HttpRequestHandler;
import org.eclipse.rdf4j.repository.config.RepositoryConfig;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test the plugin support with external JSON-LD contexts.
 *
 * @author <a href="mailto:borislav.bonev@ontotext.com">Borislav Bonev</a>
 * @since 19/10/2022
 */
public class TestPluginMongoExternalContext extends AbstractMongoBasicTest {
  private static HttpServer server;
  private static AtomicInteger hitCount = new AtomicInteger();

  @Override
  protected void loadData() {
    loadFilesToMongo();
  }

  @Override protected boolean isLearnMode() {
    return false;
  }

  @BeforeClass
  public static void setupHttpEndpoint() throws IOException {
    server = createTestServer(stubResponses(), 54334);
    server.start();
  }

  @AfterClass
  public static void shutdownServer() {
    server.stop();
  }

  @After
  public void resetHitCount() {
    hitCount.set(0);
  }

  private static Map<String, HttpRequestHandler> stubResponses() {
    Map<String, HttpRequestHandler> handlers = new HashMap<>(4);
    handlers.put("/context/context.json", buildContextResponse());
    return handlers;
  }

  private static HttpRequestHandler buildContextResponse() {
    return (httpRequest, httpResponse, httpContext) -> {
      hitCount.incrementAndGet();
      httpResponse.addHeader(HttpHeaders.CONTENT_TYPE, "application/ld-json");
      httpResponse.setStatusCode(200);
      BasicHttpEntity entity = new BasicHttpEntity();
      File contextFile = INPUT_DIR.resolve(
              TestPluginMongoExternalContext.class.getSimpleName() + "/context/context.json")
              .toFile();
      byte[] contextBytes = FileUtils.readFileToByteArray(contextFile);
      entity.setContent(new ByteArrayInputStream(contextBytes));
      httpResponse.setEntity(entity);
    };
  }

  public static HttpServer createTestServer(Map<String, HttpRequestHandler> handlers, int port) {
    Objects.requireNonNull(handlers, "The input map is required!");
    InetSocketAddress address = new InetSocketAddress("localhost", port);
    ServerBootstrap bootstrap = ServerBootstrap.bootstrap()
            .setLocalAddress(address.getAddress())
            .setListenerPort(port);
    handlers.forEach(bootstrap::registerHandler);
    return bootstrap.create();
  }

  /**
   * We should test that the context cache works and is actually used
   */
  @Test
  public void testExternalContextWithCaching() throws Exception {

    query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n" +
            "PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\n" +
            "PREFIX bbc: <http://www.bbc.co.uk/ontologies/bbc/>\n" +
            "select distinct ?s ?p ?o {\n"
            + "\t?search a inst:spb100 ;\n"
            + "\t:find \"{}\" ;"
            + "\t:entity ?entity .\n"
            + "\tgraph inst:spb100 {\n"
            + "\t\t?s bbc:primaryContentOf ?o .\n"
            + "\t}\n"
            + "}";

    verifyUnorderedResult();
    Assert.assertEquals(1, hitCount.get());
  }

  /**
   * We should test that the context cache works and is actually used
   */
  @Test
  public void testResolveBaseFromExternalContext() throws Exception {

    query = "PREFIX : <http://www.ontotext.com/connectors/mongodb#>\n" +
            "PREFIX inst: <http://www.ontotext.com/connectors/mongodb/instance#>\n" +
            "PREFIX cwork: <http://www.bbc.co.uk/ontologies/creativework/>\n" +
            "select distinct ?s ?category {\n"
            + "\t?search a inst:spb100 ;\n"
            + "\t:find \"{'@id' : 'things/1#id'}\" ;\n"
            + "\t:entity ?entity .\n"
            + "\tgraph inst:spb100 {\n"
            + "\t\t?s a ?o .\n"
            + "\t\t?s cwork:category ?category\n"
            + "\t}\n"
            + "}";

    verifyUnorderedResult();
  }

  @Override
  protected RepositoryConfig createRepositoryConfiguration() {
    return StandardUtils.createOwlimSe("empty");
  }

}
