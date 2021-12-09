package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.SummaryAnalysis;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaQuery;
import org.bson.Document;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TermTest {

	public static final String TERM_TEST_INDEX = "term";

	private static ZuliaWorkPool zuliaWorkPool;
	private static final int repeatCount = 100;

	@BeforeAll
	public static void initAll() throws Exception {

		TestHelper.createNodes(3);

		TestHelper.startNodes();

		Thread.sleep(2000);

		zuliaWorkPool = TestHelper.createClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.create("id", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title", FieldType.STRING).indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.setIndexName(TERM_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		for (int i = 0; i < repeatCount; i++) {
			int uniqueDocs = 6;
			indexRecord(i * uniqueDocs, "one two three");
			indexRecord(i * uniqueDocs + 1, "something really special and different");
			indexRecord(i * uniqueDocs + 2, "something special");
			indexRecord(i * uniqueDocs + 3, "amazing and stuff");
			indexRecord(i * uniqueDocs + 4, "blah blah blah");
			indexRecord(i * uniqueDocs + 5, "one amazing thing");
		}

	}

	private void indexRecord(int id, String title) throws Exception {

		String uniqueId = "" + id;

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("title", title);

		Store s = new Store(uniqueId, TERM_TEST_INDEX);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void termTest() throws Exception {

		Search search = new Search(TERM_TEST_INDEX);
		search.addAnalysis(new SummaryAnalysis("title").setTopN(5));
		SearchResult searchResult = zuliaWorkPool.search(search);

		List<ZuliaQuery.AnalysisResult> analysisResults = searchResult.getSummaryAnalysisResults();

		ZuliaQuery.AnalysisResult analysisResult = analysisResults.get(0);

		System.out.println(analysisResult.getTermsCount());

		for (ZuliaBase.Term term : analysisResult.getTermsList()) {
			System.out.println(term);
		}

		//Assertions.assertEquals(0.5, analysisResults.getMin().getDoubleValue(), 0.001);
		//Assertions.assertEquals(3.5, analysisResults.getMax().getDoubleValue(), 0.001);

	}

	@Test
	@Order(7)
	public void shutdown() throws Exception {
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
	}
}
