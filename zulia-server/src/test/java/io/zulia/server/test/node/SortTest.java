package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaConstants;
import io.zulia.client.command.Reindex;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;

import static io.zulia.message.ZuliaIndex.SortAs.StringHandling.LOWERCASE_FOLDING;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class SortTest {

	private static ZuliaWorkPool zuliaWorkPool;

	private static final String INDEX_NAME = "titleSort";

	@BeforeAll
	public static void initAll() throws Exception {

		TestHelper.createNodes(3);

		TestHelper.startNodes();

		Thread.sleep(2000);

		zuliaWorkPool = TestHelper.createClient();
	}

	@Test
	@Order(1)
	public void indexingTest() throws Exception {

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.create("id", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title", FieldType.STRING).indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("stars", FieldType.NUMERIC_INT).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("starsLong", FieldType.NUMERIC_LONG).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("rating", FieldType.NUMERIC_FLOAT).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("ratingDouble", FieldType.NUMERIC_FLOAT).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("special", FieldType.BOOL).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("added", FieldType.DATE).index().sort());
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);

		for (int id = 0; id < 200; id++) {
			String title = "some title";
			String otherTitle = "blah";
			Integer stars = 1;
			Long starsLong = 1L;
			Float rating = 5.0f;
			Double ratingDouble = 5.0d;
			Boolean special = false;

			Date added = null;

			if (id == 5) {
				title = null;
				otherTitle = "Blah";
				stars = 4;
				starsLong = 4L;
				rating = 1.2f;
				ratingDouble = 1.1d;
				added = Date.from(LocalDate.of(2014, Month.OCTOBER, 4).atStartOfDay(ZoneId.of("UTC")).toInstant());
			}
			if (id == 6) {
				title = null;
				otherTitle = "Blāh";
				stars = 4;
				starsLong = 4L;
				rating = 1.2f;
				ratingDouble = 1.1d;
				added = Date.from(LocalDate.of(2014, Month.OCTOBER, 6).atStartOfDay(ZoneId.of("UTC")).toInstant());
			}
			if (id == 10) {
				title = null;
				otherTitle = "out of ideas";
				stars = 4;
				starsLong = 4L;
				rating = 1.1f;
				ratingDouble = 1.1d;
				added = Date.from(LocalDate.of(2014, Month.OCTOBER, 4).atStartOfDay(ZoneId.of("UTC")).toInstant());
			}
			if (id == 20) {
				title = "other title";
				otherTitle = "still more blah";
				stars = 3;
				starsLong = 3L;
				rating = null;
				ratingDouble = null;
				special = null;
				added = Date.from(LocalDate.of(2015, Month.APRIL, 1).atStartOfDay(ZoneId.of("UTC")).toInstant());
			}
			if (id == 30) {
				title = "a special title";
				otherTitle = "primary colors";
				stars = null;
				starsLong = null;
				rating = 4.7f;
				ratingDouble = 4.7d;
				special = true;
				added = Date.from(LocalDate.of(2020, Month.JANUARY, 31).atStartOfDay(ZoneId.of("UTC")).toInstant());
			}
			if (id == 40) {
				title = "oh so special secret title";
				otherTitle = "secondary colors";
				starsLong = Integer.MAX_VALUE + 100000L;
				rating = 4.7f;
				ratingDouble = Float.MAX_VALUE + 1000000d;
				special = true;
				added = Date.from(LocalDate.of(1951, Month.DECEMBER, 20).atStartOfDay(ZoneId.of("UTC")).toInstant());
			}

			String uniqueId = "" + id;
			Document mongoDocument = new Document();
			mongoDocument.put("id", uniqueId);
			mongoDocument.put("title", title);
			mongoDocument.put("otherTitle", otherTitle); // used to test adding a sortable field

			mongoDocument.put("stars", stars);
			mongoDocument.put("starsLong", starsLong);
			mongoDocument.put("rating", rating);
			mongoDocument.put("ratingDouble", ratingDouble);
			mongoDocument.put("special", special);
			mongoDocument.put("added", added);

			zuliaWorkPool.store(new Store(uniqueId, INDEX_NAME, ResultDocBuilder.from(mongoDocument)));

		}

	}

	@Test
	@Order(2)
	public void titleSort() throws Exception {
		SearchResult searchResult;

		Search search = new Search(INDEX_NAME).setAmount(10);

		search.addSort(new Sort("title")); //default ascending missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get("title"));

		search.clearSort();
		search.addSort(new Sort("title").ascending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get("title"));

		search.clearSort();
		search.addSort(new Sort("title").ascending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get("title"));

		search.clearSort();
		search.addSort(new Sort("title").ascending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals("a special title", searchResult.getFirstDocument().get("title"));

		search.clearSort();
		search.addSort(new Sort("title").descending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals("some title", searchResult.getFirstDocument().get("title"));

		search.clearSort();
		search.addSort(new Sort("title").descending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals("some title", searchResult.getFirstDocument().get("title"));

		search.clearSort();
		search.addSort(new Sort("title").descending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get("title"));
	}

	@Test
	@Order(3)
	public void starsSort() throws Exception {
		SearchResult searchResult;

		Search search = new Search(INDEX_NAME).setAmount(10);

		String field = "stars";
		search.addSort(new Sort(field)); //default ascending missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1, searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4, searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(4, searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));
	}

	@Test
	@Order(4)
	public void starsLongSort() throws Exception {
		SearchResult searchResult;

		Search search = new Search(INDEX_NAME).setAmount(10);

		String field = "starsLong";
		search.addSort(new Sort(field)); //default ascending missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1L, searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(Integer.MAX_VALUE + 100000L, searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(Integer.MAX_VALUE + 100000L, searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));
	}

	@Test
	@Order(5)
	public void ratingSort() throws Exception {
		SearchResult searchResult;

		Search search = new Search(INDEX_NAME).setAmount(10);

		String field = "rating";
		search.addSort(new Sort(field)); //default ascending missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		//mongodb bson does not support float so it comes back as a double, the search engine is indexing in float precision however
		Assertions.assertEquals(1.1f, (double) searchResult.getFirstDocument().get(field), 0.001f);

		search.clearSort();
		search.addSort(new Sort(field).descending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(5.0f, (double) searchResult.getFirstDocument().get(field), 0.001f);

		search.clearSort();
		search.addSort(new Sort(field).descending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(5.0f, (double) searchResult.getFirstDocument().get(field), 0.001f);

		search.clearSort();
		search.addSort(new Sort(field).descending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));
	}

	@Test
	@Order(6)
	public void ratingDoubleSort() throws Exception {
		SearchResult searchResult;

		Search search = new Search(INDEX_NAME).setAmount(10);

		String field = "ratingDouble";
		search.addSort(new Sort(field)); //default ascending missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(1.1d, (double) searchResult.getFirstDocument().get(field), 0.001d);

		search.clearSort();
		search.addSort(new Sort(field).descending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(Float.MAX_VALUE + 1000000d, (double) searchResult.getFirstDocument().get(field), 0.001d);

		search.clearSort();
		search.addSort(new Sort(field).descending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(Float.MAX_VALUE + 1000000d, (double) searchResult.getFirstDocument().get(field), 0.001d);

		search.clearSort();
		search.addSort(new Sort(field).descending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));
	}

	@Test
	@Order(7)
	public void boolSort() throws Exception {
		SearchResult searchResult;

		Search search = new Search(INDEX_NAME).setAmount(10);

		String field = "special";
		search.addSort(new Sort(field)); //default ascending missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(false, searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(true, searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(true, searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));
	}

	@Test
	@Order(8)
	public void dateSort() throws Exception {
		SearchResult searchResult;

		Search search = new Search(INDEX_NAME).setAmount(10);

		String field = "added";
		search.addSort(new Sort(field)); //default ascending missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).ascending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(Date.from(LocalDate.of(1951, Month.DECEMBER, 20).atStartOfDay(ZoneId.of("UTC")).toInstant()),
				searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending()); //default missing first
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(Date.from(LocalDate.of(2020, Month.JANUARY, 31).atStartOfDay(ZoneId.of("UTC")).toInstant()),
				searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending().missingFirst());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(Date.from(LocalDate.of(2020, Month.JANUARY, 31).atStartOfDay(ZoneId.of("UTC")).toInstant()),
				searchResult.getFirstDocument().get(field));

		search.clearSort();
		search.addSort(new Sort(field).descending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertNull(searchResult.getFirstDocument().get(field));
	}

	@Test
	@Order(9)
	public void scoreSort() throws Exception {
		SearchResult searchResult;

		Search search = new Search(INDEX_NAME).setAmount(10).addQuery(new ScoredQuery("title:special OR title:secret"));

		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals("40", searchResult.getFirstDocument().get("id"));

		search.clearSort();
		search.addSort(new Sort(ZuliaConstants.SCORE_FIELD)); //default ascending
		searchResult = zuliaWorkPool.search(search);

		Assertions.assertEquals("30", searchResult.getFirstDocument().get("id"));

		search.clearSort();
		search.addSort(new Sort(ZuliaConstants.SCORE_FIELD).descending());
		searchResult = zuliaWorkPool.search(search);

		Assertions.assertEquals("40", searchResult.getFirstDocument().get("id"));
	}

	@Test
	@Order(10)
	public void compoundSort() throws Exception {
		Search search = new Search(INDEX_NAME).setAmount(10);

		search.addSort(new Sort("stars").descending());
		search.addSort(new Sort("rating").ascending()); //ascending is default but to be clear

		SearchResult searchResult = zuliaWorkPool.search(search);

		Assertions.assertEquals(4, searchResult.getFirstDocument().get("stars"));
		Assertions.assertEquals(1.1f, (double) searchResult.getFirstDocument().get("rating"), 0.001f);
	}

	@Test
	@Order(11)
	public void restart() throws Exception {
		TestHelper.stopNodes();
		Thread.sleep(2000);
		TestHelper.startNodes();
		Thread.sleep(2000);
	}

	@Test
	@Order(12)
	public void afterRestartRetry() throws Exception {
		boolSort();
		dateSort();
		compoundSort();
		ratingSort();
		ratingDoubleSort();
		starsSort();
		starsLongSort();
		titleSort();
	}

	@Test
	@Order(13)
	public void reindexTest() throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.create("id", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD)
				.sortAs("theId")); //change sort as to be theId instead of just id
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title", FieldType.STRING).indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("stars", FieldType.NUMERIC_INT).index()); // no longer sortable
		indexConfig.addFieldConfig(FieldConfigBuilder.create("starsLong", FieldType.NUMERIC_LONG).index().sort());
		//indexConfig.addFieldConfig(FieldConfigBuilder.create("rating", FieldType.NUMERIC_FLOAT).index().sort()); // no longer indexed or sortable
		indexConfig.addFieldConfig(FieldConfigBuilder.create("ratingDouble", FieldType.NUMERIC_FLOAT).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("special", FieldType.BOOL).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("added", FieldType.DATE).index().sort());
		//sort() adds standard string (case senstive sorting with a field name the same as the stored field
		//sortAs(LOWERCASE_FOLDING, "otherTitleFolding") add another sortable field with a lowercase and ascii folding filter applied to make case insensitive sort and fancy letter insensitive (gotta be a better term here)
		indexConfig.addFieldConfig(FieldConfigBuilder.create("otherTitle", FieldType.STRING).index().sort().sortAs(LOWERCASE_FOLDING, "otherTitleFolding"));
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);

		zuliaWorkPool.reindex(new Reindex(INDEX_NAME));

		SearchResult searchResult;
		Search search = new Search(INDEX_NAME).setAmount(1);

		search.addSort(new Sort("otherTitle").ascending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals("Blah", searchResult.getFirstDocument().get("otherTitle"));

		search.clearSort();
		search.addSort(new Sort("otherTitleFolding").ascending().missingLast());
		search.addSort(new Sort("starts").ascending().missingLast());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals("blah", searchResult.getFirstDocument().get("otherTitle"));

		search.clearSort();
		search.addSort(new Sort("otherTitle").descending());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals("still more blah", searchResult.getFirstDocument().get("otherTitle"));

		search.clearSort();
		search.addSort(new Sort("theId").descending()); // use the new sort as id field
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals("99", searchResult.getFirstDocument().get("id")); // sorting as string so this is 99 instead of 199

	}

	@Test
	@Order(14)
	public void multiIndexTest() throws Exception {

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("magicNumber");
		indexConfig.addFieldConfig(FieldConfigBuilder.create("id", FieldType.NUMERIC_INT).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("magicNumber", FieldType.NUMERIC_INT).index().sort());
		indexConfig.setIndexName("anotherIndex");
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);

		indexConfig.setIndexName("anotherIndex2");
		zuliaWorkPool.createIndex(indexConfig);

		for (int id = 0; id < 200; id++) {

			int magicNumber = 7;
			if (id > 10) {
				magicNumber = -1;
			}

			Document mongoDocument = new Document().append("id", id).append("magicNumber", magicNumber);
			zuliaWorkPool.store(new Store(id + "", "anotherIndex", ResultDocBuilder.from(mongoDocument)));
		}

		for (int id = 0; id < 100; id++) {

			int magicNumber = (id % 10) + 5;

			Document mongoDocument = new Document().append("id", id).append("magicNumber", magicNumber);
			zuliaWorkPool.store(new Store(id + "", "anotherIndex2", ResultDocBuilder.from(mongoDocument)));
		}

		SearchResult searchResult;
		Search search = new Search("anotherIndex", "anotherIndex2").setAmount(1);

		search.addSort(new Sort("magicNumber"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(-1, searchResult.getFirstDocument().get("magicNumber"));

		search.clearSort();
		search.addSort(new Sort("magicNumber").descending());
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(14, searchResult.getFirstDocument().get("magicNumber"));

	}

	@AfterAll
	public static void shutdown() throws Exception {
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
	}
}
