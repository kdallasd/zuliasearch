package io.zulia.server.filestorage;

import com.google.protobuf.ByteString;
import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.client.model.IndexOptions;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaBase.Metadata;
import io.zulia.message.ZuliaQuery.FetchType;
import org.bson.Document;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Logger;

public class MongoDocumentStorage implements DocumentStorage {
	@SuppressWarnings("unused")
	private final static Logger log = Logger.getLogger(MongoDocumentStorage.class.getSimpleName());

	private static final String ASSOCIATED_FILES = "associatedFiles";
	private static final String FILES = "files";
	private static final String CHUNKS = "chunks";
	private static final String ASSOCIATED_METADATA = "metadata";

	private static final String TIMESTAMP = "_tstamp_";

	private static final String DOCUMENT_UNIQUE_ID_KEY = "_uid_";
	private static final String FILE_UNIQUE_ID_KEY = "_fid_";

	private static final String ENABLESHARDING = "enablesharding";
	private static final String ADMIN = "admin";
	public static final String SHARDCOLLECTION = "shardcollection";

	private MongoClient mongoClient;
	private String database;
	private String indexName;

	public MongoDocumentStorage(MongoClient mongoClient, String indexName, String dbName, boolean sharded) {
		this.mongoClient = mongoClient;
		this.indexName = indexName;
		this.database = dbName;

		MongoDatabase storageDb = mongoClient.getDatabase(database);
		MongoCollection<Document> coll = storageDb.getCollection(ASSOCIATED_FILES + "." + FILES);
		coll.createIndex(new Document(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, 1), new IndexOptions().background(true));
		coll.createIndex(new Document(ASSOCIATED_METADATA + "." + FILE_UNIQUE_ID_KEY, 1), new IndexOptions().background(true));

		if (sharded) {

			MongoDatabase adminDb = mongoClient.getDatabase(ADMIN);
			Document enableCommand = new Document();
			enableCommand.put(ENABLESHARDING, database);
			adminDb.runCommand(enableCommand);

			shardCollection(storageDb, adminDb, ASSOCIATED_FILES + "." + CHUNKS);
		}
	}

	private void shardCollection(MongoDatabase db, MongoDatabase adminDb, String collectionName) {
		Document shardCommand = new Document();
		MongoCollection<Document> collection = db.getCollection(collectionName);
		shardCommand.put(SHARDCOLLECTION, collection.getNamespace().getFullName());
		shardCommand.put("key", new BasicDBObject("_id", 1));
		adminDb.runCommand(shardCommand);
	}

	private GridFSBucket createGridFSConnection() {
		MongoDatabase db = mongoClient.getDatabase(database);
		return GridFSBuckets.create(db, ASSOCIATED_FILES);
	}

	@Override
	public void deleteAllDocuments() {
		GridFSBucket gridFS = createGridFSConnection();
		gridFS.drop();
	}

	@Override
	public void drop() {
		MongoDatabase db = mongoClient.getDatabase(database);
		db.drop();
	}

	@Override
	public void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, long timestamp, Map<String, String> metadataMap) throws Exception {
		GridFSBucket gridFS = createGridFSConnection();

		deleteAssociatedDocument(uniqueId, fileName);

		GridFSUploadOptions gridFSUploadOptions = getGridFSUploadOptions(uniqueId, fileName, timestamp, metadataMap);
		gridFS.uploadFromStream(fileName, is, gridFSUploadOptions);
	}

	private GridFSUploadOptions getGridFSUploadOptions(String uniqueId, String fileName, long timestamp, Map<String, String> metadataMap) {
		Document metadata = new Document();
		if (metadataMap != null) {
			for (String key : metadataMap.keySet()) {
				metadata.put(key, metadataMap.get(key));
			}
		}
		metadata.put(TIMESTAMP, timestamp);
		metadata.put(DOCUMENT_UNIQUE_ID_KEY, uniqueId);
		metadata.put(FILE_UNIQUE_ID_KEY, getGridFsId(uniqueId, fileName));

		return new GridFSUploadOptions().chunkSizeBytes(1024).metadata(metadata);
	}

	@Override
	public void storeAssociatedDocument(AssociatedDocument doc) throws Exception {

		byte[] bytes = doc.getDocument().toByteArray();

		ByteArrayInputStream byteInputStream = new ByteArrayInputStream(bytes);

		Map<String, String> metadata = new HashMap<>();
		for (Metadata meta : doc.getMetadataList()) {
			metadata.put(meta.getKey(), meta.getValue());
		}

		storeAssociatedDocument(doc.getDocumentUniqueId(), doc.getFilename(), byteInputStream, doc.getTimestamp(), metadata);
	}

	@Override
	public List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception {
		GridFSBucket gridFS = createGridFSConnection();
		List<AssociatedDocument> assocDocs = new ArrayList<>();
		if (!FetchType.NONE.equals(fetchType)) {
			GridFSFindIterable files = gridFS.find(new Document(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, uniqueId));
			for (GridFSFile file : files) {
				AssociatedDocument ad = loadGridFSToAssociatedDocument(gridFS, file, fetchType);
				assocDocs.add(ad);
			}

		}
		return assocDocs;
	}

	private String getGridFsId(String uniqueId, String filename) {
		return uniqueId + "-" + filename;
	}

	@Override
	public InputStream getAssociatedDocumentStream(String uniqueId, String fileName) {
		GridFSBucket gridFS = createGridFSConnection();
		GridFSFile file = gridFS.find(new Document(ASSOCIATED_METADATA + "." + FILE_UNIQUE_ID_KEY, getGridFsId(uniqueId, fileName))).first();

		if (file == null) {
			return null;
		}

		InputStream is = gridFS.openDownloadStream(file.getObjectId());
		;

		Document metadata = file.getMetadata();

		return is;
	}

	@Override
	public AssociatedDocument getAssociatedDocument(String uniqueId, String fileName, FetchType fetchType) throws Exception {
		GridFSBucket gridFS = createGridFSConnection();
		if (!FetchType.NONE.equals(fetchType)) {
			GridFSFile file = gridFS.find(new Document(ASSOCIATED_METADATA + "." + FILE_UNIQUE_ID_KEY, getGridFsId(uniqueId, fileName))).first();
			if (null != file) {
				return loadGridFSToAssociatedDocument(gridFS, file, fetchType);
			}
		}
		return null;
	}

	private AssociatedDocument loadGridFSToAssociatedDocument(GridFSBucket gridFS, GridFSFile file, FetchType fetchType) throws IOException {
		AssociatedDocument.Builder aBuilder = AssociatedDocument.newBuilder();
		aBuilder.setFilename(file.getFilename());
		Document metadata = file.getMetadata();

		long timestamp = (long) metadata.remove(TIMESTAMP);

		aBuilder.setTimestamp(timestamp);

		aBuilder.setDocumentUniqueId((String) metadata.remove(DOCUMENT_UNIQUE_ID_KEY));
		for (String field : metadata.keySet()) {
			aBuilder.addMetadata(Metadata.newBuilder().setKey(field).setValue((String) metadata.get(field)));
		}

		if (FetchType.FULL.equals(fetchType)) {

			ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
			gridFS.downloadToStream(file.getObjectId(), byteArrayOutputStream);
			byte[] bytes = byteArrayOutputStream.toByteArray();
			if (null != bytes) {
				aBuilder.setDocument(ByteString.copyFrom(bytes));
			}
		}
		aBuilder.setIndexName(indexName);
		return aBuilder.build();
	}

	public void getAssociatedDocuments(OutputStream outputstream, Document filter) throws IOException {
		Charset charset = Charset.forName("UTF-8");

		GridFSBucket gridFS = createGridFSConnection();
		GridFSFindIterable gridFSFiles = gridFS.find(filter);
		outputstream.write("{\n".getBytes(charset));
		outputstream.write(" \"associatedDocs\": [\n".getBytes(charset));

		boolean first = true;
		for (GridFSFile gridFSFile : gridFSFiles) {
			if (first) {
				first = false;
			}
			else {
				outputstream.write(",\n".getBytes(charset));
			}

			Document metadata = gridFSFile.getMetadata();

			String uniqueId = metadata.getString(DOCUMENT_UNIQUE_ID_KEY);
			String uniquieIdKeyValue = "  { \"uniqueId\": \"" + uniqueId + "\", ";
			outputstream.write(uniquieIdKeyValue.getBytes(charset));

			String filename = gridFSFile.getFilename();
			String filenameKeyValue = "\"filename\": \"" + filename + "\", ";
			outputstream.write(filenameKeyValue.getBytes(charset));

			Date uploadDate = gridFSFile.getUploadDate();
			String uploadDateKeyValue = "\"uploadDate\": {\"$date\":" + uploadDate.getTime() + "}";
			outputstream.write(uploadDateKeyValue.getBytes(charset));

			metadata.remove(TIMESTAMP);
			metadata.remove(DOCUMENT_UNIQUE_ID_KEY);
			metadata.remove(FILE_UNIQUE_ID_KEY);

			if (!metadata.isEmpty()) {
				String metaJson = metadata.toJson();
				String metaString = ", \"meta\": " + metaJson;
				outputstream.write(metaString.getBytes(charset));
			}

			outputstream.write(" }".getBytes(charset));

		}
		outputstream.write("\n ]\n}".getBytes(charset));
	}

	@Override
	public List<String> getAssociatedFilenames(String uniqueId) throws Exception {
		GridFSBucket gridFS = createGridFSConnection();
		ArrayList<String> fileNames = new ArrayList<>();
		gridFS.find(new Document(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, uniqueId))
				.forEach((Consumer<GridFSFile>) gridFSFile -> fileNames.add(gridFSFile.getFilename()));

		return fileNames;
	}

	@Override
	public void deleteAssociatedDocument(String uniqueId, String fileName) {
		GridFSBucket gridFS = createGridFSConnection();
		gridFS.find(new Document(ASSOCIATED_METADATA + "." + FILE_UNIQUE_ID_KEY, getGridFsId(uniqueId, fileName)))
				.forEach((Block<GridFSFile>) gridFSFile -> gridFS.delete(gridFSFile.getObjectId()));

	}

	@Override
	public void deleteAssociatedDocuments(String uniqueId) {
		GridFSBucket gridFS = createGridFSConnection();
		gridFS.find(new Document(ASSOCIATED_METADATA + "." + DOCUMENT_UNIQUE_ID_KEY, uniqueId))
				.forEach((Block<GridFSFile>) gridFSFile -> gridFS.delete(gridFSFile.getObjectId()));
	}

}
