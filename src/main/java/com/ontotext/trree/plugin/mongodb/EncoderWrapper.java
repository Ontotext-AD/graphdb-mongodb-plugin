package com.ontotext.trree.plugin.mongodb;

import com.ontotext.trree.sdk.PluginException;
import org.bson.*;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

/**
 * The whole idea behind this class is to decode encoded "." and "$" symbols in the JSON-LD keys in the mongo
 * documents.
 */
public class EncoderWrapper implements Encoder<Document> {

	private Encoder<Document> encoder;

	public EncoderWrapper(Encoder<Document> encoder) {
		this.encoder = encoder;
	}

	@Override
	public void encode(BsonWriter writer, Document value, EncoderContext encoderContext) {
		encoder.encode(new BsonWriterWrapper(writer), value, encoderContext);
	}

	@Override
	public Class<Document> getEncoderClass() {
		return encoder.getEncoderClass();
	}

	private class BsonWriterWrapper implements BsonWriter {
		private BsonWriter writer;

		public BsonWriterWrapper(BsonWriter writer) {
			this.writer = writer;
		}

		@Override
		public void flush() {
			writer.flush();
		}

		@Override
		public void writeBinaryData(BsonBinary binary) {
			writer.writeBinaryData(binary);
		}

		@Override
		public void writeBinaryData(String name, BsonBinary binary) {
			writer.writeBinaryData(name, binary);
		}

		@Override
		public void writeBoolean(boolean value) {
			writer.writeBoolean(value);
		}

		@Override
		public void writeBoolean(String name, boolean value) {
			writer.writeBoolean(name, value);
		}

		@Override
		public void writeDateTime(long value) {
			writer.writeDateTime(value);
		}

		@Override
		public void writeDateTime(String name, long value) {
			writer.writeDateTime(name, value);
		}

		@Override
		public void writeDBPointer(BsonDbPointer value) {
			writer.writeDBPointer(value);
		}

		@Override
		public void writeDBPointer(String name, BsonDbPointer value) {
			writer.writeDBPointer(name, value);
		}

		@Override
		public void writeDouble(double value) {
			writer.writeDouble(value);
		}

		@Override
		public void writeDouble(String name, double value) {
			writer.writeDouble(name, value);
		}

		@Override
		public void writeEndArray() {
			writer.writeEndArray();
		}

		@Override
		public void writeEndDocument() {
			writer.writeEndDocument();
		}

		@Override
		public void writeInt32(int value) {
			writer.writeInt32(value);
		}

		@Override
		public void writeInt32(String name, int value) {
			writer.writeInt32(name, value);
		}

		@Override
		public void writeInt64(long value) {
			writer.writeInt64(value);
		}

		@Override
		public void writeInt64(String name, long value) {
			writer.writeInt64(name, value);
		}

		@Override
		public void writeDecimal128(Decimal128 value) {
			writer.writeDecimal128(value);
		}

		@Override
		public void writeDecimal128(String name, Decimal128 value) {
			writer.writeDecimal128(name, value);
		}

		@Override
		public void writeJavaScript(String code) {
			writer.writeJavaScript(code);
		}

		@Override
		public void writeJavaScript(String name, String code) {
			writer.writeJavaScript(name, code);
		}

		@Override
		public void writeJavaScriptWithScope(String code) {
			writer.writeJavaScriptWithScope(code);
		}

		@Override
		public void writeJavaScriptWithScope(String name, String code) {
			writer.writeJavaScriptWithScope(name, code);
		}

		@Override
		public void writeMaxKey() {
			writer.writeMaxKey();
		}

		@Override
		public void writeMaxKey(String name) {
			writer.writeMaxKey(name);
		}

		@Override
		public void writeMinKey() {
			writer.writeMinKey();
		}

		@Override
		public void writeMinKey(String name) {
			writer.writeMinKey(name);
		}

		@Override
		public void writeName(String name) {
			try {
				writer.writeName(URLDecoder.decode(name, "UTF-8"));
			} catch (UnsupportedEncodingException e) {
				throw new PluginException("Could not decode JSON key: " + name, e);
			}
		}

		@Override
		public void writeNull() {
			writer.writeNull();
		}

		@Override
		public void writeNull(String name) {
			writer.writeNull(name);
		}

		@Override
		public void writeObjectId(ObjectId objectId) {
			writer.writeObjectId(objectId);
		}

		@Override
		public void writeObjectId(String name, ObjectId objectId) {
			writer.writeObjectId(name, objectId);
		}

		@Override
		public void writeRegularExpression(BsonRegularExpression regularExpression) {
			writer.writeRegularExpression(regularExpression);
		}

		@Override
		public void writeRegularExpression(String name, BsonRegularExpression regularExpression) {
			writer.writeRegularExpression(name, regularExpression);
		}

		@Override
		public void writeStartArray() {
			writer.writeStartArray();
		}

		@Override
		public void writeStartArray(String name) {
			writer.writeStartArray(name);
		}

		@Override
		public void writeStartDocument() {
			writer.writeStartDocument();
		}

		@Override
		public void writeStartDocument(String name) {
			writer.writeStartDocument(name);
		}

		@Override
		public void writeString(String value) {
			writer.writeString(value);
		}

		@Override
		public void writeString(String name, String value) {
			writer.writeString(name, value);
		}

		@Override
		public void writeSymbol(String value) {
			writer.writeSymbol(value);
		}

		@Override
		public void writeSymbol(String name, String value) {
			writer.writeSymbol(name, value);
		}

		@Override
		public void writeTimestamp(BsonTimestamp value) {
			writer.writeTimestamp(value);
		}

		@Override
		public void writeTimestamp(String name, BsonTimestamp value) {
			writer.writeTimestamp(name, value);
		}

		@Override
		public void writeUndefined() {
			writer.writeUndefined();
		}

		@Override
		public void writeUndefined(String name) {
			writer.writeUndefined(name);
		}

		@Override
		public void pipe(BsonReader reader) {
			writer.pipe(reader);
		}
	}
}