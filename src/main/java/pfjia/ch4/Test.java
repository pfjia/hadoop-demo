package pfjia.ch4;

import avro.StringPair;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.util.Utf8;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author pfjia
 * @since 2018/4/3 15:56
 */
public class Test {
    public static void main(String[] args) throws IOException {
        specific();
    }


    public static void generic() throws IOException {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(Thread.currentThread().getContextClassLoader().getResourceAsStream("Stringpair.avsc"));


        GenericRecord datum = new GenericData.Record(schema);
        datum.put("left", new Utf8("L"));
        datum.put("right", new Utf8("R"));

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();

        DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        GenericRecord result = reader.read(null, decoder);
        System.out.println(result.get("left"));
        System.out.println(result.get("right"));
    }

    public static void specific() throws IOException {
        StringPair datum = new StringPair();
        datum.setLeft("L");
        datum.setRight("R");

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<StringPair> writer = new SpecificDatumWriter<>(StringPair.class);
        Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(datum, encoder);
        encoder.flush();
        out.close();

        DatumReader<StringPair> reader = new SpecificDatumReader<>(StringPair.class);
        Decoder decoder = DecoderFactory.get().binaryDecoder(out.toByteArray(), null);
        StringPair result = reader.read(null, decoder);
        System.out.println(result.getLeft());
        System.out.println(result.getRight());
    }
}
