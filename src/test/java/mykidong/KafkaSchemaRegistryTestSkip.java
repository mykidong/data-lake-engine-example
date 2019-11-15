package mykidong;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import org.junit.Test;

import java.util.List;

public class KafkaSchemaRegistryTestSkip {

    public static class User
    {
        private String userName;
        private int age;
        private List<String> addresses;

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public List<String> getAddresses() {
            return addresses;
        }

        public void setAddresses(List<String> addresses) {
            this.addresses = addresses;
        }
    }

    @Test
    public void registerAvroSchemaFromPojo() throws Exception
    {
        // ====================== pojo to avro schema =======================

        // convert pojo to avro schema.
        ObjectMapper mapper = new ObjectMapper(new AvroFactory());
        AvroSchemaGenerator gen = new AvroSchemaGenerator();
        mapper.acceptJsonFormatVisitor(User.class, gen);
        AvroSchema schemaWrapper = gen.getGeneratedSchema();

        // avro schema.
        org.apache.avro.Schema avroSchema = schemaWrapper.getAvroSchema();
        String asJson = avroSchema.toString(true);
        System.out.printf("avsc: %s\n", asJson);


        // ====================== register avro schema =======================

        // schema registry url.
        String url = System.getProperty("url", "http://localhost:8081");

        // topic.
        String topic = System.getProperty("topic");

        // subject.
        String subject = topic + "-value";

        // register avro shema to schema registry.
        CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(url, 20);
        client.register(subject, avroSchema);
        SchemaMetadata schemaMetadata = client.getLatestSchemaMetadata(subject);
    }
}
