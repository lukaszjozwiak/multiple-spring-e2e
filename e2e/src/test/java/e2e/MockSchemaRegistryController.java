package e2e;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
@Slf4j
class MockSchemaRegistryController {

  private final MockSchemaRegistryClient client;

  @PostMapping(
      path = "/subjects/{subject}/versions",
      consumes = "application/vnd.schemaregistry.v1+json")
  public Map<String, Integer> register(
      @PathVariable("subject") String subject,
      @RequestBody RegisterRequest req,
      @RequestParam(name = "normalize", defaultValue = "false") boolean normalize)
      throws IOException, RestClientException {

    var schema = new AvroSchema(new org.apache.avro.Schema.Parser().parse(req.schema()));
    int id = client.register(subject, schema);
    return Map.of("id", id);
  }

  @GetMapping("/schemas/ids/{id}")
  public Map<String, String> schemaById(@PathVariable("id") int id)
      throws IOException, RestClientException {

    ParsedSchema ps = client.getSchemaById(id);
    return Map.of("schema", ps.canonicalString());
  }

  @GetMapping("/subjects/{subject}/versions/latest")
  public Map<String, Object> latest(@PathVariable("subject") String subject)
      throws IOException, RestClientException {

    var meta = client.getLatestSchemaMetadata(subject);
    return Map.of(
        "subject",
        subject,
        "version",
        meta.getVersion(),
        "id",
        meta.getId(),
        "schema",
        meta.getSchema());
  }

  @GetMapping("/subjects")
  public Collection<String> subjects() throws RestClientException, IOException {
    return client.getAllSubjects();
  }

  record RegisterRequest(String schema, String schemaType, List<Map<String, Object>> references) {}
}
