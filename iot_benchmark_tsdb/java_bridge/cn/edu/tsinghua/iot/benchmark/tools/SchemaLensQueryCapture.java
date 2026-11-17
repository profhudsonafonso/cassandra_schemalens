package cn.edu.tsinghua.iot.benchmark.tools;

import cn.edu.tsinghua.iot.benchmark.client.operation.Operation;
import cn.edu.tsinghua.iot.benchmark.schema.MetaDataSchema;
import cn.edu.tsinghua.iot.benchmark.workload.GenerateQueryWorkLoad;
import cn.edu.tsinghua.iot.benchmark.workload.query.impl.Query;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Captures real parameter objects from GenerateQueryWorkLoad, not hand-written SQL.
 * This is a diagnostic sampler: it does NOT replay the random mixed-workload scheduler.
 * Run using the original IoT Benchmark configuration with table dialect and Q12 enabled.
 */
public final class SchemaLensQueryCapture {
  private SchemaLensQueryCapture() {}

  private static Query make(GenerateQueryWorkLoad generator, Operation op) throws Exception {
    switch (op) {
      case PRECISE_QUERY: return generator.getPreciseQuery();
      case RANGE_QUERY: return generator.getRangeQuery();
      case VALUE_RANGE_QUERY: return generator.getValueRangeQuery();
      case AGG_RANGE_QUERY: return generator.getAggRangeQuery();
      case AGG_VALUE_QUERY: return generator.getAggValueQuery();
      case AGG_RANGE_VALUE_QUERY: return generator.getAggRangeValueQuery();
      case GROUP_BY_QUERY: return generator.getGroupByQuery();
      case LATEST_POINT_QUERY: return generator.getLatestPointQuery();
      case RANGE_QUERY_ORDER_BY_TIME_DESC: return generator.getRangeQuery();
      case VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC: return generator.getValueRangeQuery();
      case GROUP_BY_QUERY_ORDER_BY_TIME_DESC: return generator.getGroupByQuery();
      case SET_OP_QUERY: return generator.getSetOpQuery();
      default: throw new IllegalArgumentException("Uncapturable query operation: " + op);
    }
  }

  /** Fail loudly if a serializer emits missing or reference-only sensor DTOs. */
  private static void validateSensorObjects(JSONObject query, Operation op) {
    if (op != Operation.SET_OP_QUERY) return;
    com.alibaba.fastjson.JSONArray children = query.getJSONArray("childRangeQueries");
    if (children == null || children.size() < 2) {
      throw new IllegalStateException("Q12 must have at least two child queries");
    }
    for (int i = 0; i < children.size(); i++) {
      JSONObject child = children.getJSONObject(i);
      com.alibaba.fastjson.JSONArray devices = child.getJSONArray("deviceSchema");
      if (devices == null || devices.isEmpty()) {
        throw new IllegalStateException("Q12 child " + i + " has no complete device schemas");
      }
      for (int j = 0; j < devices.size(); j++) {
        JSONObject device = devices.getJSONObject(j);
        if (device == null || device.containsKey("$ref")) {
          throw new IllegalStateException("Q12 child " + i + " contains device $ref");
        }
        com.alibaba.fastjson.JSONArray sensors = device.getJSONArray("sensors");
        if (sensors == null || sensors.isEmpty()) {
          throw new IllegalStateException("Q12 child " + i + " device " + j + " has no sensors");
        }
        for (int k = 0; k < sensors.size(); k++) {
          JSONObject sensor = sensors.getJSONObject(k);
          if (sensor == null || sensor.containsKey("$ref")
              || sensor.getString("name") == null || sensor.getString("name").isEmpty()) {
            throw new IllegalStateException("Q12 child " + i + " device " + j
                + " sensor " + k + " is not a complete native Sensor DTO");
          }
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length < 1 || args.length > 2) {
      throw new IllegalArgumentException("Usage: SchemaLensQueryCapture OUTPUT.jsonl [SAMPLES_PER_CLASS]");
    }
    final int repetitions = args.length == 2 ? Integer.parseInt(args[1]) : 1;
    if (repetitions < 1 || repetitions > 10000) throw new IllegalArgumentException("Invalid count");
    // Initializes the generator's native device/sensor schema; no live IoTDB is required.
    MetaDataSchema.getInstance();
    GenerateQueryWorkLoad generator = new GenerateQueryWorkLoad(0);
    List<String> events = new ArrayList<>();
    for (Operation op : Operation.values()) {
      if (op == Operation.INGESTION || op == Operation.VERIFICATION_QUERY || op == Operation.DEVICE_QUERY) continue;
      for (int i = 0; i < repetitions; i++) {
        Query query = make(generator, op);
        JSONObject event = new JSONObject(true);
        event.put("origin", "iot_benchmark_generate_query_workload");
        event.put("operation", op.name());
        event.put("sample_index", i);
        event.put("client_id", 0);
        // Native Q12 child queries intentionally reuse Sensor/List<Sensor> objects.
        // Fastjson's default circular-reference tracking can encode later
        // occurrences as {"$ref":"..."}, which is NOT a Sensor DTO. This
        // disables reference substitution; the native query DTOs are acyclic.
        JSONObject fullQuery = JSON.parseObject(
            JSON.toJSONString(query, SerializerFeature.DisableCircularReferenceDetect));
        validateSensorObjects(fullQuery, op);
        event.put("query", fullQuery);
        events.add(JSON.toJSONString(event));
      }
    }
    Path target = Path.of(args[0]);
    if (target.getParent() != null) Files.createDirectories(target.getParent());
    Files.write(target, events, StandardCharsets.UTF_8);
    System.out.println("Exported " + events.size() + " native query objects to " + target);
  }
}
