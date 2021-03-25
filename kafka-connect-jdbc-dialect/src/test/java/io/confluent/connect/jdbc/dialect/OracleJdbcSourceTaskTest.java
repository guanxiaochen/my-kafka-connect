/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.connect.jdbc.dialect;

import io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig;
import io.confluent.connect.jdbc.source.JdbcSourceTask;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.header.Header;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.confluent.connect.jdbc.source.JdbcSourceConnectorConfig.NumericMapping;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
public class OracleJdbcSourceTaskTest {

  protected Time time;
  @Mock
  protected SourceTaskContext taskContext;
  protected Map<String, String> config;
  protected JdbcSourceTask task;
  protected HeaderConverter headerConverter;
  protected JsonConverter jsonConverter;
  @Mock
  private OffsetStorageReader reader;



  @Before
  public void setup() throws Exception {
    time = new MockTime();
    task = new JdbcSourceTask(time);
    config = getProps();
    task.start(config);
    jsonConverter = new JsonConverter();
    headerConverter = new SimpleHeaderConverter();
    jsonConverter.configure(config);
  }

  @After
  public void tearDown() throws Exception {
    task.stop();
  }

  @Test
  public void test() throws InterruptedException {
    List<SourceRecord> records = task.poll();
    for (SourceRecord record : records) {
      RecordHeaders headers = convertHeaderFor(record);
      byte[] bytes = jsonConverter.fromConnectData(record.topic(), headers, record.valueSchema(), record.value());
      System.out.println(new String(bytes));
    }
    Thread.sleep(500);
  }

  protected Map<String, String> getProps() {
    Map<String, String> props = new HashMap<>();
    props.put(JdbcSourceConnectorConfig.CONNECTION_URL_CONFIG, "jdbc:oracle:thin:@192.168.245.199:1521:orclgps");
    props.put(JdbcSourceConnectorConfig.CONNECTION_USER_CONFIG, "tmgps");
    props.put(JdbcSourceConnectorConfig.CONNECTION_PASSWORD_CONFIG, "tmgps");
//    props.put(JdbcSourceConnectorConfig.DIALECT_NAME_CONFIG, "TOracleDatabaseDialect");

    props.put(JdbcSourceConnectorConfig.MODE_CONFIG, JdbcSourceConnectorConfig.MODE_BULK);
    props.put(JdbcSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "guan_java_");

    String tableName = "TEST_CONNECTOR_GUAN";
    props.put(JdbcSourceConnectorConfig.TABLE_WHITELIST_CONFIG, tableName);
    props.put(JdbcSourceTaskConfig.TABLES_CONFIG, tableName);
    props.put(JdbcSourceTaskConfig.NUMERIC_MAPPING_CONFIG, NumericMapping.BEST_FIT.toString());



    props.put(JsonConverterConfig.TYPE_CONFIG, ConverterType.VALUE.getName());
    props.put(JsonConverterConfig.DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
    props.put(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
    return props;
  }

  private RecordHeaders convertHeaderFor(SourceRecord record) {
    Headers headers = record.headers();
    RecordHeaders result = new RecordHeaders();
    if (headers != null) {
      String topic = record.topic();
      for (Header header : headers) {
        String key = header.key();
        byte[] rawHeader = headerConverter.fromConnectHeader(topic, key, header.schema(), header.value());
        result.add(key, rawHeader);
      }
    }
    return result;
  }
}
