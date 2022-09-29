package org.apache.flink.streaming.connectors.redis.datastream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.RedisSinkOptions;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisHandlerServices;
import org.apache.flink.streaming.connectors.redis.common.hanlder.RedisMapperHandler;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisSinkMapper;
import org.apache.flink.streaming.connectors.redis.table.RedisSinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.types.DataType;

import org.junit.Test;

import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_COMMAND;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_MODE;
import static org.apache.flink.streaming.connectors.redis.descriptor.RedisValidator.REDIS_SINGLE;
import static org.apache.flink.streaming.connectors.redis.table.SQLTest.REDIS_HOST;
import static org.apache.flink.streaming.connectors.redis.table.SQLTest.REDIS_PASSWORD;
import static org.apache.flink.streaming.connectors.redis.table.SQLTest.REDIS_PORT;

/** Created by jeff.zou on 2021/2/26. */
public class DataStreamTest {

    //    private RedisServer redisServer;
    //
    //    @Before
    //    public void before() throws Exception {
    //        redisServer = RedisServer.builder().port(6379).setting("maxheap 51200").build();
    //        redisServer.start();
    //    }

    //
    //    @After
    //    public void stopRedis() {
    //        redisServer.stop();
    //    }

    @Test
    public void testDateStreamInsert() throws Exception {

        Configuration configuration = new Configuration();
        configuration.setString(REDIS_MODE, REDIS_SINGLE);
        configuration.setString(REDIS_COMMAND, RedisCommand.HSET.name());

        RedisSinkMapper redisMapper =
                (RedisSinkMapper)
                        RedisHandlerServices.findRedisHandler(
                                        RedisMapperHandler.class, configuration.toMap())
                                .createRedisMapper(configuration);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        BinaryRowData binaryRowData = new BinaryRowData(3);
        BinaryRowWriter binaryRowWriter = new BinaryRowWriter(binaryRowData);
        binaryRowWriter.writeString(0, StringData.fromString("tom"));
        binaryRowWriter.writeString(1, StringData.fromString("math"));
        binaryRowWriter.writeString(2, StringData.fromString("152"));

        DataStream<BinaryRowData> dataStream = env.fromElements(binaryRowData, binaryRowData);

        String[] columnNames = new String[]{"name", "subject", "scope"};
        DataType[] columnDataTypes =
                new DataType[]{DataTypes.STRING(), DataTypes.STRING(), DataTypes.STRING()};
        TableSchema resolvedSchema = TableSchema.builder().fields(columnNames, columnDataTypes).build();

        RedisSinkOptions redisSinkOptions =
                new RedisSinkOptions.Builder().setMaxRetryTimes(3).build();
        FlinkJedisConfigBase conf =
                new FlinkJedisPoolConfig.Builder()
                        .setHost(REDIS_HOST)
                        .setPort(REDIS_PORT)
                        .setPassword(REDIS_PASSWORD)
                        .build();

        RedisSinkFunction redisSinkFunction =
                new RedisSinkFunction<>(conf, redisMapper, redisSinkOptions, resolvedSchema);

        dataStream.addSink(redisSinkFunction).setParallelism(1);
        env.execute("RedisSinkTest");
    }
}
