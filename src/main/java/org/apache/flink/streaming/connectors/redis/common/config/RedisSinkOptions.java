package org.apache.flink.streaming.connectors.redis.common.config;

/**
 * sink options. @Author: Jeff Zou @Date: 2022/9/28 16:36
 */
public class RedisSinkOptions {
    private final int maxRetryTimes;

    private final String additionalKey;
    private final RedisValueDataStructure redisValueDataStructure;

    public int getMaxRetryTimes() {
        return maxRetryTimes;
    }

    public String getAdditionalKey() {
        return additionalKey;
    }

    public RedisValueDataStructure getRedisValueDataStructure() {
        return redisValueDataStructure;
    }

    public RedisSinkOptions(int maxRetryTimes, RedisValueDataStructure redisValueDataStructure, String additionalKey) {
        this.maxRetryTimes = maxRetryTimes;
        this.additionalKey = additionalKey;
        this.redisValueDataStructure = redisValueDataStructure;
    }

    /**
     * RedisSinkOptions.Builder.
     */
    public static class Builder {
        private int maxRetryTimes;

        private String additionalKey;

        private RedisValueDataStructure redisValueDataStructure;

        public Builder setRedisValueDataStructure(RedisValueDataStructure redisValueDataStructure) {
            this.redisValueDataStructure = redisValueDataStructure;
            return this;
        }

        public Builder setMaxRetryTimes(int maxRetryTimes) {
            this.maxRetryTimes = maxRetryTimes;
            return this;
        }


        public Builder setAdditionalKey(String additionalKey) {
            this.additionalKey = additionalKey;
            return this;
        }

        public RedisSinkOptions build() {
            return new RedisSinkOptions(maxRetryTimes, redisValueDataStructure, additionalKey);
        }
    }
}
