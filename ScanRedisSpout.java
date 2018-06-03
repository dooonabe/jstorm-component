package spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;

import java.util.*;

/**
 * Created by on 2018/5/24.
 * I designed this spout for this reason:prevent one key from being consumered by different bolt thread (the next bolt should * use shuffingfield pattern),i want one key consumered by on bolt thread.
 */
public class ScanRedisSpout extends BaseRichSpout {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanRedisSpout.class);
    private static final String REDIS_CONFIG_KEY = "redisConfigKey";
    private static final String PREFIX_KEY = "prefixConfigKey";
    private SpoutOutputCollector collector;
    private JedisCluster jedisCluster;
    private ScanParams scanParams;
    private String STPRE;
    private Set<String> allKeys;
    private List<JedisScanBean> jedisList;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
        jedisCluster = JedisClusterConnection.createJedisCluster((Map<String, Object>) map.get(REDIS_CONFIG_KEY));
        Map prefix = (Map) map.get(PREFIX_KEY);
        STPRE = prefix.get(StormTpoConfig.SESSION_TIME_PREFIX_CONFIG).toString();
        scanParams = new ScanParams().count(1000).match(STPRE + "*");
        allKeys = new HashSet<>();
        jedisList = getJedisScanBean(jedisCluster);
    }

    @Override
    public void nextTuple() {
        try {
            if (allKeys.size() == 0) {
                getKeys();
                if (allKeys.size() != 0) {
                    allKeys.stream().forEach(key -> collector.emit(new Values(key), key));
                }
            } else {

            }
        } catch (Exception e) {
            LOGGER.info("error:{}", e);
            allKeys = new HashSet<>();
        }
    }

    private void getKeys() {
        for (JedisScanBean jedisScanBean : jedisList) {
            ScanResult<String> scanResult = jedisScanBean.getJedis().scan(jedisScanBean.getCursor(), jedisScanBean.getScanParams());
            allKeys.addAll(scanResult.getResult());
            jedisScanBean.setCursor(scanResult.getStringCursor());
        }
    }

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
        LOGGER.info("ack:{}", msgId);
        allKeys.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
        LOGGER.info("fail:{}", msgId);
        allKeys.remove(msgId);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("key"));
    }

    /**
     * 在redis集群的各个主节点查询key，并将结果集放入set中
	 * get key from every redis cluster nodes and put all keys in a SET
     * @param jedisCluster
     * @return
     */
    private List<JedisScanBean> getJedisScanBean(JedisCluster jedisCluster) {
        Map<String, JedisPool> nodes = jedisCluster.getClusterNodes();
        Set<String> keySet = new HashSet<>();
        List<JedisScanBean> list = new ArrayList<>();
        String cursor = ScanParams.SCAN_POINTER_START;
        for (Map.Entry<String, JedisPool> entry : nodes.entrySet()) {
            Jedis jedis = entry.getValue().getResource();
            JedisScanBean bean = new JedisScanBean(jedis, scanParams, cursor);
            list.add(bean);
        }
        return list;
    }


    class JedisScanBean {
        private Jedis jedis;
        private ScanParams scanParams;
        private String cursor;

        JedisScanBean() {
        }

        JedisScanBean(Jedis jedis, ScanParams scanParams, String cursor) {
            this.jedis = jedis;
            this.scanParams = scanParams;
            this.cursor = cursor;
        }

        public Jedis getJedis() {
            return jedis;
        }

        public void setJedis(Jedis jedis) {
            this.jedis = jedis;
        }

        public ScanParams getScanParams() {
            return scanParams;
        }

        public void setScanParams(ScanParams scanParams) {
            this.scanParams = scanParams;
        }

        public String getCursor() {
            return cursor;
        }

        public void setCursor(String cursor) {
            this.cursor = cursor;
        }
    }

}
