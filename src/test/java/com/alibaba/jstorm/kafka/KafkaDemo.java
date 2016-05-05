package com.alibaba.jstorm.kafka;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.kafka.KafkaSpout;
import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.jstorm.utils.LoadConf;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.SpoutDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class KafkaDemo{
	private static Logger LOG = LoggerFactory.getLogger(KafkaDemo.class);

    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "bolt.parallel";
    
    private static Map conf = new HashMap<Object, Object>();

	
    public static void SetBuilder(TopologyBuilder builder, Map conf) {

        int spout_Parallelism_hint = JStormUtils.parseInt(
                conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT), 1);
        int bolt_Parallelism_hint = JStormUtils.parseInt(
                conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT), 2);

        KafkaSpout kafkaSpout = new KafkaSpout();
        
        SpoutDeclarer spoutDeclarer= builder.setSpout("kafka-spout",kafkaSpout, spout_Parallelism_hint);
        builder.setBolt("kafka-bolt", new KafkaBolt(),bolt_Parallelism_hint)
        		.shuffleGrouping("kafka-spout");

        int ackerNum = JStormUtils.parseInt(
                conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1);
        Config.setNumAckers(conf, ackerNum);
        // conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 6);
        // conf.put(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS, 20);
        // conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);

        int workerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS), 20);
        conf.put(Config.TOPOLOGY_WORKERS, workerNum);

    }
    
	public static void SetLocalTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        conf.put(TOPOLOGY_BOLT_PARALLELISM_HINT, 1);
        SetBuilder(builder, conf);
        
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("kafka-test", conf, builder.createTopology());

        //Thread.sleep(60000);
        //cluster.killTopology("SplitMerge");
        //cluster.shutdown();
    }
	
	public static void LoadConf(String arg) {
        if (arg.endsWith("yaml")) {
            conf = LoadConf.LoadYaml(arg);
        } else {
            conf = LoadConf.LoadProperty(arg);
        }
    }

    public static boolean local_mode(Map conf) {
        String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
        if (mode != null) {
            if (mode.equals("local")) {
                return true;
            }
        }

        return false;

    }

    public static void main(String[] args) throws Exception {
//        if (args.length == 0) {
//            System.err.println("Please input configuration file");
//            System.exit(-1);
//        }

        LoadConf("detail.yaml");
        SetLocalTopology();
    }
}