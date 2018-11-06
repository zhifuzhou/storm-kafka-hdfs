package com.suning.hdfstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import storm.kafka.*;

import java.util.Arrays;
import java.util.Map;

public class DistributeWordTopology {
    public static class KafkaWordToUpperCase extends BaseRichBolt{
        private static final Log LOG= LogFactory.getLog(KafkaWordToUpperCase.class);
        private OutputCollector collector;
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector=collector;
        }

        public void execute(Tuple input) {
            String line=input.getString(0).trim();
            if(!line.equals("")){
                String upperLine=line.toUpperCase();
                collector.emit(input,new Values(upperLine,upperLine.length()));
            }
            collector.ack(input);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("line","len"));
        }
    }
    public static class RealtimeBolt extends BaseRichBolt{
        private static Log LOG=LogFactory.getLog(KafkaWordToUpperCase.class);
        private OutputCollector collector;
        public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
            this.collector=collector;
        }

        public void execute(Tuple input) {
            String line=input.getString(0).trim();
            collector.ack(input);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {

        }
    }
    public static void main(String[] args){
        String zks="h1:2181,h2:2181,h3:2181";
        String topic="my-replicated-topic5";
        String zkRoot="/storm";
        String id="word";

        BrokerHosts brokerHosts=new ZkHosts(zks);
        SpoutConfig spoutConfig=new SpoutConfig(brokerHosts,topic,zkRoot,id);
        spoutConfig.scheme=new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.forceFromStart=false;
        spoutConfig.zkServers= Arrays.asList(new String[]{"h1","h2","h3"});
        spoutConfig.zkPort=2181;

        RecordFormat format=new DelimitedRecordFormat().withFieldDelimiter("\t");
        SyncPolicy syncPolicy=new CountSyncPolicy(1000);
        FileRotationPolicy rotationPolicy=new TimedRotationPolicy(1.0f, TimedRotationPolicy.TimeUnit.MINUTES);
        FileNameFormat fileNameFormat=new DefaultFileNameFormat()
                .withPath("/storm/")
                .withPrefix("app_")
                .withExtension(".log");
        HdfsBolt hdfsBolt=new HdfsBolt().withFsUrl("hdfs:/h1:8020")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(format)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        TopologyBuilder builder=new TopologyBuilder();
        builder.setSpout("kafka-reader",new KafkaSpout(spoutConfig),5);
        builder.setBolt("to-upper",new KafkaWordToUpperCase(),3).shuffleGrouping("kafka-reader");
        builder.setBolt("hdfs-bolt",(IBasicBolt) hdfsBolt,2).shuffleGrouping("to-upper");
        builder.setBolt("realtime",new RealtimeBolt(),2).shuffleGrouping("to-upper");

        Config config=new Config();
        String name=DistributeWordTopology.class.getSimpleName();
        if(args!=null && args.length>0){
            String nimbus=args[0];
            config.put(Config.NIMBUS_HOST,nimbus);
            config.setNumWorkers(3);
            try {
                StormSubmitter.submitTopologyWithProgressBar(name,config,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }else{
            config.setMaxTaskParallelism(3);
            LocalCluster localCluster=new LocalCluster();
            localCluster.submitTopology(name,config,builder.createTopology());
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            localCluster.shutdown();
        }
    }
}
