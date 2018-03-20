package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

/**
 * Created by Daniela on 7/14/17.
 */
public class FilterOne extends BaseRichBolt {

    private OutputCollector collector;
    private TopologyContext context;
    private Map conf;
    static Logger LOG = LoggerFactory.getLogger(FilterOne.class);

    /**
     * Called when a task for this component is initialized within a worker on the cluster. It provides the bolt with the environment in which the bolt executes.
     * @param stormConf
     * @param context
     * @param collector
     */
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context = context;
        this.conf = stormConf;
        this.collector = collector;
    }
    /**
     * Declare the output schema for all the streams of this topology.
     * @param declarer this is used to declare output stream ids, output fields, and whether or not each output stream is a direct stream
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ID","signal"));
    }
    /**
     * Process the input tuple and optionally emit new tuples based on the input tuple.
     * All acking is managed for you.
     * @param input
     */
    @Override
    public void execute(Tuple input) {
        try {
            String[] datos=input.getString(1).trim().split(" ");
            int[] signalT=new int[datos.length];
            for (int i=0;i<datos.length;i++){
                signalT[i]=Integer.parseInt(datos[i])*2;
            }
            LOG.info("Filter one .______________________________________________________."+ Arrays.toString(datos));
            collector.emit(new Values(input.getString(0), signalT));
            collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Error executing tuple! "+e.getMessage()+" "+ FilterOne.class);
            collector.fail(input);
        }
    }
}
