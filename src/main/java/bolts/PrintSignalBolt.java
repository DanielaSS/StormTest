package bolts;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.Map;

/**
 * Created by sistemas on 7/25/17.
 */
public class PrintSignalBolt  extends BaseRichBolt {

    private OutputCollector collector;
    private TopologyContext context;
    private Map conf;
    static Logger LOG = LoggerFactory.getLogger(PrintSignalBolt.class);

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.context = context;
        this.conf = stormConf;
        this.collector=collector;
    }

    @Override
    public void execute(Tuple input) {
        BufferedWriter output;
        try {
            //Creacion de los archivos con nombre la identificacion del paciente, mas los filtros aplicados a la senal
            output = new BufferedWriter(new FileWriter("/home/local/LABINFO/2105684/Public/"+input.getString(0)+".out", true));
            LOG.info("PrintSignal .______________________________________________________."+input.getString(0));
            //Agregamos el valor de la senal procesada
            double [] temp=(double[]) input.getValue(1);
            for (int i =0;i<temp.length;i++){
                LOG.info("PrintSignal .______________________________________________________."+temp[i]+"");
                output.append(temp[i]+"");
                output.newLine();
            }
            output.close();
            collector.ack(input);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error("Error executing tuple! "+e.getMessage()+" "+ PrintSignalBolt.class);
            collector.fail(input);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
