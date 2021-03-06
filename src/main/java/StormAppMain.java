import bolts.*;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import spouts.RedisSpout;

/**
 * Created by sistemas on 7/14/17.
 */
public class StormAppMain {
    private static String REDIS_HOST="10.2.78.211";
    private static int PORT=6379;


    public static void main(String[] args) throws Exception{

        //Configuracion
        Config config = new Config(); //Instancia del objeto Config
        config.setDebug(true); //Habilitar la depuracion de la aplicacion
        config.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1); //Maximo de tuplas que pueden estar pendientes en una tarea del spout
       // config.setNumWorkers(3); //worker process puede tener una maquina del clúster.
        config.put(Config.STORM_ID,"analysisSignalsRedis");//Nombre de la topologia

        //Topologia
        //Constructor para la topologia
        TopologyBuilder builder = new TopologyBuilder();
        //spout con redis server suscrito al canal (CHANNEL)
        builder.setSpout("redisspout",new RedisSpout(REDIS_HOST,PORT,"Paciente1"));
        builder.setSpout("redisspout2",new RedisSpout(REDIS_HOST,PORT,"Paciente2"));
        //bolts
        //FilterOne recibe informacion desde los spouts
        builder.setBolt("filterone", new FilterOne())
                .shuffleGrouping("redisspout")
                .shuffleGrouping("redisspout2");
        //Crea un archivo con los datos procesados por cada filtro.
        builder.setBolt("printsignal",new PrintSignalBolt())
                .shuffleGrouping("filterone");
        /*
        //Ejecucion en el cluster.
        StormSubmitter.submitTopology("analysisSignalsRedis", config, builder.createTopology());
        */

        //Ejecucion localcluster
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("analysisSignals", config, builder.createTopology());

    }
}
