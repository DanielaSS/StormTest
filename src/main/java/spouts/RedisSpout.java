package spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sistemas on 7/28/17.
 */

public class RedisSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private TopologyContext context;
    private Map conf;
    static Logger LOG = LoggerFactory.getLogger(RedisSpout.class); //Logs
    private  String host, channel; //HOST del redis-server y canal al cual se suscribe
    private int port;  //puerto por donde corre el servidor de redis (redis-server)
    private static LinkedBlockingQueue<String> queue; //cola para procesar los mensajes
    private static JedisPool pool; //comunicacion con redis

    /**
     * Constructor del spout
     *
     * @param host
     * @param port
     * @param channel
     */
    public RedisSpout(String host, int port, String channel){
        this.host = host;
        this.port = port;
        this.channel = channel;
    }
    class ListenerThread extends Thread {
        LinkedBlockingQueue<String> queue;
        JedisPool pool;
        String pattern;

        public ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool, String pattern) {
            this.queue = queue;
            this.pool = pool;
            this.pattern = pattern;
        }

        public void run() {

            JedisPubSub listener = new JedisPubSub() {

                @Override
                public void onMessage(String channel, String message) {
                    queue.offer(message);
                }

                @Override
                public void onPMessage(String pattern, String channel, String message) {
                    queue.offer(message);
                }

                @Override
                public void onPSubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void onPUnsubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void onSubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }

                @Override
                public void onUnsubscribe(String channel, int subscribedChannels) {
                    // TODO Auto-generated method stub

                }
            };

            Jedis jedis = pool.getResource();
            try {
                jedis.psubscribe(listener, pattern);
            } finally {
                pool.returnResource(jedis);
            }
        }
    };

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector= collector;
        this.context = context;
        this.conf = conf;
        //Se crea una cola con capacidad de 1000 mensajes
        queue = new LinkedBlockingQueue<String>(1000);
        pool = new JedisPool(new JedisPoolConfig(),host,port); //canal de comunicacion con el redis server
        ListenerThread listener = new ListenerThread(queue,pool,channel);
        //Para recibir constantemente del server
        listener.start(); //se empieza  a escuchar
    }

    @Override
    public void nextTuple() {
        //Si en la cola hay algo se porcesa y se emite a los bolts
        if(!queue.isEmpty()) {
            String[] raw = queue.poll().trim().split("#"); //Formato: ID#8datos, el número representa una parte de la señal
            LOG.info("Spout redis_____________________________________________________________________" + raw[0] + " " + raw[1]);
            collector.emit(new Values(raw[0]+channel, raw[1]));
        }
    }

    @Override
    public void declareOutputFields (OutputFieldsDeclarer declarer){
            declarer.declare(new Fields("ID", "signal"));
    }

    @Override
    public void close() {
        pool.destroy();
    }

}

