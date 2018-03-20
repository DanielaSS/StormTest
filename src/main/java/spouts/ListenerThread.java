package spouts;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by sistemas on 7/28/17.
 */
public class ListenerThread extends Thread {

    LinkedBlockingQueue<String> queue;
    JedisPool pool;
    String channel;

    public ListenerThread(LinkedBlockingQueue<String> queue, JedisPool pool, String channel) {
        this.queue = queue;
        this.pool = pool;
        this.channel = channel;
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
            //Se suscribe al canal
            jedis.subscribe(listener, channel);
        } finally {
            pool.returnResource(jedis);
        }
    }
}
