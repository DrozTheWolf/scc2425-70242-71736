package cloudUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import cloudUtils.PropsCloud;

import tukano.api.User;
import tukano.api.Short;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;
import utils.JSON;

import java.util.logging.Logger;


public class RedisCache {

    private static final int REDIS_PORT = 6380;
    private static final int REDIS_TIMEOUT = 1000;
    private static final boolean Redis_USE_TLS = true;

    private static final String USER_KEY = "user:";
    private static final String SHORT_KEY = "short:";
    private static final String FOLLOW_KEY = "follow:";
    private static final String LIKE_KEY = "like:";

    private static JedisPool instance;
    final private static Logger Log = Logger.getLogger(RedisCache.class.getName());

    public synchronized static JedisPool getCachePool() {
        if( instance != null)
            return instance;

        PropsCloud.load(PropsCloud.PROPS_PATH);
        String redis_key = PropsCloud.get("REDIS_KEY", "");
        String redis_url = PropsCloud.get("REDIS_URL", "");

        var poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(128);
        poolConfig.setMaxIdle(128);
        poolConfig.setMinIdle(16);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setBlockWhenExhausted(true);
        instance = new JedisPool(poolConfig, redis_url, REDIS_PORT, REDIS_TIMEOUT, redis_key, Redis_USE_TLS);
        return instance;
    }

    public static String buildClassToString(Object obj){

        if(obj instanceof User) {
            return USER_KEY + ((User) obj).getUserId();
        } else if (obj instanceof Short) {
            return SHORT_KEY + ((Short) obj).getShortId();
        } else {
            Log.info("buildClassToString() failed cause obj is not instanceOf Anything");
            throw new RuntimeException();
        }
    }

    public static <T> String buildClassToString(String id, Class<T> clazz){

        if(clazz == User.class) {
            return USER_KEY + id;
        } else if (clazz == Short.class) {
            return SHORT_KEY + id;
        } else {
            Log.info("buildClassToString() failed cause obj is not instanceOf Anything");
            throw new RuntimeException();
        }
    }

    public static void deleteOne(Object obj, JedisPool pool){
        Log.info("Deleted from cache");
        try (Jedis jedis = pool.getResource()){
            String key = buildClassToString(obj);
            jedis.del(key);
        } catch (Exception e){
            e.printStackTrace();
            throw e;
        }
    }

    // puts one object to cache
    public static void putOne(Object obj, JedisPool pool){
        Log.info("Put an Object in Cache");
        try (Jedis jedis = pool.getResource()) {

            String key = buildClassToString(obj);
            String value = JSON.encode( obj );

            jedis.set(key, value);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    // id corresponds with an object ID in database
    public static <T> T getOne(Object id, Class<T> clazz, JedisPool pool) {
        Log.info("Get Object from Cache");
        try (Jedis jedis = pool.getResource()){

            String key = buildClassToString((String) id, clazz);
            String value = jedis.get(key);

            if (value == null){
                return null;
            }

            // String type = key.split(":")[0];

            return JSON.decode(value, clazz);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

}
