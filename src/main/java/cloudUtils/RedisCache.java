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

    // used to determine that clases Likes and Follow are not to be cached
    private static final String NO_CACHE = "NO_CACHE";

    // time in seconds for a key to expire if no operations are done in it
    private static final int KEY_EXPIRATION = 100;

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
            Log.info("buildClassToString() object not be cached " + obj);
            return NO_CACHE;
        }
    }

    public static <T> String buildClassToString(String id, Class<T> clazz){

        if(clazz == User.class) {
            return USER_KEY + id;
        } else if (clazz == Short.class) {
            return SHORT_KEY + id;
        } else {
            Log.info("buildClassToString() object not to be cached " + id);
            return NO_CACHE;
        }
    }

    public static void deleteOne(Object obj, JedisPool pool){

        try (Jedis jedis = pool.getResource()){
            String key = buildClassToString(obj);

            if (key.equals(NO_CACHE)){
                return;
            }

            Log.info("Deleted from cache " + key);
            jedis.del(key);
        } catch (Exception e){
            e.printStackTrace();
            throw e;
        }
    }

    // puts one object to cache
    public static void putOne(Object obj, JedisPool pool){

        try (Jedis jedis = pool.getResource()) {

            String key = buildClassToString(obj);

            if (key.equals(NO_CACHE)){
                return;
            }

            String value = JSON.encode( obj );

            Log.info("Put an Object in Cache " + key);
            jedis.set(key, value);
            jedis.expire(key, KEY_EXPIRATION);

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    // id corresponds with an object ID in database
    public static <T> T getOne(Object id, Class<T> clazz, JedisPool pool) {

        try (Jedis jedis = pool.getResource()){

            String key = buildClassToString((String) id, clazz);

            if (key.equals(NO_CACHE)){
                return null;
            }

            String value = jedis.get(key);

            if (value == null){
                return null;
            }

            Log.info("Get Object from Cache " + key);
            jedis.expire(key, KEY_EXPIRATION);
            return JSON.decode(value, clazz);
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

}
