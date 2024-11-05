package cloudUtils;

import com.azure.cosmos.*;
import com.azure.cosmos.models.*;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import redis.clients.jedis.JedisPool;
import tukano.api.Result;
import org.hibernate.Session;
import tukano.api.Result;
import cloudUtils.PropsCloud;
import cloudUtils.RedisCache;

import tukano.api.User;
import tukano.api.Short;
import tukano.impl.data.Following;
import tukano.impl.data.Likes;

public class NoSQLCosmosCache {

    private static final String CONTAINER_USERS = "users";
    private static final String CONTAINER_SHORTS = "shorts";
    private static final String CONTAINER_FOLLOW = "followers";
    private static final String CONTAINER_LIKES = "likes";

    private static final String KEY_PATH = "/id"; // Adjust as needed

    private static final int NOT_FOUND = 404;

    private static NoSQLCosmosCache instance;

    private CosmosClient client;
    private static CosmosDatabase db;

    private static JedisPool pool;

    public static synchronized NoSQLCosmosCache getInstance() {
        if( instance != null)
            return instance;

        pool = RedisCache.getCachePool();

        PropsCloud.load(PropsCloud.PROPS_PATH);
        String connect_url = PropsCloud.get("COSMOSDB_URL", "");
        String connect_key = PropsCloud.get("COSMOSDB_KEY", "");
        String connect_db = PropsCloud.get("COSMOSDB_DATABASE", "");

        CosmosClient client = new CosmosClientBuilder()
                .endpoint(connect_url)
                .key(connect_key)
                //.directMode()
                .gatewayMode()
                // replace by .directMode() for better performance
                .consistencyLevel(ConsistencyLevel.SESSION)
                .connectionSharingAcrossClientsEnabled(true)
                .contentResponseOnWriteEnabled(true)
                .buildClient();

        db = client.getDatabase(connect_db);

        instance = new cloudUtils.NoSQLCosmosCache(client);
        return instance;
    }

    public NoSQLCosmosCache(CosmosClient client) {
        this.client = client;
    }

    private synchronized void init() {
        if( db != null)
            return;

        PropsCloud.load(PropsCloud.PROPS_PATH);
        String connect_db = PropsCloud.get("COSMOSDB_DATABASE", "");

        db = client.getDatabase(connect_db);

    }

    public void close() {
        client.close();
    }

    private <T> CosmosContainer getCosmosContainer(Class<T> clazz){

            if (clazz == User.class){
                return createOrReturnContainer(CONTAINER_USERS);
            } else if (clazz == Short.class){
                return createOrReturnContainer(CONTAINER_SHORTS);
            } else if (clazz == Following.class){
                return createOrReturnContainer(CONTAINER_FOLLOW);
            } else {
                return createOrReturnContainer(CONTAINER_LIKES);
            }
    }

    private CosmosContainer createOrReturnContainer(String name){
        try {
            CosmosContainer container = db.getContainer(name);
            container.read();
            return container;
        } catch (CosmosException e){
            if (e.getStatusCode() == NOT_FOUND){
                CosmosContainerProperties containerProps = new CosmosContainerProperties(name, KEY_PATH);
                db.createContainer(containerProps);
                return db.getContainer(name);
            }
            throw e;
        }
    }

    public <T> tukano.api.Result<T> getOne(String id, Class<T> clazz) {
        T resultOBj = RedisCache.getOne(id, clazz, pool);
        CosmosContainer container = getCosmosContainer(clazz);
        if(resultOBj == null){
            var result = tryCatch( () -> container.readItem(id, new PartitionKey(id), clazz).getItem());
            RedisCache.putOne(result.value(), pool);
            return result;
        }
        return Result.ok(resultOBj);
    }

    @SuppressWarnings("unchecked")
    public <T> tukano.api.Result<T> deleteOne(T obj) {
        RedisCache.deleteOne(obj, pool);
        CosmosContainer container = getCosmosContainer(obj.getClass());
        return (tukano.api.Result<T>) tryCatch( () -> container.deleteItem(obj, new CosmosItemRequestOptions()).getItem());
    }

    public <T> tukano.api.Result<T> updateOne(T obj) {
        RedisCache.putOne(obj, pool);
        CosmosContainer container = getCosmosContainer(obj.getClass());
        return tryCatch( () -> container.upsertItem(obj).getItem());
    }

    public <T> tukano.api.Result<T> insertOne(T obj) {
        RedisCache.putOne(obj, pool);
        CosmosContainer container = getCosmosContainer(obj.getClass());
        return tryCatch( () -> container.createItem(obj).getItem());
    }

    public <T, Q> tukano.api.Result<List<T>> query(Class<T> clazz, String queryStr, Class<Q> seeFromm) {
        return tryCatch(() -> {
            CosmosContainer container = getCosmosContainer(seeFromm);
            var res = container.queryItems(queryStr, new CosmosQueryRequestOptions(), clazz);
            var returnRes = res.stream().toList();
            System.out.println("\n\n" + returnRes );
            return res.stream().toList();
        });
    }

    <T> tukano.api.Result<T> tryCatch(Supplier<T> supplierFunc) {
        try {
            return tukano.api.Result.ok(supplierFunc.get());
        } catch( CosmosException ce ) {
            ce.printStackTrace();
            return tukano.api.Result.error ( errorCodeFromStatus(ce.getStatusCode() ));
        } catch( Exception x ) {
            x.printStackTrace();
            return tukano.api.Result.error( tukano.api.Result.ErrorCode.INTERNAL_ERROR);
        }
    }

    static tukano.api.Result.ErrorCode errorCodeFromStatus(int status ) {
        return switch( status ) {
            case 200 -> tukano.api.Result.ErrorCode.OK;
            case 404 -> tukano.api.Result.ErrorCode.NOT_FOUND;
            case 409 -> tukano.api.Result.ErrorCode.CONFLICT;
            default -> tukano.api.Result.ErrorCode.INTERNAL_ERROR;
        };
    }


}
