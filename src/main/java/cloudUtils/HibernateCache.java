package cloudUtils;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.cfg.Configuration;
import org.hibernate.exception.ConstraintViolationException;
import redis.clients.jedis.JedisPool;
import tukano.api.Result;
import cloudUtils.RedisCache;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

public class HibernateCache {

    private static final String HIBERNATE_CFG_FILE = "hibernate.cfg.xml";
    private SessionFactory sessionFactory;
    private static HibernateCache instance;
    private static JedisPool pool;

    private HibernateCache() {
        try {
            sessionFactory = new Configuration().configure().buildSessionFactory();
            pool = RedisCache.getCachePool();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the Hibernate instance, initializing if necessary. Requires a
     * configuration file (hibernate.cfg.xml)
     *
     * @return
     */
    synchronized public static HibernateCache getInstance() {
        if (instance == null)
            instance = new HibernateCache();
        return instance;
    }

    public Result<Void> persistOne(Object  obj) {
        // update cache before even doing a get
        RedisCache.putOne(obj, pool);
        return execute( (hibernate) -> {
            hibernate.persist( obj );
        });
    }

    public <T> Result<T> updateOne(T obj) {
        return execute( hibernate -> {
            var res = hibernate.merge( obj );
            if( res == null)
                return Result.error( Result.ErrorCode.NOT_FOUND );

            // update cache
            RedisCache.putOne(res, pool);
            return Result.ok( res );
        });
    }

    public <T> Result<T> deleteOne(T obj) {
        RedisCache.deleteOne(obj, pool);
        return execute( hibernate -> {
            hibernate.remove( obj );
            return Result.ok( obj );
        });
    }

    public <T> Result<T> getOne(Object id, Class<T> clazz) {
        // check cache to be faster
        T resultObj = RedisCache.getOne(id, clazz, pool);
        if (resultObj == null){
            // only check database if value not in cache
            try (var session = sessionFactory.openSession()) {
                var res = session.find(clazz, id);
                if (res == null) {
                    return Result.error(Result.ErrorCode.NOT_FOUND);
                } else {
                    RedisCache.putOne(res, pool);
                    return Result.ok(res);
                }
            } catch (Exception e) {
                throw e;
            }
        } else {
            return Result.ok(resultObj);
        }
    }

    public <T> List<T> sql(String sqlStatement, Class<T> clazz) {
        try (var session = sessionFactory.openSession()) {
            var query = session.createNativeQuery(sqlStatement, clazz);
            return query.list();
        } catch (Exception e) {
            throw e;
        }
    }

    public <T> Result<T> execute(Consumer<Session> proc) {
        return execute( (hibernate) -> {
            proc.accept( hibernate);
            return Result.ok();
        });
    }

    public <T> Result<T> execute(Function<Session, Result<T>> func) {
        Transaction tx = null;
        try (var session = sessionFactory.openSession()) {
            tx = session.beginTransaction();
            var res = func.apply( session );
            session.flush();
            tx.commit();
            return res;
        }
        catch (ConstraintViolationException __) {
            return Result.error(Result.ErrorCode.CONFLICT);
        }
        catch (Exception e) {
            if( tx != null )
                tx.rollback();

            e.printStackTrace();
            throw e;
        }
    }

}
