package utils;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import cloudUtils.HibernateCache;
import com.azure.cosmos.CosmosContainer;
import org.hibernate.Session;
import cloudUtils.NoSQLCosmos;
import cloudUtils.NoSQLCosmosCache;
import utils.Hibernate;

import tukano.api.Result;

public class DB {

	public static final boolean useCache = true;

	// if false then use NoSQL
	public static final boolean usePostegre = false;

	// use only with Postegre/Hibernate
	public static <T> List<T> sql(String query, Class<T> clazz) {
		return useCache ? HibernateCache.getInstance().sql(query, clazz) : Hibernate.getInstance().sql(query, clazz);
	}

	// use only with CosmosDB/NoSQL
	public static <T, Q> List<T> sqlDB(String query, Class<T> clazz, Class<Q> seeFrom) {
		return useCache ? NoSQLCosmosCache.getInstance().query(clazz, query, seeFrom).value()
				: NoSQLCosmos.getInstance().query(clazz, query, seeFrom).value();
	}

	// not in use
	/*
	public static <T> List<T> sql(Class<T> clazz, String fmt, Object ... args) {
		return useCache ? HibernateCache.getInstance().sql(String.format(fmt, args), clazz)
				: Hibernate.getInstance().sql(String.format(fmt, args), clazz);
	}*/
	
	public static <T> Result<T> getOne(String id, Class<T> clazz) {

		if (usePostegre){
			return useCache ? HibernateCache.getInstance().getOne(id, clazz)
					: Hibernate.getInstance().getOne(id, clazz);
		} else {
			return useCache ? NoSQLCosmosCache.getInstance().getOne(id, clazz) : NoSQLCosmos.getInstance().getOne(id, clazz);
		}
	}
	
	public static <T> Result<T> deleteOne(T obj) {

		if (usePostegre){
			return useCache ? HibernateCache.getInstance().deleteOne(obj)
					: Hibernate.getInstance().deleteOne(obj);
		} else {
			return useCache ? NoSQLCosmosCache.getInstance().deleteOne(obj) : NoSQLCosmos.getInstance().deleteOne(obj);
		}
	}
	
	public static <T> Result<T> updateOne(T obj) {

		if (usePostegre){
			return useCache ? HibernateCache.getInstance().updateOne(obj)
					: Hibernate.getInstance().updateOne(obj);
		} else {
			return useCache ? NoSQLCosmosCache.getInstance().updateOne(obj) : NoSQLCosmos.getInstance().updateOne(obj);
		}

	}
	
	public static <T> Result<T> insertOne( T obj) {

		if (usePostegre){
			return useCache ? Result.errorOrValue(HibernateCache.getInstance().persistOne(obj), obj)
					: Result.errorOrValue(Hibernate.getInstance().persistOne(obj), obj);
		} else {
			return useCache ? NoSQLCosmosCache.getInstance().insertOne(obj) : NoSQLCosmos.getInstance().insertOne(obj);
		}

	}

	// use this only for Postegre/Hibernate
	public static <T> Result<T> transaction( Consumer<Session> c) {
			return useCache ? HibernateCache.getInstance().execute( c::accept )
					: Hibernate.getInstance().execute( c::accept );
	}

	public static <T> Result<T> transaction( Function<Session, Result<T>> func) {
		return useCache ? HibernateCache.getInstance().execute( func ) : Hibernate.getInstance().execute( func );
	}
}
