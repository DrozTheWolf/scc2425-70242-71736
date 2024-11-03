package utils;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import cloudUtils.HibernateCache;
import com.azure.cosmos.CosmosContainer;
import utils.Hibernate;
import org.hibernate.Session;
import cloudUtils.NoSQLCosmos;

import tukano.api.Result;

public class DB {

	public static final boolean useCache = false;

	// if false then use NoSQL
	public static final boolean usePostegre = true;


	public static <T> List<T> sql(String query, Class<T> clazz) {

		if (usePostegre){
			return useCache ? HibernateCache.getInstance().sql(query, clazz) : Hibernate.getInstance().sql(query, clazz);
		} else {
			return useCache ? null : NoSQLCosmos.getInstance().query(clazz, query).value();
		}
	}
	
	public static <T> List<T> sql(Class<T> clazz, String fmt, Object ... args) {

		if (usePostegre){
			return useCache ? HibernateCache.getInstance().sql(String.format(fmt, args), clazz)
					: Hibernate.getInstance().sql(String.format(fmt, args), clazz);
		} else {
			return null;
		}

	}
	
	public static <T> Result<T> getOne(String id, Class<T> clazz) {

		if (usePostegre){
			return useCache ? HibernateCache.getInstance().getOne(id, clazz)
					: Hibernate.getInstance().getOne(id, clazz);
		} else {
			return useCache ? null : NoSQLCosmos.getInstance().getOne(id, clazz);
		}
	}
	
	public static <T> Result<T> deleteOne(T obj) {

		if (usePostegre){
			return useCache ? HibernateCache.getInstance().deleteOne(obj)
					: Hibernate.getInstance().deleteOne(obj);
		} else {
			return useCache ? null : NoSQLCosmos.getInstance().deleteOne(obj);
		}

	}
	
	public static <T> Result<T> updateOne(T obj) {

		if (usePostegre){
			return useCache ? HibernateCache.getInstance().updateOne(obj)
					: Hibernate.getInstance().updateOne(obj);
		} else {
			return useCache ? null : NoSQLCosmos.getInstance().updateOne(obj);
		}

	}
	
	public static <T> Result<T> insertOne( T obj) {

		if (usePostegre){
			return useCache ? Result.errorOrValue(HibernateCache.getInstance().persistOne(obj), obj)
					: Result.errorOrValue(Hibernate.getInstance().persistOne(obj), obj);
		} else {
			return useCache ? null : NoSQLCosmos.getInstance().insertOne(obj);
		}

	}

	// use this only for Postegre/Hibernate
	public static <T> Result<T> transaction( Consumer<Session> c) {
			return useCache ? HibernateCache.getInstance().execute( c::accept )
					: Hibernate.getInstance().execute( c::accept );
	}

	// use this only for CosmosDBConnection/NoSQL
	public static <T> Result<T> transactionNoSQL( Consumer<CosmosContainer> c) {
		return useCache ? null
				: NoSQLCosmos.getInstance().executeDB( c::accept );
	}

	public static <T> Result<T> transaction( Function<Session, Result<T>> func) {
		return useCache ? HibernateCache.getInstance().execute( func ) : Hibernate.getInstance().execute( func );
	}
}
