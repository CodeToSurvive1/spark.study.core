package io.github.codetosurvive.spark.dao.impl;

import io.github.codetosurvive.spark.dao.ITaskDAO;

/**
 * DAO工厂类
 * @author lixiaojiao
 *
 */
public class DAOFactory {

	/**
	 * 获取任务管理task dao
	 * @return
	 */
	public static ITaskDAO getTaskDAO() {
		return new TaskDAOImpl();
	}
}
