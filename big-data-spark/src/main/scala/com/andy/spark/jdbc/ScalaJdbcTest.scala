package com.andy.spark.jdbc

import scalikejdbc.config.DBs
import scalikejdbc.{DB, SQL}

/**
  * <p>
  *
  * @author leone
  * @since 2019-03-09
  **/
object ScalaJdbcTest {

  def main(args: Array[String]): Unit = {

    // 加载配置
    DBs.setup()
    // 更新使用的是autoCommit

    // 插入使用的是localTx
    //    DB.localTx(implicit session => {
    //      SQL("insert into t_user(account,create_time,deleted,password,age,description) values (?,?,?,?,?,?)")
    //        .bind("james", "2010-10-10 12:00:00", 1, "3424324234", 34, "desc").update.apply()
    //    })

    // 修改
    //    DB.autoCommit(implicit session => {
    //      // SQL里面是普通的sql语句，后面bind里面是语句中"?"的值，update().apply()是执行语句
    //      SQL("update t_user set account = ? where user_id = ?").bind("hello", "19012258").update().apply()
    //    })

    // 删除 autoCommit
    //    DB.autoCommit(implicit session => {
    //      SQL("delete from t_user where user_id = ?").bind(20012253).update().apply()
    //    })

    // 读取使用的是readOnly
    val result: List[User] = DB.readOnly(implicit session => {
      SQL("select * from t_user limit 10").map(rs => {
        User(rs.long("user_id"), rs.string("account"), rs.date("create_time"), rs.boolean("deleted"), rs.string("password"), rs.int("age"), rs.string("description"))
      }).list().apply()
    })

    for (user <- result) {
      println(user)
    }

  }

  case class User(userId: Long, account: String, createTime: java.util.Date, deleted: Boolean, password: String, age: Int, description: String) {}

}

