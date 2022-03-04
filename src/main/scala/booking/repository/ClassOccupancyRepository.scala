package booking.repository

import scalikejdbc.{DBSession, scalikejdbcSQLInterpolationImplicitDef}

trait ClassOccupancyRepository {
  def update(session: ScalikeJdbcSession, classId: String): Unit

  def remove(session: ScalikeJdbcSession, classId: String): Unit

  def getItem(session: ScalikeJdbcSession, classId: String): Option[Int]
}

class ClassOccupancyRepositoryImpl() extends ClassOccupancyRepository {
  override def update(session: ScalikeJdbcSession, classId: String): Unit = {
    session.db.withinTx { implicit dbSession =>
      // This uses the PostgreSQL `ON CONFLICT` feature
      // Alternatively, this can be implemented by first issuing the `UPDATE`
      // and checking for the updated rows count. If no rows got updated issue
      // the `INSERT` instead.
      sql"""
           INSERT INTO class_occupancy (classid, count) VALUES ($classId, 1)
           ON CONFLICT (classid) DO UPDATE SET count = count + 1
         """.executeUpdate().apply()
    }
  }

  override def remove(session: ScalikeJdbcSession, classId: String): Unit = {
    session.db.withinTx { implicit dbSession =>
      // This uses the PostgreSQL `ON CONFLICT` feature
      // Alternatively, this can be implemented by first issuing the `UPDATE`
      // and checking for the updated rows count. If no rows got updated issue
      // the `INSERT` instead.
      sql"""
           INSERT INTO class_occupancy (classid, count) VALUES ($classId, 0)
           ON CONFLICT (classid) DO UPDATE SET count = count - 1
         """.executeUpdate().apply()
    }
  }

  override def getItem(session: ScalikeJdbcSession, classId: String): Option[Int] = {
    if (session.db.isTxAlreadyStarted) {
      session.db.withinTx { implicit dbSession =>
        select(classId)
      }
    } else {
      session.db.readOnly { implicit dbSession =>
        select(classId)
      }
    }
  }

  private def select(classId: String)(implicit dbSession: DBSession) = {
    sql"SELECT count FROM class_occupancy WHERE classid = $classId"
      .map(_.int("count"))
      .toOption()
      .apply()
  }
}