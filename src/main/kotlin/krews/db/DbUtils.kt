package krews.db

import org.flywaydb.core.Flyway
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.sqlite.SQLiteDataSource
import java.sql.Connection

fun migrateAndConnectDb(dbFile: String): Database {
    val dataSource = SQLiteDataSource()
    val dbUrl = "jdbc:sqlite:$dbFile"
    dataSource.url = dbUrl
    Flyway.configure().dataSource(dataSource).load().migrate()
    val db = Database.connect(dbUrl, "org.sqlite.JDBC")
    TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
    return db
}