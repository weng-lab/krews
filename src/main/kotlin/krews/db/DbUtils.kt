package krews.db

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.sqlite.SQLiteDataSource
import java.sql.Connection

fun migrateAndConnectDb(dbFile: String): Database {
    val hikariConfig = HikariConfig()
    hikariConfig.maximumPoolSize = 1
    hikariConfig.jdbcUrl = "jdbc:sqlite:$dbFile"
    val hikariDataSource = HikariDataSource(hikariConfig)
    Flyway.configure().dataSource(hikariDataSource).load().migrate()
    val db = Database.connect(hikariDataSource)
    TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
    return db
}