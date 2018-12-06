package krews.db

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.sqlite.SQLiteDataSource
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.sql.Connection

fun migrateAndConnectDb(dbFile: Path): Database {
    Files.createDirectories(dbFile.parent)
    val hikariConfig = HikariConfig()
    hikariConfig.maximumPoolSize = 1
    hikariConfig.jdbcUrl = "jdbc:sqlite:$dbFile"
    val hikariDataSource = HikariDataSource(hikariConfig)
    Flyway.configure().dataSource(hikariDataSource).load().migrate()
    val db = Database.connect(hikariDataSource)
    TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
    return db
}