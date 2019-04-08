package krews.db

import com.zaxxer.hikari.*
import org.flywaydb.core.Flyway
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.transactions.TransactionManager
import java.nio.file.*
import java.sql.Connection

fun setupCacheDb(dbFile: Path) = migrateAndConnectDb(dbFile, "classpath:db/migration/cache")
fun setupRunDb(dbFile: Path) = migrateAndConnectDb(dbFile, "classpath:db/migration/run")

fun migrateAndConnectDb(dbFile: Path, migrationLocation: String): Database {
    Files.createDirectories(dbFile.parent)
    val hikariConfig = HikariConfig()
    hikariConfig.maximumPoolSize = 1
    hikariConfig.jdbcUrl = "jdbc:sqlite:$dbFile"
    val hikariDataSource = HikariDataSource(hikariConfig)
    Flyway.configure()
        .dataSource(hikariDataSource)
        .locations(migrationLocation)
        .load()
        .migrate()
    return Database.connect(hikariDataSource)
}

fun postConnectionSetup() {
    TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
}