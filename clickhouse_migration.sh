#!/bin/bash

# Configuration
OLD_CLUSTER_HOST="old-clickhouse-server"
OLD_CLUSTER_SECURE_PORT="9440"
NEW_CLUSTER_HOST="new-clickhouse-server"
NEW_CLUSTER_SECURE_PORT="9440"
CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD=""
BACKUP_DIR="/var/lib/clickhouse/backup"
LOG_FILE="/var/log/clickhouse-migration.log"
ENABLE_TLS=true
VERIFY_TLS_CERT=true
CLUSTER_NAME="main"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Get timestamp with microseconds
get_timestamp() {
    date '+%Y-%m-%d %H:%M:%S.%6N'
}

# Logging functions
log() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${timestamp} - $1" | tee -a "$LOG_FILE"
}

success() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${GREEN}${timestamp} - $1${NC}" | tee -a "$LOG_FILE"
}

error() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${RED}${timestamp} - ERROR: $1${NC}" | tee -a "$LOG_FILE"
    exit 1
}

warning() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${YELLOW}${timestamp} - WARNING: $1${NC}" | tee -a "$LOG_FILE"
}

# Function to execute queries in ClickHouse
clickhouse_query() {
    local host=$1
    local port=$2
    local query=$3
    local secure_flag=""
    
    if [ "$ENABLE_TLS" = true ]; then
        secure_flag="--secure"
        if [ "$VERIFY_TLS_CERT" = false ]; then
            secure_flag="$secure_flag --insecure"
        fi
    fi
    
    clickhouse-client \
        --host="$host" \
        --port="$port" \
        $secure_flag \
        --user="$CLICKHOUSE_USER" \
        --password="$CLICKHOUSE_PASSWORD" \
        --multiquery \
        -q "$query" 2>> "$LOG_FILE"
}

clickhouse_query_old() {
    local query=$1
    clickhouse_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_SECURE_PORT" "$query"
}

clickhouse_query_new() {
    local query=$1
    clickhouse_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_SECURE_PORT" "$query"
}

# Check connections
check_connections() {
    log "Checking connection to old cluster..."
    if clickhouse_query_old "SELECT 1"; then
        success "Connection to old cluster established"
    else
        error "Failed to connect to old cluster"
    fi

    log "Checking connection to new cluster..."
    if clickhouse_query_new "SELECT 1"; then
        success "Connection to new cluster established"
    else
        error "Failed to connect to new cluster"
    fi
}

# Create backup directory
create_backup_dir() {
    log "Creating backup directory..."
    mkdir -p "$BACKUP_DIR/ddl" "$BACKUP_DIR/data" "$BACKUP_DIR/users"
    chown -R clickhouse:clickhouse "$BACKUP_DIR" 2>/dev/null || true
}

# 1. Export DDL
export_ddl() {
    log "=== Step 1: Exporting DDL ==="
    
    # Export list of databases (excluding system databases)
    local databases
    databases=$(clickhouse_query_old \
        "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', '_temporary_and_external_tables')")
    
    if [ -z "$databases" ]; then
        warning "No non-system databases found"
        return 0
    fi
    
    for db in $databases; do
        log "Exporting database: $db"
        
        # Create directory for database
        mkdir -p "$BACKUP_DIR/ddl/$db"
        
        # Create DDL for database
        clickhouse_query_old \
            "SHOW CREATE DATABASE $db" > "$BACKUP_DIR/ddl/$db/database.sql"
        
        # Export tables
        export_tables "$db"
        
        # Export views
        export_views "$db"
        
        # Export dictionaries
        export_dictionaries "$db"
    done
    
    success "DDL export completed"
}

export_tables() {
    local db=$1
    local tables
    tables=$(clickhouse_query_old \
        "SELECT name FROM system.tables WHERE database = '$db' AND engine NOT LIKE '%View%'")
    
    for table in $tables; do
        log "  Exporting table: $table"
        clickhouse_query_old \
            "SHOW CREATE TABLE $db.$table" > "$BACKUP_DIR/ddl/$db/$table.sql"
    done
}

export_views() {
    local db=$1
    local views
    views=$(clickhouse_query_old \
        "SELECT name FROM system.tables WHERE database = '$db' AND engine LIKE '%View%'")
    
    for view in $views; do
        log "  Exporting view: $view"
        clickhouse_query_old \
            "SHOW CREATE TABLE $db.$view" > "$BACKUP_DIR/ddl/$db/$view.view.sql"
    done
}

export_dictionaries() {
    local db=$1
    local dicts
    dicts=$(clickhouse_query_old \
        "SELECT name FROM system.dictionaries WHERE database = '$db'")
    
    for dict in $dicts; do
        log "  Exporting dictionary: $dict"
        clickhouse_query_old \
            "SHOW CREATE DICTIONARY $db.$dict" > "$BACKUP_DIR/ddl/$db/$dict.dict.sql"
    done
}

# Export users and permissions
export_users() {
    log "Exporting users and permissions..."
    
    # Export users
    clickhouse_query_old \
        "SHOW USERS" | while read -r user; do
            clickhouse_query_old \
                "SHOW CREATE USER $user" > "$BACKUP_DIR/users/$user.user.sql" 2>/dev/null || true
        done
    
    # Export roles
    clickhouse_query_old \
        "SHOW ROLES" | while read -r role; do
            clickhouse_query_old \
                "SHOW CREATE ROLE $role" > "$BACKUP_DIR/users/$role.role.sql" 2>/dev/null || true
        done
    
    # Export quotas
    clickhouse_query_old \
        "SHOW QUOTAS" | while read -r quota; do
            clickhouse_query_old \
                "SHOW CREATE QUOTA $quota" > "$BACKUP_DIR/users/$quota.quota.sql" 2>/dev/null || true
        done
    
    # Export settings profiles
    clickhouse_query_old \
        "SHOW SETTINGS PROFILES" | while read -r profile; do
            clickhouse_query_old \
                "SHOW CREATE SETTINGS PROFILE $profile" > "$BACKUP_DIR/users/$profile.profile.sql" 2>/dev/null || true
        done
    
    success "User export completed"
}

# Function to modify DDL for cluster and engine requirements
modify_ddl_for_cluster() {
    local file_path=$1
    local entity_type=$2
    local db_name=$3
    local entity_name=$4
    
    if [ ! -f "$file_path" ]; then
        warning "File $file_path does not exist"
        return 1
    fi
    
    log "  Modifying DDL for $entity_type: $db_name.$entity_name"
    
    # Read the file content
    local content
    content=$(cat "$file_path")
    
    case $entity_type in
        "database")
            # Add ON CLUSTER clause to CREATE DATABASE
            if ! echo "$content" | grep -q "ON CLUSTER"; then
                content=$(echo "$content" | sed "s/CREATE DATABASE \(.*\)/CREATE DATABASE \1 ON CLUSTER $CLUSTER_NAME/")
            fi
            ;;
            
        "table")
            # First get original engine from file
            local original_engine
            original_engine=$(echo "$content" | grep -o "ENGINE = [^()]*" | head -1 | sed 's/ENGINE = //')
            
            # Add ON CLUSTER clause if not present
            if ! echo "$content" | grep -q "ON CLUSTER"; then
                content=$(echo "$content" | sed "s/CREATE TABLE \(.*\)/CREATE TABLE \1 ON CLUSTER $CLUSTER_NAME/")
            fi
            
            # Handle engine modifications based on original engine
            if echo "$original_engine" | grep -qi "MergeTree" && ! echo "$original_engine" | grep -qi "Replicated"; then
                # Original was MergeTree, change to ReplicatedMergeTree with shard
                log "    Changing MergeTree to ReplicatedMergeTree with sharding"
                content=$(echo "$content" | sed "s/ENGINE = MergeTree/ENGINE = ReplicatedMergeTree('\/clickhouse\/$db_name\/$entity_name\/{shard}', '{replica}')/")
            elif echo "$original_engine" | grep -qi "ReplicatedMergeTree"; then
                # Original was ReplicatedMergeTree, change to new format
                log "    Modifying ReplicatedMergeTree path"
                # Extract existing path to preserve other parameters
                content=$(echo "$content" | sed "s|ENGINE = ReplicatedMergeTree('[^']*', '[^']*')|ENGINE = ReplicatedMergeTree('/clickhouse/$db_name/$entity_name', '{replicated}')|")
            fi
            ;;
            
        "view")
            # Add ON CLUSTER clause to CREATE VIEW
            if ! echo "$content" | grep -q "ON CLUSTER"; then
                content=$(echo "$content" | sed "s/CREATE VIEW \(.*\)/CREATE VIEW \1 ON CLUSTER $CLUSTER_NAME/")
            fi
            ;;
            
        "dictionary")
            # Add ON CLUSTER clause to CREATE DICTIONARY
            if ! echo "$content" | grep -q "ON CLUSTER"; then
                content=$(echo "$content" | sed "s/CREATE DICTIONARY \(.*\)/CREATE DICTIONARY \1 ON CLUSTER $CLUSTER_NAME/")
            fi
            ;;
    esac
    
    # Write modified content back to file
    echo "$content" > "$file_path"
}

# 2. Apply DDL to new cluster
apply_ddl() {
    log "=== Step 2: Applying DDL to new cluster ==="
    
    # Apply database DDL with modifications
    for db_sql in "$BACKUP_DIR/ddl"/*/database.sql; do
        if [ -f "$db_sql" ]; then
            local db_name
            db_name=$(basename "$(dirname "$db_sql")")
            
            # Modify DDL for cluster
            modify_ddl_for_cluster "$db_sql" "database" "$db_name" ""
            
            log "Creating database: $db_name"
            clickhouse_query_new < "$db_sql"
        fi
    done
    
    # Apply tables DDL with modifications
    apply_tables
    
    # Apply views with modifications
    apply_views
    
    # Apply dictionaries with modifications
    apply_dictionaries
    
    success "DDL application completed"
}

apply_tables() {
    # First create all regular tables
    for table_sql in "$BACKUP_DIR/ddl"/*/*.sql; do
        if [ -f "$table_sql" ] && [[ ! $table_sql =~ \.(view|dict)\.sql$ ]]; then
            local db_name
            local table_name
            db_name=$(basename "$(dirname "$table_sql")")
            table_name=$(basename "$table_sql" .sql)
            
            # Modify DDL for cluster and engine requirements
            modify_ddl_for_cluster "$table_sql" "table" "$db_name" "$table_name"
            
            log "Creating table: $db_name.$table_name"
            clickhouse_query_new < "$table_sql"
        fi
    done
}

apply_views() {
    for view_sql in "$BACKUP_DIR/ddl"/*/*.view.sql; do
        if [ -f "$view_sql" ]; then
            local db_name
            local view_name
            db_name=$(basename "$(dirname "$view_sql")")
            view_name=$(basename "$view_sql" .view.sql)
            
            # Modify DDL for cluster
            modify_ddl_for_cluster "$view_sql" "view" "$db_name" "$view_name"
            
            log "Creating view: $db_name.$view_name"
            clickhouse_query_new < "$view_sql"
        fi
    done
}

apply_dictionaries() {
    for dict_sql in "$BACKUP_DIR/ddl"/*/*.dict.sql; do
        if [ -f "$dict_sql" ]; then
            local db_name
            local dict_name
            db_name=$(basename "$(dirname "$dict_sql")")
            dict_name=$(basename "$dict_sql" .dict.sql)
            
            # Modify DDL for cluster
            modify_ddl_for_cluster "$dict_sql" "dictionary" "$db_name" "$dict_name"
            
            log "Creating dictionary: $db_name.$dict_name"
            clickhouse_query_new < "$dict_sql"
        fi
    done
}

# 3. Data migration with sharding
migrate_data() {
    log "=== Step 3: Data migration with sharding ==="
    
    # Get list of all non-system tables
    local databases
    databases=$(clickhouse_query_old \
        "SELECT name FROM system.databases WHERE name NOT IN ('system', 'information_schema', '_temporary_and_external_tables')")
    
    if [ -z "$databases" ]; then
        warning "No databases to migrate"
        return 0
    fi
    
    for db in $databases; do
        log "Migrating data from database: $db"
        
        local tables
        tables=$(clickhouse_query_old \
            "SELECT name FROM system.tables WHERE database = '$db' AND engine NOT LIKE '%View%'")
        
        for table in $tables; do
            migrate_table "$db" "$table"
        done
    done
    
    success "Data migration completed"
}

migrate_table() {
    local db=$1
    local table=$2
    
    log "  Migrating table: $db.$table"
    
    # Get table information
    local engine
    engine=$(clickhouse_query_old \
        "SELECT engine FROM system.tables WHERE database = '$db' AND name = '$table'")
    
    case $engine in
        *Distributed*)
            migrate_distributed_table "$db" "$table"
            ;;
        *MergeTree*)
            migrate_mergetree_table "$db" "$table"
            ;;
        *Replicated*)
            migrate_replicated_table "$db" "$table"
            ;;
        *)
            migrate_simple_table "$db" "$table"
            ;;
    esac
}

migrate_mergetree_table() {
    local db=$1
    local table=$2
    
    # For MergeTree tables, migrate by partitions
    local partitions
    partitions=$(clickhouse_query_old \
        "SELECT DISTINCT partition FROM system.parts WHERE database = '$db' AND table = '$table' AND active")
    
    if [ -z "$partitions" ]; then
        # If no partitions, migrate entire table
        log "    Migrating entire table..."
        clickhouse_query_new \
            "INSERT INTO $db.$table SELECT * FROM remoteSecure('$OLD_CLUSTER_HOST:$OLD_CLUSTER_SECURE_PORT', $db, $table, '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD')"
    else
        # Migrate by partitions
        for partition in $partitions; do
            log "    Migrating partition: $partition"
            clickhouse_query_new \
                "INSERT INTO $db.$table SELECT * FROM remoteSecure('$OLD_CLUSTER_HOST:$OLD_CLUSTER_SECURE_PORT', $db, $table, '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD') WHERE partition = '$partition'"
        done
    fi
}

migrate_distributed_table() {
    local db=$1
    local table=$2
    
    # For distributed tables, get underlying tables
    local underlying_tables
    underlying_tables=$(clickhouse_query_old \
        "SELECT underlying_table FROM system.distributed_tables WHERE database = '$db' AND name = '$table'")
    
    if [ -z "$underlying_tables" ]; then
        # If information not found, migrate as regular table
        migrate_simple_table "$db" "$table"
    else
        # Migrate data from underlying tables
        for underlying_table in $underlying_tables; do
            log "    Migrating from underlying table: $underlying_table"
            clickhouse_query_new \
                "INSERT INTO $db.$table SELECT * FROM remoteSecure('$OLD_CLUSTER_HOST:$OLD_CLUSTER_SECURE_PORT', $db, $underlying_table, '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD')"
        done
    fi
}

migrate_replicated_table() {
    local db=$1
    local table=$2
    
    log "    Migrating replicated table..."
    # For replicated tables, use same approach as MergeTree
    migrate_mergetree_table "$db" "$table"
}

migrate_simple_table() {
    local db=$1
    local table=$2
    
    log "    Migrating simple table..."
    clickhouse_query_new \
        "INSERT INTO $db.$table SELECT * FROM remoteSecure('$OLD_CLUSTER_HOST:$OLD_CLUSTER_SECURE_PORT', $db, $table, '$CLICKHOUSE_USER', '$CLICKHOUSE_PASSWORD')"
}

# 4. Apply users and permissions
apply_users() {
    log "=== Step 4: Applying users and permissions ==="
    
    for user_file in "$BACKUP_DIR/users"/*.user.sql; do
        if [ -f "$user_file" ]; then
            local user_name
            user_name=$(basename "$user_file" .user.sql)
            log "Creating user: $user_name"
            clickhouse_query_new < "$user_file"
        fi
    done
    
    for role_file in "$BACKUP_DIR/users"/*.role.sql; do
        if [ -f "$role_file" ]; then
            local role_name
            role_name=$(basename "$role_file" .role.sql)
            log "Creating role: $role_name"
            clickhouse_query_new < "$role_file"
        fi
    done
    
    success "Users and permissions applied"
}

# 5. Verify migration integrity
verify_migration() {
    log "=== Step 5: Verifying migration integrity ==="
    
    local old_count
    local new_count
    
    # Verify table counts
    old_count=$(clickhouse_query_old \
        "SELECT count() FROM system.tables WHERE database NOT IN ('system', 'information_schema')")
    new_count=$(clickhouse_query_new \
        "SELECT count() FROM system.tables WHERE database NOT IN ('system', 'information_schema')")
    
    log "Tables in old cluster: $old_count"
    log "Tables in new cluster: $new_count"
    
    if [ "$old_count" -eq "$new_count" ]; then
        success "Table counts match"
    else
        warning "Table counts don't match!"
    fi
    
    # Sample data verification
    log "Performing sample data verification..."
    
    # Check a few random tables
    local sample_tables
    sample_tables=$(clickhouse_query_old \
        "SELECT concat(database, '.', name) FROM system.tables WHERE database NOT IN ('system', 'information_schema') AND engine NOT LIKE '%View%' LIMIT 3")
    
    if [ -z "$sample_tables" ]; then
        warning "No tables found for verification"
        return 0
    fi
    
    for table in $sample_tables; do
        log "  Verifying table: $table"
        old_count=$(clickhouse_query_old "SELECT count() FROM $table")
        new_count=$(clickhouse_query_new "SELECT count() FROM $table")
        
        if [ "$old_count" -eq "$new_count" ]; then
            success "    Data in $table matches: $old_count rows"
        else
            warning "    Data in $table doesn't match! Old: $old_count, New: $new_count"
        fi
    done
    
    # Verify engine transformations
    verify_engine_transformations
    
    success "Verification completed"
}

verify_engine_transformations() {
    log "  Verifying engine transformations..."
    
    # Get all tables from old cluster
    local old_tables
    old_tables=$(clickhouse_query_old \
        "SELECT database, name, engine FROM system.tables WHERE database NOT IN ('system', 'information_schema') AND engine NOT LIKE '%View%'")
    
    while read -r db table old_engine; do
        # Get new engine
        local new_engine
        new_engine=$(clickhouse_query_new \
            "SELECT engine FROM system.tables WHERE database = '$db' AND name = '$table'")
        
        if [ -n "$new_engine" ]; then
            log "    Table $db.$table: Old=$old_engine, New=$new_engine"
            
            # Verify transformation rules
            if echo "$old_engine" | grep -qi "MergeTree" && ! echo "$old_engine" | grep -qi "Replicated"; then
                # Should be transformed to ReplicatedMergeTree with shard
                if echo "$new_engine" | grep -qi "ReplicatedMergeTree" && echo "$new_engine" | grep -q "{shard}"; then
                    success "      ✓ Correctly transformed to sharded ReplicatedMergeTree"
                else
                    warning "      ✗ Incorrect transformation: expected sharded ReplicatedMergeTree"
                fi
            elif echo "$old_engine" | grep -qi "ReplicatedMergeTree"; then
                # Should be transformed to ReplicatedMergeTree without shard
                if echo "$new_engine" | grep -qi "ReplicatedMergeTree" && ! echo "$new_engine" | grep -q "{shard}"; then
                    success "      ✓ Correctly transformed to non-sharded ReplicatedMergeTree"
                else
                    warning "      ✗ Incorrect transformation: expected non-sharded ReplicatedMergeTree"
                fi
            fi
        else
            warning "    Table $db.$table not found in new cluster"
        fi
    done <<< "$old_tables"
}

# Main function
main() {
    log "Starting ClickHouse cluster migration"
    log "Old cluster: $OLD_CLUSTER_HOST:$OLD_CLUSTER_SECURE_PORT"
    log "New cluster: $NEW_CLUSTER_HOST:$NEW_CLUSTER_SECURE_PORT"
    log "Target cluster name: $CLUSTER_NAME"
    log "TLS Enabled: $ENABLE_TLS"
    log "Verify TLS Certificate: $VERIFY_TLS_CERT"
    
    # Check dependencies
    if ! command -v clickhouse-client &> /dev/null; then
        error "clickhouse-client not found. Please install ClickHouse client."
    fi
    
    check_connections
    create_backup_dir
    
    # Execute migration steps
    export_ddl
    export_users
    apply_ddl
    migrate_data
    apply_users
    verify_migration
    
    success "Migration successfully completed!"
    log "Logs saved to: $LOG_FILE"
    log "DDL backups saved to: $BACKUP_DIR"
}

# Command line arguments handling
while [[ $# -gt 0 ]]; do
    case $1 in
        --old-host)
            OLD_CLUSTER_HOST="$2"
            shift 2
            ;;
        --new-host)
            NEW_CLUSTER_HOST="$2"
            shift 2
            ;;
        --old-port)
            OLD_CLUSTER_SECURE_PORT="$2"
            shift 2
            ;;
        --new-port)
            NEW_CLUSTER_SECURE_PORT="$2"
            shift 2
            ;;
        --user)
            CLICKHOUSE_USER="$2"
            shift 2
            ;;
        --password)
            CLICKHOUSE_PASSWORD="$2"
            shift 2
            ;;
        --backup-dir)
            BACKUP_DIR="$2"
            shift 2
            ;;
        --cluster-name)
            CLUSTER_NAME="$2"
            shift 2
            ;;
        --disable-tls)
            ENABLE_TLS=false
            shift
            ;;
        --insecure)
            VERIFY_TLS_CERT=false
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo "Options:"
            echo "  --old-host HOST          Old cluster host (default: $OLD_CLUSTER_HOST)"
            echo "  --new-host HOST          New cluster host (default: $NEW_CLUSTER_HOST)"
            echo "  --old-port PORT          Old cluster secure port (default: $OLD_CLUSTER_SECURE_PORT)"
            echo "  --new-port PORT          New cluster secure port (default: $NEW_CLUSTER_SECURE_PORT)"
            echo "  --user USER              ClickHouse user (default: $CLICKHOUSE_USER)"
            echo "  --password PASSWORD      ClickHouse password"
            echo "  --backup-dir DIR         Backup directory (default: $BACKUP_DIR)"
            echo "  --cluster-name NAME      Target cluster name (default: $CLUSTER_NAME)"
            echo "  --disable-tls            Disable TLS (use unencrypted connections)"
            echo "  --insecure               Don't verify TLS certificates"
            echo "  --help                   Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown parameter: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Run main function
main
