#!/bin/bash

# Configuration
OLD_CLUSTER_HOST="localhost"
OLD_CLUSTER_PORT="19000"
OLD_CLICKHOUSE_USER="default"
OLD_CLICKHOUSE_PASSWORD="changeme"

NEW_CLUSTER_HOST="localhost"
NEW_CLUSTER_PORT="29000"
NEW_CLICKHOUSE_USER="default"
NEW_CLICKHOUSE_PASSWORD="changeme"

CLUSTER_NAME="epm_cluster"
BACKUP_DIR="/Users/alexey_zheleznoy/Desktop/jobs/clickhouse/my_ch/backup"
LOG_FILE="/Users/alexey_zheleznoy/Desktop/jobs/clickhouse/my_ch/clickhouse-migration.log"

# Exclude system databases
EXCLUDED_DATABASES="'system', 'information_schema', 'INFORMATION_SCHEMA', 'default'"

# Exclude system tables/views
EXCLUDED_TABLE_ENGINES="'dictionary', '%postgres%', '%view%'"

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
    echo -e "$timestamp - $1" | tee -a "$LOG_FILE"
}

success() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${GREEN}$timestamp - $1${NC}"
    echo "$timestamp - $1" >> "$LOG_FILE"
}

error() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${RED}$timestamp - ERROR: $1${NC}" >&2
    echo "$timestamp - ERROR: $1" >> "$LOG_FILE"
    exit 1
}

warning() {
    local timestamp
    timestamp=$(get_timestamp)
    echo -e "${YELLOW}$timestamp - WARNING: $1${NC}"
    echo "$timestamp - WARNING: $1" >> "$LOG_FILE"
}

# Function to execute queries in ClickHouse with proper error handling
clickhouse_query() {
    local host=$1
    local port=$2
    local user=$3
    local password=$4
    local query=$5
    
    # Execute query and capture both output and errors
    local output
    output=$(clickhouse client \
        --host="$host" \
        --port="$port" \
        --user="$user" \
        --password="$password" \
        --multiquery \
        -q "$query" 2>&1)
    
    local exit_code=$?
    
    # Always log the full output
    if [ -n "$output" ]; then
        # Log to file (without color codes for cleaner logs)
        echo "$output" | sed -r "s/\x1B\[[0-9;]*[JKmsu]//g" >> "$LOG_FILE"
    fi
    
    # If there was an error, extract meaningful error message
    if [ $exit_code -ne 0 ] && [ -n "$output" ]; then
        # Extract ClickHouse error message (usually starts with "Code: ")
        local error_msg
        error_msg=$(echo "$output" | grep -o "Code: [0-9]\+.*" | head -1)
        
        if [ -z "$error_msg" ]; then
            # If no standard error format, take the first non-empty line
            error_msg=$(echo "$output" | grep -v "^$" | head -1)
        fi
        
        # Output the error to stderr
        echo "$error_msg" >&2
    fi
    
    # Return output for successful queries
    if [ $exit_code -eq 0 ]; then
        echo "$output"
    fi
    
    return $exit_code
}

clickhouse_query_old() {
    local query=$1
    clickhouse_query "$OLD_CLUSTER_HOST" "$OLD_CLUSTER_PORT" "$OLD_CLICKHOUSE_USER" "$OLD_CLICKHOUSE_PASSWORD" "$query"
}

clickhouse_query_new() {
    local query=$1
    clickhouse_query "$NEW_CLUSTER_HOST" "$NEW_CLUSTER_PORT" "$NEW_CLICKHOUSE_USER" "$NEW_CLICKHOUSE_PASSWORD" "$query"
}

# Check connections
check_connections() {
    log "Checking connection to old cluster..."
    if clickhouse_query_old "SELECT hostname()" >/dev/null 2>&1; then
        success "Connection to old cluster established"
    else
        error "Failed to connect to old cluster"
    fi

    log "Checking connection to new cluster..."
    if clickhouse_query_new "SELECT hostname()" >/dev/null 2>&1; then
        success "Connection to new cluster established"
    else
        error "Failed to connect to new cluster"
    fi
}

# Create backup directory
create_backup_dir() {
    log "Creating backup directory..."
    mkdir -p "$BACKUP_DIR/ddl" "$BACKUP_DIR/data"
    chown -R clickhouse:clickhouse "$BACKUP_DIR" 2>/dev/null || true
}

# Function to fix escaped characters in DDL files
fix_escaped_chars_in_ddl() {
    local file_path=$1
    
    if [ ! -f "$file_path" ]; then
        return 1
    fi
    
    # Read the file content
    local content
    content=$(cat "$file_path")
    
    # Replace escaped quotes with regular quotes
    content=$(echo "$content" | sed "s/\\\\'/'/g")
    
    # Replace escaped backslashes with single backslashes
    content=$(echo "$content" | sed 's/\\\\/\\/g')
    
    # Write fixed content back
    echo "$content" > "$file_path"
}

export_tables() {
    local db=$1
    local tables

    tables=$(clickhouse_query_old \
        "SELECT name FROM system.tables WHERE database = '$db' AND engine NOT ILIKE '%view%' \
            AND engine NOT ILIKE 'dictionary' AND engine NOT ILIKE '%postgres%'")
    
    for table in $tables; do
        log "  Exporting table: $table"
        clickhouse_query_old \
            "SHOW CREATE TABLE $db.\`$table\`" > "$BACKUP_DIR/ddl/$db/$table.sql"
        
        # Fix escaped characters in DDL
        fix_escaped_chars_in_ddl "$BACKUP_DIR/ddl/$db/$table.sql"
    done
}

export_views() {
    local db=$1
    local views

    views=$(clickhouse_query_old \
        "SELECT name FROM system.tables WHERE database = '$db' AND engine ILIKE '%view%'")
    
    for view in $views; do
        log "  Exporting view: $view"
        clickhouse_query_old \
            "SHOW CREATE TABLE $db.\`$view\`" > "$BACKUP_DIR/ddl/$db/$view.view.sql"
        
        # Fix escaped characters in DDL
        fix_escaped_chars_in_ddl "$BACKUP_DIR/ddl/$db/$view.view.sql"
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
        
        # Fix escaped characters in DDL
        fix_escaped_chars_in_ddl "$BACKUP_DIR/ddl/$db/$dict.dict.sql"
    done
}

# 1. Export DDL
export_ddl() {
    log "=== Step 1: Exporting DDL ==="
    
    # Export list of databases (excluding system databases)
    local databases
    databases=$(clickhouse_query_old \
        "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
    
    if [ -z "$databases" ]; then
        warning "No non-system databases found"
        return 0
    fi
    
    for db in $databases; do
        log "Exporting database: $db"
        
        mkdir -p "$BACKUP_DIR/ddl/$db"
        clickhouse_query_old \
            "SHOW CREATE DATABASE $db" > "$BACKUP_DIR/ddl/$db/database.sql"
        
        # Fix escaped characters in database DDL
        fix_escaped_chars_in_ddl "$BACKUP_DIR/ddl/$db/database.sql"
        
        export_tables "$db"
        export_views "$db"
        export_dictionaries "$db"
    done
    
    success "DDL export completed"
}

# Function to add ON CLUSTER clause to DDL
add_on_cluster_to_ddl() {
    local file_path=$1
    local entity_type=$2
    local db_name=$3
    local entity_name=$4
    
    if [ ! -f "$file_path" ]; then
        warning "File $file_path does not exist"
        return 1
    fi
    
    log "  Adding ON CLUSTER to $entity_type: $db_name.$entity_name"
    
    # Read the file content
    local content
    content=$(cat "$file_path")
    
    # Remove escape sequences like \n, \t
    content=$(echo -e "$content")
    
    # Check if ON CLUSTER already exists
    if echo "$content" | grep -qi "ON CLUSTER"; then
        log "    ON CLUSTER already present"
        return 0
    fi
    
    # Split into lines
    local lines=()
    while IFS= read -r line; do
        lines+=("$line")
    done <<< "$content"
    
    # Process CREATE statement line
    if [[ ${lines[0]} =~ ^CREATE[[:space:]]+(DATABASE|TABLE|VIEW|DICTIONARY) ]]; then
        # Add ON CLUSTER after the entity name in the first line
        if [[ ${lines[0]} =~ ^(CREATE[[:space:]]+[A-Z]+[[:space:]]+[^[:space:]]+)(.*) ]]; then
            local create_part="${BASH_REMATCH[1]}"
            local rest_part="${BASH_REMATCH[2]}"
            
            # Add ON CLUSTER after the entity name
            lines[0]="${create_part} ON CLUSTER $CLUSTER_NAME${rest_part}"
            
            # Write back to file
            printf "%s\n" "${lines[@]}" > "$file_path"
            log "    Successfully added ON CLUSTER $CLUSTER_NAME"
        else
            warning "    Failed to parse CREATE statement: ${lines[0]}"
            return 1
        fi
    else
        warning "    No CREATE statement found in first line"
        return 1
    fi
}

# 2. Apply DDL to new cluster
apply_ddl() {
    log "=== Step 2: Applying DDL to new cluster ==="
    
    local has_errors=0
    
    # Apply database DDL
    for db_dir in "$BACKUP_DIR/ddl"/*/; do
        if [ -d "$db_dir" ]; then
            local db_name
            db_name=$(basename "$db_dir")
            local db_sql="$db_dir/database.sql"
            
            if [ -f "$db_sql" ]; then
                # Add ON CLUSTER clause
                if ! add_on_cluster_to_ddl "$db_sql" "database" "$db_name" ""; then
                    warning "Failed to modify DDL for database: $db_name"
                    has_errors=1
                    continue
                fi
                
                log "Creating database: $db_name"
                
                # Read the SQL from file
                local sql_statement
                sql_statement=$(cat "$db_sql")
                
                # Execute and check result
                if ! clickhouse_query_new "$sql_statement" >/dev/null 2>&1; then
                    # Get the actual error from the last query
                    local error_msg
                    error_msg=$(clickhouse_query_new "$sql_statement" 2>&1 | grep -o "Code: [0-9]\+.*" | head -1)
                    
                    if [ -z "$error_msg" ]; then
                        error_msg="Unknown error (check $LOG_FILE for details)"
                    fi
                    
                    warning "Failed to create database '$db_name': $error_msg"
                    has_errors=1
                else
                    log "  Successfully created database: $db_name"
                fi
            else
                warning "Database DDL file not found for: $db_name"
                has_errors=1
            fi
        fi
    done
    
    if [ $has_errors -eq 0 ]; then
        # Apply tables DDL
        apply_tables
        
        # Apply views DDL
        apply_views
        
        # Apply dictionaries DDL
        apply_dictionaries
    else
        error "Stopping DDL application due to previous errors"
    fi
    
    success "DDL application completed"
}

apply_tables() {
    log "Applying tables..."
    
    local has_errors=0
    for db_dir in "$BACKUP_DIR/ddl"/*/; do
        if [ -d "$db_dir" ]; then
            local db_name
            db_name=$(basename "$db_dir")
            
            for table_sql in "$db_dir"/*.sql; do
                if [ -f "$table_sql" ]; then
                    local filename
                    filename=$(basename "$table_sql")
                    
                    # Skip non-table files
                    if [[ "$filename" == "database.sql" ]] || \
                       [[ "$filename" == *".view.sql" ]] || \
                       [[ "$filename" == *".dict.sql" ]]; then
                        continue
                    fi
                    
                    local table_name
                    table_name=$(basename "$table_sql" .sql)
                    
                    # Add ON CLUSTER clause
                    if ! add_on_cluster_to_ddl "$table_sql" "table" "$db_name" "$table_name"; then
                        warning "Failed to modify DDL for table: $db_name.$table_name"
                        has_errors=1
                        continue
                    fi
                    
                    log "  Creating table: $db_name.$table_name"
                    
                    # Read the SQL from file
                    local sql_statement
                    sql_statement=$(cat "$table_sql")
                    
                    # Execute and check result
                    if ! clickhouse_query_new "$sql_statement" >/dev/null 2>&1; then
                        local error_msg
                        error_msg=$(clickhouse_query_new "$sql_statement" 2>&1 | grep -o "Code: [0-9]\+.*" | head -1)
                        
                        if [ -z "$error_msg" ]; then
                            error_msg="Unknown error (check $LOG_FILE for details)"
                        fi
                        
                        warning "Failed to create table '$db_name.$table_name': $error_msg"
                        has_errors=1
                    else
                        log "    Successfully created table: $db_name.$table_name"
                    fi
                fi
            done
        fi
    done
    
    [ $has_errors -eq 0 ] || warning "Some tables failed to create"
}

apply_views() {
    log "Applying views..."
    
    local has_errors=0
    for db_dir in "$BACKUP_DIR/ddl"/*/; do
        if [ -d "$db_dir" ]; then
            local db_name
            db_name=$(basename "$db_dir")
            
            for view_sql in "$db_dir"/*.view.sql; do
                if [ -f "$view_sql" ]; then
                    local view_name
                    view_name=$(basename "$view_sql" .view.sql)
                    
                    # Add ON CLUSTER clause
                    if ! add_on_cluster_to_ddl "$view_sql" "view" "$db_name" "$view_name"; then
                        warning "Failed to modify DDL for view: $db_name.$view_name"
                        has_errors=1
                        continue
                    fi
                    
                    log "  Creating view: $db_name.$view_name"
                    
                    # Read the SQL from file
                    local sql_statement
                    sql_statement=$(cat "$view_sql")
                    
                    # Execute and check result
                    if ! clickhouse_query_new "$sql_statement" >/dev/null 2>&1; then
                        local error_msg
                        error_msg=$(clickhouse_query_new "$sql_statement" 2>&1 | grep -o "Code: [0-9]\+.*" | head -1)
                        
                        if [ -z "$error_msg" ]; then
                            error_msg="Unknown error (check $LOG_FILE for details)"
                        fi
                        
                        warning "Failed to create view '$db_name.$view_name': $error_msg"
                        has_errors=1
                    else
                        log "    Successfully created view: $db_name.$view_name"
                    fi
                fi
            done
        fi
    done
    
    [ $has_errors -eq 0 ] || warning "Some views failed to create"
}

apply_dictionaries() {
    log "Applying dictionaries..."
    
    local has_errors=0
    for db_dir in "$BACKUP_DIR/ddl"/*/; do
        if [ -d "$db_dir" ]; then
            local db_name
            db_name=$(basename "$db_dir")
            
            for dict_sql in "$db_dir"/*.dict.sql; do
                if [ -f "$dict_sql" ]; then
                    local dict_name
                    dict_name=$(basename "$dict_sql" .dict.sql)
                    
                    # Add ON CLUSTER clause
                    if ! add_on_cluster_to_ddl "$dict_sql" "dictionary" "$db_name" "$dict_name"; then
                        warning "Failed to modify DDL for dictionary: $db_name.$dict_name"
                        has_errors=1
                        continue
                    fi
                    
                    log "  Creating dictionary: $db_name.$dict_name"
                    
                    # Read the SQL from file
                    local sql_statement
                    sql_statement=$(cat "$dict_sql")
                    
                    # Execute and check result
                    if ! clickhouse_query_new "$sql_statement" >/dev/null 2>&1; then
                        local error_msg
                        error_msg=$(clickhouse_query_new "$sql_statement" 2>&1 | grep -o "Code: [0-9]\+.*" | head -1)
                        
                        if [ -z "$error_msg" ]; then
                            error_msg="Unknown error (check $LOG_FILE for details)"
                        fi
                        
                        warning "Failed to create dictionary '$db_name.$dict_name': $error_msg"
                        has_errors=1
                    else
                        log "    Successfully created dictionary: $db_name.$dict_name"
                    fi
                fi
            done
        fi
    done
    
    [ $has_errors -eq 0 ] || warning "Some dictionaries failed to create"
}

# 3. Data migration
migrate_data() {
    log "=== Step 3: Data migration ==="
    
    # Get list of all non-system databases
    local databases
    databases=$(clickhouse_query_old \
        "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
    
    if [ -z "$databases" ]; then
        warning "No databases to migrate"
        return 0
    fi
    
    local total_errors=0
    for db in $databases; do
        log "Migrating data from database: $db"
        
        # Get list of tables (excluding views and dictionaries)
        local tables
        tables=$(clickhouse_query_old \
            "SELECT name FROM system.tables WHERE database = '$db' AND engine NOT ILIKE '%view%' \
                AND engine NOT ILIKE 'dictionary' AND engine NOT ILIKE '%postgres%'")
        
        for table in $tables; do
            if ! migrate_table_data "$db" "$table"; then
                total_errors=$((total_errors + 1))
            fi
        done
    done
    
    if [ $total_errors -eq 0 ]; then
        success "Data migration completed"
    else
        warning "Data migration completed with $total_errors errors"
    fi
}

migrate_table_data() {
    local db=$1
    local table=$2
    
    log "  Migrating table: $db.$table"
    
    # Get row count for logging
    local row_count
    row_count=$(clickhouse_query_old "SELECT count() FROM $db.\`$table\`" | tr -d '\n')
    
    log "    Total rows: $row_count"
    
    if [ -z "$row_count" ] || [ "$row_count" == "0" ]; then
        log "    Table is empty, skipping"
        return 0
    fi
    
    # Migrate data using INSERT ... SELECT
    log "    Starting data transfer..."
    
    local start_time
    start_time=$(date +%s)
    
    local query="INSERT INTO $db.\`$table\` SELECT * FROM remote('$OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT', $db, \`$table\`, '$OLD_CLICKHOUSE_USER', '$OLD_CLICKHOUSE_PASSWORD')"
    
    if clickhouse_query_new "$query" >/dev/null 2>&1; then
        local end_time
        end_time=$(date +%s)
        local duration=$((end_time - start_time))
        
        # Verify row count
        local new_row_count
        new_row_count=$(clickhouse_query_new "SELECT count() FROM $db.\`$table\`" | tr -d '\n')
        
        if [ "$row_count" -eq "$new_row_count" ]; then
            log "    Successfully migrated $row_count rows in ${duration}s"
            return 0
        else
            warning "    Row count mismatch! Source: $row_count, Target: $new_row_count"
            return 1
        fi
    else
        local error_msg
        error_msg=$(clickhouse_query_new "$query" 2>&1 | grep -o "Code: [0-9]\+.*" | head -1)
        
        if [ -z "$error_msg" ]; then
            error_msg="Unknown error (check $LOG_FILE for details)"
        fi
        
        warning "    Failed to migrate data for table '$db.$table': $error_msg"
        return 1
    fi
}

# 4. Verify migration
verify_migration() {
    log "=== Step 4: Verifying migration ==="
    
    # Verify database counts
    local old_db_count
    local new_db_count
    old_db_count=$(clickhouse_query_old \
        "SELECT count() FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
    new_db_count=$(clickhouse_query_new \
        "SELECT count() FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
    
    log "Databases in old cluster: $old_db_count"
    log "Databases in new cluster: $new_db_count"
    
    if [ "$old_db_count" -eq "$new_db_count" ]; then
        success "Database counts match"
    else
        warning "Database counts don't match!"
    fi
    
    # Verify table counts per database
    log "Verifying table counts..."
    
    local databases
    databases=$(clickhouse_query_old \
        "SELECT name FROM system.databases WHERE name NOT IN ($EXCLUDED_DATABASES)")
    
    for db in $databases; do
        local old_table_count
        local new_table_count
        old_table_count=$(clickhouse_query_old \
            "SELECT count() FROM system.tables WHERE database = '$db' AND engine NOT ILIKE '%view%' \
                AND engine NOT ILIKE 'dictionary' AND engine NOT ILIKE '%postgres%'")
        new_table_count=$(clickhouse_query_new \
            "SELECT count() FROM system.tables WHERE database = '$db' AND engine NOT ILIKE '%view%' \
                AND engine NOT ILIKE 'dictionary' AND engine NOT ILIKE '%postgres%'")
        
        if [ "$old_table_count" -eq "$new_table_count" ]; then
            log "  Database $db: tables match ($old_table_count)"
        else
            warning "  Database $db: tables don't match! Old: $old_table_count, New: $new_table_count"
        fi
    done
    
    # Sample data verification
    log "Performing sample data verification..."
    
    # Check a few random tables
    local sample_tables
    sample_tables=$(clickhouse_query_old \
        "SELECT concat(database, '.', name) FROM system.tables WHERE database NOT IN ($EXCLUDED_DATABASES) AND engine NOT ILIKE '%view%' AND engine NOT ILIKE 'dictionary' AND engine NOT ILIKE '%postgres%' ORDER BY rand() LIMIT 3")
    
    for table in $sample_tables; do
        log "  Verifying table: $table"
        local old_count
        local new_count
        old_count=$(clickhouse_query_old "SELECT count() FROM $table" | tr -d '\n')
        new_count=$(clickhouse_query_new "SELECT count() FROM $table" | tr -d '\n')
        
        if [ "$old_count" -eq "$new_count" ]; then
            success "    Data in $table matches: $old_count rows"
        else
            warning "    Data in $table doesn't match! Old: $old_count, New: $new_count"
        fi
    done
    
    success "Verification completed"
}

# Main function
main() {
    log "Starting ClickHouse cluster migration"
    log "Old cluster: $OLD_CLUSTER_HOST:$OLD_CLUSTER_PORT"
    log "New cluster: $NEW_CLUSTER_HOST:$NEW_CLUSTER_PORT"
    log "Target cluster name: $CLUSTER_NAME"
    
    # Check dependencies
    if ! command -v clickhouse &> /dev/null; then
        error "clickhouse client not found. Please install ClickHouse client."
    fi
    
    check_connections
    create_backup_dir
    
    # Execute migration steps
    export_ddl
    apply_ddl
    migrate_data
    verify_migration
    
    success "Migration successfully completed!"
    log "Logs saved to: $LOG_FILE"
    log "DDL backups saved to: $BACKUP_DIR"
}

main