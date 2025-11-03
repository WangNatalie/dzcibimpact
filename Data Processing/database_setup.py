"""
Database setup utility for creating the database and user if they don't exist.
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import logging

logger = logging.getLogger(__name__)


def create_database_if_not_exists(db_config):
    """
    Create the database if it doesn't exist.
    
    Args:
        db_config (dict): Database configuration with keys:
            - host: Database host
            - port: Database port
            - database: Database name to create
            - user: Database user (must have CREATEDB privilege)
            - password: Database password
    
    Returns:
        bool: True if database was created or already exists, False otherwise
    """
    # Connect to default 'postgres' database to create the target database
    # We can't connect to a database that doesn't exist yet
    try:
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database='postgres',  # Connect to default database
            user=db_config['user'],
            password=db_config['password']
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(
            "SELECT 1 FROM pg_database WHERE datname = %s",
            (db_config['database'],)
        )
        exists = cursor.fetchone()
        
        if exists:
            logger.info(f"Database '{db_config['database']}' already exists")
            cursor.close()
            conn.close()
            return True
        else:
            # Create the database
            logger.info(f"Creating database '{db_config['database']}'...")
            cursor.execute(f'CREATE DATABASE {db_config["database"]}')
            logger.info(f"Database '{db_config['database']}' created successfully")
            cursor.close()
            conn.close()
            return True
            
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        if "Connection refused" in error_msg:
            logger.error(
                f"Cannot connect to PostgreSQL server at {db_config['host']}:{db_config['port']}\n"
                f"Please ensure PostgreSQL is running.\n"
                f"On macOS, try: brew services start postgresql@14 (or your version)\n"
                f"Or: pg_ctl -D /usr/local/var/postgres start"
            )
        elif "password authentication failed" in error_msg.lower():
            logger.error(
                f"Authentication failed for user '{db_config['user']}'\n"
                f"Please check your database credentials."
            )
        elif "could not translate host name" in error_msg.lower():
            logger.error(f"Cannot resolve hostname '{db_config['host']}'")
        else:
            logger.error(f"Database setup failed: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error during database setup: {e}")
        return False


def create_user_if_not_exists(db_config, user_privileges=None):
    """
    Create a database user if it doesn't exist.
    
    Args:
        db_config (dict): Database configuration
        user_privileges (dict): Optional privileges for the user:
            - createdb: Allow user to create databases (default: True)
            - password: Optional password (uses db_config['password'] if not provided)
    
    Returns:
        bool: True if user was created or already exists, False otherwise
    """
    if user_privileges is None:
        user_privileges = {'createdb': True}
    
    try:
        # Connect to default 'postgres' database
        conn = psycopg2.connect(
            host=db_config['host'],
            port=db_config['port'],
            database='postgres',
            user='postgres',  # Use default superuser to create other users
            password=db_config.get('admin_password', db_config['password'])
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if user exists
        cursor.execute(
            "SELECT 1 FROM pg_user WHERE usename = %s",
            (db_config['user'],)
        )
        exists = cursor.fetchone()
        
        if exists:
            logger.info(f"User '{db_config['user']}' already exists")
            cursor.close()
            conn.close()
            return True
        else:
            # Create the user
            logger.info(f"Creating user '{db_config['user']}'...")
            password = user_privileges.get('password', db_config['password'])
            createdb = 'CREATEDB' if user_privileges.get('createdb', True) else 'NOCREATEDB'
            
            cursor.execute(
                f"CREATE USER {db_config['user']} WITH PASSWORD %s {createdb}",
                (password,)
            )
            logger.info(f"User '{db_config['user']}' created successfully")
            cursor.close()
            conn.close()
            return True
            
    except psycopg2.OperationalError as e:
        # If we can't connect as postgres, try with the provided user
        # This might fail if the user doesn't have permission to create users
        logger.warning(f"Could not create user (may require superuser privileges): {e}")
        return False
    except Exception as e:
        logger.error(f"Error creating user: {e}")
        return False


def setup_database(db_config, create_user=False):
    """
    Complete database setup: create user (optional) and database.
    
    Args:
        db_config (dict): Database configuration
        create_user (bool): Whether to attempt to create the user if it doesn't exist
    
    Returns:
        bool: True if setup was successful, False otherwise
    """
    logger.info("Starting database setup...")
    
    # Optionally create user first
    if create_user:
        if not create_user_if_not_exists(db_config):
            logger.warning("User creation failed or skipped. Continuing with database creation...")
    
    # Create database
    return create_database_if_not_exists(db_config)

