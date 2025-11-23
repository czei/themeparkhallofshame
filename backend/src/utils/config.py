"""
Theme Park Downtime Tracker - Configuration Management
Handles environment configuration with AWS SSM Parameter Store for production
and python-dotenv for local development.
"""

import os
from typing import Optional
from dotenv import load_dotenv

# Load .env file for local development
load_dotenv()


class Config:
    """
    Configuration manager with dual-mode operation:
    - Local: Reads from .env file via python-dotenv
    - Production: Reads from AWS SSM Parameter Store
    """

    def __init__(self):
        self.environment = os.getenv('ENVIRONMENT', 'local')
        self._ssm_client = None

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Fetch configuration value from SSM (production) or environment (local).

        Args:
            key: Configuration key name
            default: Default value if key not found

        Returns:
            Configuration value or default
        """
        if self.environment == 'production':
            return self._get_from_ssm(key, default)
        else:
            return os.getenv(key, default)

    def _get_from_ssm(self, key: str, default: Optional[str] = None) -> Optional[str]:
        """
        Fetch configuration from AWS SSM Parameter Store.

        Args:
            key: Parameter key name
            default: Default value if parameter not found

        Returns:
            Parameter value or default

        Raises:
            ConfigurationError: If parameter not found and no default provided,
                               or if AWS credentials/permissions are invalid
        """
        try:
            if self._ssm_client is None:
                import boto3
                self._ssm_client = boto3.client(
                    'ssm',
                    region_name=os.getenv('AWS_REGION', 'us-east-1')
                )

            ssm_prefix = os.getenv('AWS_SSM_PREFIX', '/themeparkhall')
            parameter_name = f"{ssm_prefix}/{key}"

            response = self._ssm_client.get_parameter(
                Name=parameter_name,
                WithDecryption=True
            )
            return response['Parameter']['Value']

        except self._ssm_client.exceptions.ParameterNotFound:
            # Parameter doesn't exist in SSM
            if default is not None:
                return default
            raise ConfigurationError(
                f"Required parameter '{key}' not found in SSM at path '{parameter_name}'. "
                f"Please create the parameter or provide a default value."
            )

        except Exception as e:
            # Handle other errors (credentials, permissions, network, etc.)
            error_type = type(e).__name__
            if default is not None:
                # Log the error but continue with default
                import logging
                logging.warning(
                    f"Failed to fetch SSM parameter '{key}': {error_type}: {e}. "
                    f"Using default value."
                )
                return default
            raise ConfigurationError(
                f"Failed to fetch parameter '{key}' from SSM: {error_type}: {e}. "
                f"Check AWS credentials, IAM permissions, and network connectivity."
            )

    def get_int(self, key: str, default: int) -> int:
        """
        Get configuration value as integer.

        Args:
            key: Configuration key name
            default: Default value if key not found or conversion fails

        Returns:
            Integer value or default
        """
        value = self.get(key, str(default))
        try:
            return int(value)
        except (ValueError, TypeError) as e:
            # Log warning but don't crash - use default instead
            import logging
            logging.warning(
                f"Invalid integer for config key '{key}': '{value}'. "
                f"Using default={default}. Error: {e}"
            )
            return default

    def get_bool(self, key: str, default: bool) -> bool:
        """
        Get configuration value as boolean.

        Args:
            key: Configuration key name
            default: Default value if key not found

        Returns:
            Boolean value or default
        """
        value = self.get(key, str(default))
        if value is None:
            return default
        return value.lower() in ('true', '1', 'yes', 'on')

    @property
    def is_production(self) -> bool:
        """Check if running in production environment."""
        return self.environment == 'production'

    @property
    def is_local(self) -> bool:
        """Check if running in local development environment."""
        return self.environment == 'local'


class ConfigurationError(Exception):
    """Raised when configuration cannot be loaded."""
    pass


# Global configuration instance
config = Config()


# Database configuration
DB_HOST = config.get('DB_HOST', 'localhost')
DB_PORT = config.get_int('DB_PORT', 3306)
DB_NAME = config.get('DB_NAME', 'themepark_tracker_dev')
DB_USER = config.get('DB_USER', 'root')
DB_PASSWORD = config.get('DB_PASSWORD', '')

# Queue-Times.com API configuration
QUEUE_TIMES_API_BASE_URL = config.get('QUEUE_TIMES_API_BASE_URL', 'https://queue-times.com')
QUEUE_TIMES_API_KEY = config.get('QUEUE_TIMES_API_KEY', '')

# Flask configuration
FLASK_ENV = config.get('FLASK_ENV', 'development')
FLASK_DEBUG = config.get_bool('FLASK_DEBUG', True)
SECRET_KEY = config.get('SECRET_KEY', 'dev-secret-key-change-in-production')

# Logging configuration
LOG_LEVEL = config.get('LOG_LEVEL', 'INFO')

# Data collection settings
COLLECTION_INTERVAL_MINUTES = config.get_int('COLLECTION_INTERVAL_MINUTES', 10)
MAX_RETRY_ATTEMPTS = config.get_int('MAX_RETRY_ATTEMPTS', 3)
RETRY_BACKOFF_MULTIPLIER = config.get_int('RETRY_BACKOFF_MULTIPLIER', 2)

# Geographic filter for testing phase (US-only)
FILTER_COUNTRY = config.get('FILTER_COUNTRY', 'US')  # Set to empty string '' for all countries

# API rate limiting
API_RATE_LIMIT_PER_HOUR = config.get_int('API_RATE_LIMIT_PER_HOUR', 100)
API_RATE_LIMIT_PER_DAY = config.get_int('API_RATE_LIMIT_PER_DAY', 1000)

# Database connection pool settings (from research.md)
DB_POOL_SIZE = 10
DB_POOL_MAX_OVERFLOW = 20
DB_POOL_RECYCLE = 3600  # Recycle connections after 1 hour
DB_POOL_PRE_PING = True  # Health check connections before use
