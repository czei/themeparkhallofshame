"""
Theme Park Downtime Tracker - Configuration Unit Tests

Tests the Config class with:
- Environment variable loading
- AWS SSM Parameter Store integration (mocked)
- Type conversions (int, bool)
- Default value handling
- Error handling for missing configuration

Priority: P0 - Foundation (configuration used by all modules)
"""

import pytest
import sys
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

# Add src to path for imports
backend_src = Path(__file__).parent.parent.parent / 'src'
sys.path.insert(0, str(backend_src.absolute()))


# ============================================================================
# Test Class: Config - Local Mode (Environment Variables)
# ============================================================================

class TestConfigLocalMode:
    """
    Test Config class in local development mode.

    Mode: ENVIRONMENT='local'
    Source: os.getenv() from .env file or system environment
    Priority: P0 - Most common mode during development
    """

    def test_config_defaults_to_local_environment(self):
        """
        Config should default to 'local' environment if ENVIRONMENT not set.

        Given: ENVIRONMENT not set in environment
        When: Config() is instantiated
        Then: environment should be 'local'
        """
        with patch.dict(os.environ, {}, clear=True):
            from utils.config import Config
            config = Config()
            assert config.environment == 'local'
            assert config.is_local is True
            assert config.is_production is False

    def test_config_reads_environment_variable(self):
        """
        Config should read ENVIRONMENT from environment variables.

        Given: ENVIRONMENT='local' in environment
        When: Config() is instantiated
        Then: environment should be 'local'
        """
        with patch.dict(os.environ, {'ENVIRONMENT': 'local'}):
            from utils.config import Config
            config = Config()
            assert config.environment == 'local'

    def test_get_returns_environment_variable_in_local_mode(self):
        """
        Config.get() should read from os.getenv() in local mode.

        Given: ENVIRONMENT='local', TEST_KEY='test_value'
        When: config.get('TEST_KEY') is called
        Then: Return 'test_value'
        """
        with patch.dict(os.environ, {'ENVIRONMENT': 'local', 'TEST_KEY': 'test_value'}):
            from utils.config import Config
            config = Config()
            result = config.get('TEST_KEY')
            assert result == 'test_value'

    def test_get_returns_default_when_key_not_found(self):
        """
        Config.get() should return default value when key not found.

        Given: ENVIRONMENT='local', MISSING_KEY not set
        When: config.get('MISSING_KEY', 'default_value') is called
        Then: Return 'default_value'
        """
        with patch.dict(os.environ, {'ENVIRONMENT': 'local'}, clear=True):
            from utils.config import Config
            config = Config()
            result = config.get('MISSING_KEY', 'default_value')
            assert result == 'default_value'

    def test_get_returns_none_when_key_not_found_and_no_default(self):
        """
        Config.get() should return None when key not found and no default.

        Given: ENVIRONMENT='local', MISSING_KEY not set
        When: config.get('MISSING_KEY') is called
        Then: Return None
        """
        with patch.dict(os.environ, {'ENVIRONMENT': 'local'}, clear=True):
            from utils.config import Config
            config = Config()
            result = config.get('MISSING_KEY')
            assert result is None


# ============================================================================
# Test Class: Config - Production Mode (AWS SSM)
# ============================================================================

class TestConfigProductionMode:
    """
    Test Config class in production mode with AWS SSM Parameter Store.

    Mode: ENVIRONMENT='production'
    Source: AWS SSM Parameter Store (mocked)
    Priority: P1 - Production deployment
    """

    def test_config_recognizes_production_environment(self):
        """
        Config should recognize 'production' environment.

        Given: ENVIRONMENT='production'
        When: Config() is instantiated
        Then: environment should be 'production', is_production should be True
        """
        with patch.dict(os.environ, {'ENVIRONMENT': 'production'}):
            from utils.config import Config
            config = Config()
            assert config.environment == 'production'
            assert config.is_production is True
            assert config.is_local is False

    @patch('boto3.client')
    def test_get_fetches_from_ssm_in_production_mode(self, mock_boto_client):
        """
        Config.get() should fetch from AWS SSM in production mode.

        Given: ENVIRONMENT='production'
        When: config.get('DB_HOST') is called
        Then: Fetch from SSM at path /themeparkhall/DB_HOST
        """
        # Mock SSM client
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {
            'Parameter': {'Value': 'prod-database.aws.com'}
        }
        mock_boto_client.return_value = mock_ssm

        with patch.dict(os.environ, {'ENVIRONMENT': 'production'}):
            from utils.config import Config
            config = Config()
            result = config.get('DB_HOST')

            # Verify SSM was called correctly
            mock_boto_client.assert_called_once_with('ssm', region_name='us-east-1')
            mock_ssm.get_parameter.assert_called_once_with(
                Name='/themeparkhall/DB_HOST',
                WithDecryption=True
            )
            assert result == 'prod-database.aws.com'

    @patch('boto3.client')
    def test_get_uses_custom_ssm_prefix(self, mock_boto_client):
        """
        Config should use custom AWS_SSM_PREFIX if provided.

        Given: ENVIRONMENT='production', AWS_SSM_PREFIX='/custom/prefix'
        When: config.get('DB_HOST') is called
        Then: Fetch from SSM at path /custom/prefix/DB_HOST
        """
        mock_ssm = MagicMock()
        mock_ssm.get_parameter.return_value = {
            'Parameter': {'Value': 'custom-database.aws.com'}
        }
        mock_boto_client.return_value = mock_ssm

        with patch.dict(os.environ, {
            'ENVIRONMENT': 'production',
            'AWS_SSM_PREFIX': '/custom/prefix'
        }):
            from utils.config import Config
            config = Config()
            result = config.get('DB_HOST')

            mock_ssm.get_parameter.assert_called_once_with(
                Name='/custom/prefix/DB_HOST',
                WithDecryption=True
            )
            assert result == 'custom-database.aws.com'

    @patch('boto3.client')
    def test_get_returns_default_when_ssm_parameter_not_found(self, mock_boto_client):
        """
        Config.get() should return default when SSM parameter doesn't exist.

        Given: ENVIRONMENT='production', SSM parameter not found
        When: config.get('MISSING_PARAM', 'default_value') is called
        Then: Return 'default_value'
        """
        # Create a proper exception class
        class ParameterNotFound(Exception):
            pass

        mock_ssm = MagicMock()
        mock_ssm.exceptions = MagicMock()
        mock_ssm.exceptions.ParameterNotFound = ParameterNotFound
        mock_ssm.get_parameter.side_effect = ParameterNotFound("Not found")
        mock_boto_client.return_value = mock_ssm

        with patch.dict(os.environ, {'ENVIRONMENT': 'production'}):
            from utils.config import Config
            config = Config()
            result = config.get('MISSING_PARAM', 'default_value')
            assert result == 'default_value'

    @patch('boto3.client')
    def test_get_raises_error_when_ssm_parameter_not_found_and_no_default(self, mock_boto_client):
        """
        Config.get() should raise ConfigurationError when SSM parameter missing and no default.

        Given: ENVIRONMENT='production', SSM parameter not found, no default
        When: config.get('REQUIRED_PARAM') is called
        Then: Raise ConfigurationError with helpful message
        """
        from utils.config import Config, ConfigurationError

        # Create a proper exception class
        class ParameterNotFound(Exception):
            pass

        mock_ssm = MagicMock()
        mock_ssm.exceptions = MagicMock()
        mock_ssm.exceptions.ParameterNotFound = ParameterNotFound
        mock_ssm.get_parameter.side_effect = ParameterNotFound("Not found")
        mock_boto_client.return_value = mock_ssm

        with patch.dict(os.environ, {'ENVIRONMENT': 'production'}):
            config = Config()
            with pytest.raises(ConfigurationError) as exc_info:
                config.get('REQUIRED_PARAM')

            assert 'REQUIRED_PARAM' in str(exc_info.value)
            assert '/themeparkhall/REQUIRED_PARAM' in str(exc_info.value)

    @patch('boto3.client')
    def test_get_returns_default_on_aws_credentials_error(self, mock_boto_client):
        """
        Config.get() should return default when AWS credentials are invalid.

        Given: ENVIRONMENT='production', AWS credentials invalid
        When: config.get('DB_HOST', 'localhost') is called
        Then: Return 'localhost' and log warning
        """
        # Create a proper exception class for ParameterNotFound
        class ParameterNotFound(Exception):
            pass

        mock_ssm = MagicMock()
        # Set up exceptions attribute
        mock_ssm.exceptions = MagicMock()
        mock_ssm.exceptions.ParameterNotFound = ParameterNotFound
        # Raise a different exception (credentials error)
        mock_ssm.get_parameter.side_effect = Exception("NoCredentialsError")
        mock_boto_client.return_value = mock_ssm

        with patch.dict(os.environ, {'ENVIRONMENT': 'production'}):
            from utils.config import Config
            config = Config()
            result = config.get('DB_HOST', 'localhost')
            assert result == 'localhost'


# ============================================================================
# Test Class: Type Conversion Methods
# ============================================================================

class TestConfigTypeConversions:
    """
    Test Config type conversion methods: get_int(), get_bool()

    Priority: P0 - Used for port numbers, boolean flags
    """

    def test_get_int_converts_string_to_integer(self):
        """
        Config.get_int() should convert string to integer.

        Given: DB_PORT='3306' (string)
        When: config.get_int('DB_PORT', 3306) is called
        Then: Return 3306 (integer)
        """
        with patch.dict(os.environ, {'ENVIRONMENT': 'local', 'DB_PORT': '3306'}):
            from utils.config import Config
            config = Config()
            result = config.get_int('DB_PORT', 3306)
            assert result == 3306
            assert isinstance(result, int)

    def test_get_int_returns_default_when_key_not_found(self):
        """
        Config.get_int() should return default when key not found.

        Given: MISSING_PORT not set
        When: config.get_int('MISSING_PORT', 8080) is called
        Then: Return 8080 (default)
        """
        with patch.dict(os.environ, {'ENVIRONMENT': 'local'}, clear=True):
            from utils.config import Config
            config = Config()
            result = config.get_int('MISSING_PORT', 8080)
            assert result == 8080

    def test_get_int_returns_default_on_invalid_conversion(self):
        """
        Config.get_int() should return default when value cannot be converted.

        Given: INVALID_PORT='not-a-number'
        When: config.get_int('INVALID_PORT', 3306) is called
        Then: Return 3306 (default) and log warning
        """
        with patch.dict(os.environ, {
            'ENVIRONMENT': 'local',
            'INVALID_PORT': 'not-a-number'
        }):
            from utils.config import Config
            config = Config()
            result = config.get_int('INVALID_PORT', 3306)
            assert result == 3306

    def test_get_bool_converts_true_strings(self):
        """
        Config.get_bool() should convert 'true', '1', 'yes', 'on' to True.

        Given: Various truthy string values
        When: config.get_bool() is called
        Then: Return True
        """
        with patch.dict(os.environ, {'ENVIRONMENT': 'local'}):
            from utils.config import Config
            config = Config()

            truthy_values = ['true', 'True', 'TRUE', '1', 'yes', 'Yes', 'on', 'On']
            for value in truthy_values:
                with patch.dict(os.environ, {'TEST_BOOL': value}):
                    result = config.get_bool('TEST_BOOL', False)
                    assert result is True, f"'{value}' should be True"

    def test_get_bool_converts_false_strings(self):
        """
        Config.get_bool() should convert anything else to False.

        Given: Various falsy string values
        When: config.get_bool() is called
        Then: Return False
        """
        with patch.dict(os.environ, {'ENVIRONMENT': 'local'}):
            from utils.config import Config
            config = Config()

            falsy_values = ['false', 'False', 'FALSE', '0', 'no', 'No', 'off', 'Off', 'random']
            for value in falsy_values:
                with patch.dict(os.environ, {'TEST_BOOL': value}):
                    result = config.get_bool('TEST_BOOL', True)
                    assert result is False, f"'{value}' should be False"

    def test_get_bool_returns_default_when_key_not_found(self):
        """
        Config.get_bool() should return default when key not found.

        Given: MISSING_BOOL not set
        When: config.get_bool('MISSING_BOOL', True) is called
        Then: Return True (default)
        """
        with patch.dict(os.environ, {'ENVIRONMENT': 'local'}, clear=True):
            from utils.config import Config
            config = Config()
            result = config.get_bool('MISSING_BOOL', True)
            assert result is True


# ============================================================================
# Test Class: Global Configuration Constants
# ============================================================================

class TestGlobalConfigConstants:
    """
    Test global configuration constants exported by config module.

    Priority: P1 - These are used throughout the application
    """

    def test_database_config_constants_exist(self):
        """
        Database configuration constants should be available.

        Given: Config module loaded
        When: Importing DB_* constants
        Then: All constants should be defined
        """
        from utils import config

        assert hasattr(config, 'DB_HOST')
        assert hasattr(config, 'DB_PORT')
        assert hasattr(config, 'DB_NAME')
        assert hasattr(config, 'DB_USER')
        assert hasattr(config, 'DB_PASSWORD')

    def test_database_config_defaults(self):
        """
        Database configuration should have sensible defaults for local dev.

        Given: No environment variables set
        When: Importing DB_* constants
        Then: Defaults should be localhost, port 3306, dev database
        """
        with patch.dict(os.environ, {}, clear=True):
            # Need to reload config module to pick up cleared env
            import importlib
            from utils import config as config_module
            importlib.reload(config_module)

            assert config_module.DB_HOST == 'localhost'
            assert config_module.DB_PORT == 3306
            assert config_module.DB_NAME == 'themepark_tracker_dev'

    def test_queue_times_api_config_exists(self):
        """
        Queue-Times API configuration should be available.

        Given: Config module loaded
        When: Importing QUEUE_TIMES_* constants
        Then: All constants should be defined
        """
        from utils import config

        assert hasattr(config, 'QUEUE_TIMES_API_BASE_URL')
        assert hasattr(config, 'QUEUE_TIMES_API_KEY')

    def test_flask_config_exists(self):
        """
        Flask configuration should be available.

        Given: Config module loaded
        When: Importing FLASK_* constants
        Then: All constants should be defined
        """
        from utils import config

        assert hasattr(config, 'FLASK_ENV')
        assert hasattr(config, 'FLASK_DEBUG')
        assert hasattr(config, 'SECRET_KEY')

    def test_collection_settings_exist(self):
        """
        Data collection settings should be available.

        Given: Config module loaded
        When: Importing collection constants
        Then: All constants should be defined
        """
        from utils import config

        assert hasattr(config, 'COLLECTION_INTERVAL_MINUTES')
        assert hasattr(config, 'MAX_RETRY_ATTEMPTS')
        assert hasattr(config, 'RETRY_BACKOFF_MULTIPLIER')
        assert hasattr(config, 'FILTER_COUNTRY')


# ============================================================================
# Test Class: ConfigurationError Exception
# ============================================================================

class TestConfigurationError:
    """
    Test ConfigurationError exception class.

    Priority: P2 - Error handling
    """

    def test_configuration_error_is_exception(self):
        """
        ConfigurationError should be an Exception subclass.

        Given: ConfigurationError class
        When: Checking inheritance
        Then: Should be subclass of Exception
        """
        from utils.config import ConfigurationError
        assert issubclass(ConfigurationError, Exception)

    def test_configuration_error_can_be_raised(self):
        """
        ConfigurationError should be raiseable with custom message.

        Given: ConfigurationError class
        When: Raising with custom message
        Then: Should capture message correctly
        """
        from utils.config import ConfigurationError

        with pytest.raises(ConfigurationError) as exc_info:
            raise ConfigurationError("Test error message")

        assert "Test error message" in str(exc_info.value)
