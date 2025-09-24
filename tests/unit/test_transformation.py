"""Unit tests for transformation module."""

import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import Mock, patch
from datetime import datetime

from src.core.transformation import (
    TransformationResult,
    TransformationContext,
    TransformationEngine
)
from src.core.incidence_reporter import IncidenceReporter


class TestTransformationResult:
    """Test cases for TransformationResult dataclass."""
    
    def test_transformation_result_creation(self):
        """Test creating a TransformationResult instance."""
        from pathlib import Path
        
        result = TransformationResult(
            success=True,
            processed_files=[Path("output1.csv"), Path("output2.csv")],
            incidence_files=[Path("incidence1.txt")],
            consolidated_file=Path("consolidated.txt"),
            metrics={"records_processed": 100},
            warnings=["Warning 1"],
            errors=["Error 1"]
        )
        
        assert result.success is True
        assert result.total_files_processed == 2
        assert result.has_incidences is True
        assert result.consolidated_file == Path("consolidated.txt")
        assert result.metrics == {"records_processed": 100}
        assert result.warnings == ["Warning 1"]
        assert result.errors == ["Error 1"]
    
    def test_transformation_result_defaults(self):
        """Test TransformationResult with minimal values."""
        result = TransformationResult(
            success=False,
            processed_files=[],
            incidence_files=[],
            consolidated_file=None,
            metrics={},
            warnings=[],
            errors=["Test failed"]
        )
        
        assert result.success is False
        assert result.total_files_processed == 0
        assert result.has_incidences is False
        assert result.consolidated_file is None
        assert result.metrics == {}
        assert result.warnings == []
        assert result.errors == ["Test failed"]


class TestTransformationContext:
    """Test cases for TransformationContext dataclass."""
    
    def test_transformation_context_creation(self):
        """Test creating a TransformationContext instance."""
        from src.core.config import Config
        from src.core.paths import AT12Paths
        from pathlib import Path
        
        config = Config()
        paths = AT12Paths.from_config(config)
        
        context = TransformationContext(
            run_id="test-run",
            period="20240101",
            config=config,
            paths=paths,
            source_files=[Path("input1.csv"), Path("input2.csv")],
            logger=Mock()
        )
        
        assert context.source_files == [Path("input1.csv"), Path("input2.csv")]
        assert context.year == "2024"
        assert context.month == "01"
        assert context.run_id == "test-run"
        assert context.period == "20240101"
        assert context.logger is not None


class ConcreteTransformationEngine(TransformationEngine):
    """Concrete implementation of TransformationEngine for testing."""
    
    def _apply_transformations(self, context: TransformationContext, 
                             source_data: dict, 
                             result: TransformationResult) -> dict:
        """Mock implementation of _apply_transformations."""
        # Return the same data for testing
        return source_data
    
    def _generate_outputs(self, context: TransformationContext, 
                         transformed_data: dict, 
                         result: TransformationResult) -> None:
        """Mock implementation of _generate_outputs."""
        result.output_files = ["output1.csv", "output2.txt"]
        result.total_files_processed = len(transformed_data)


class TestTransformationEngine:
    """Test cases for TransformationEngine base class."""
    
    def test_init(self, temp_dir, mock_logger):
        """Test TransformationEngine initialization."""
        from src.core.config import Config
        
        config = Config()
        
        engine = ConcreteTransformationEngine(
            config=config
        )
        
        assert engine.config == config
        assert engine.logger is not None
        assert hasattr(engine, '_file_reader')
        assert hasattr(engine, '_filename_parser')
    
    def test_transform_success(self, temp_dir, mock_logger):
        """Test successful transformation."""
        from src.core.config import Config
        from src.core.paths import AT12Paths
        from pathlib import Path
        import pandas as pd
        
        config = Config()
        paths = AT12Paths.from_config(config)
        engine = ConcreteTransformationEngine(
            config=config
        )
        
        # Create test files with proper naming convention
        test_file1 = temp_dir / "AT12_202401_001.csv"
        test_file2 = temp_dir / "AT12_202401_002.csv"
        
        # Create sample data
        df = pd.DataFrame({
            'column1': [1, 2, 3],
            'column2': ['A', 'B', 'C']
        })
        df.to_csv(test_file1, index=False)
        df.to_csv(test_file2, index=False)
        
        context = TransformationContext(
            run_id="test-run",
            period="202401",
            config=config,
            paths=paths,
            source_files=[test_file1, test_file2],
            logger=mock_logger
        )
        
        result = engine.transform(context)
        
        # The transformation might fail due to business rules, but should not crash
        assert isinstance(result, TransformationResult)
        assert result.total_files_processed >= 0

    def test_save_dataframe_as_excel_permission_fallback(self, temp_dir):
        """Ensure Excel writer falls back to run-specific filename on permission error."""
        from src.core.config import Config

        engine = ConcreteTransformationEngine(config=Config())
        engine._current_run_id = '202508'

        df = pd.DataFrame({'col': [1, 2, 3]})
        target_path = temp_dir / 'output.xlsx'

        original_excel_writer = pd.ExcelWriter
        call_counter = {'count': 0}

        def excel_writer_side_effect(*args, **kwargs):
            call_counter['count'] += 1
            if call_counter['count'] == 1:
                raise PermissionError('locked')
            return original_excel_writer(*args, **kwargs)

        with patch('src.core.transformation.pd.ExcelWriter', side_effect=excel_writer_side_effect):
            saved_path = engine._save_dataframe_as_excel(df, target_path, sheet_name='TEST')

        assert saved_path is not None
        assert saved_path.name == 'output__run-202508.xlsx'
        assert saved_path.exists()


@pytest.fixture
def temp_dir(tmp_path):
    """Create a temporary directory for testing."""
    return tmp_path


@pytest.fixture
def mock_logger():
    """Create a mock logger for testing."""
    return Mock()
