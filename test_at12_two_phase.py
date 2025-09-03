#!/usr/bin/env python3
"""
Test script for AT12 two-phase transformation approach.
This script tests the new gated transformation logic that handles cases
where AT02/AT03 dependencies are not available.
"""

import sys
import os
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.AT12.transformation import AT12TransformationEngine
from src.core.transformation import TransformationContext, TransformationResult
from src.core.config import Config

def create_sample_base_at12_data():
    """Create sample BASE_AT12 data for testing."""
    data = {
        'Numero_Prestamo': ['12345', '67890', '11111'],
        'Identificacion_cliente': ['1234567890', '0987654321', '1111111111'],
        'Tipo_Garantia': ['INMUEBLE', 'VEHICULO', 'INMUEBLE'],
        'Valor_Garantia': ['100000,50', '50000,25', '75000,00'],
        'Fecha_Actualizacion_Avaluo': ['2023-01-15', '2023-02-20', '2023-03-10'],
        'Tipo_Poliza': ['', 'AUTO', ''],
        'Estado_Garantia': ['ACTIVA', 'ACTIVA', 'ACTIVA']
    }
    return pd.DataFrame(data)

def test_two_phase_transformation():
    """Test the two-phase transformation approach."""
    print("Testing AT12 Two-Phase Transformation...")
    
    # Create test configuration
    config = Config()
    
    # Initialize transformation engine
    engine = AT12TransformationEngine(config)
    
    # Create sample data
    sample_data = create_sample_base_at12_data()
    print(f"Created sample BASE_AT12 data with {len(sample_data)} records")
    
    # Import required classes
    from src.core.paths import AT12Paths
    import logging
    
    # Create transformation context
    paths = AT12Paths.from_config(config)
    logger = logging.getLogger(__name__)
    
    context = TransformationContext(
        run_id="test_run_001",
        period="20231201",
        config=config,
        paths=paths,
        source_files=[Path("/tmp/test_base_at12.csv")],
        logger=logger
    )
    
    # Ensure output directory exists
    context.paths.base_transforms_dir.mkdir(parents=True, exist_ok=True)
    
    # Test Case 1: Transformation without dependencies (empty source_data)
    print("\n=== Test Case 1: No Dependencies Available ===")
    result1 = TransformationResult(
        success=False,
        processed_files=[],
        incidence_files=[],
        consolidated_file=None,
        metrics={},
        errors=[],
        warnings=[]
    )
    source_data_empty = {}
    
    try:
        # Test Phase 1a (independent operations)
        df_phase1a = engine._phase1a_independent_operations(
            sample_data.copy(), context, result1, subtype='BASE_AT12'
        )
        print(f"✓ Phase 1a completed successfully: {len(df_phase1a)} records")
        
        # Test Phase 1b (dependent operations) - should handle missing dependencies gracefully
        df_phase1b = engine._phase1b_dependent_operations(
            df_phase1a.copy(), context, result1, source_data_empty, subtype='BASE_AT12'
        )
        print(f"✓ Phase 1b completed successfully: {len(df_phase1b)} records")
        
        # Test Phase 2 with gated processing
        has_at02 = 'AT02_CUENTAS' in source_data_empty
        has_at03 = 'AT03_CREDITOS' in source_data_empty
        print(f"Dependencies available - AT02: {has_at02}, AT03: {has_at03}")
        
        print("✓ Two-phase transformation completed without dependencies")
        
    except Exception as e:
        print(f"✗ Test Case 1 failed: {e}")
        return False
    
    # Test Case 2: Transformation with mock dependencies
    print("\n=== Test Case 2: With Mock Dependencies ===")
    result2 = TransformationResult(
        success=False,
        processed_files=[],
        incidence_files=[],
        consolidated_file=None,
        metrics={},
        errors=[],
        warnings=[]
    )
    
    # Create mock AT03_CREDITOS data
    mock_at03 = pd.DataFrame({
        'num_cta': ['12345', '67890'],
        'tipo_facilidad': ['01', '02']
    })
    
    # Create mock AT02_CUENTAS data
    mock_at02 = pd.DataFrame({
        'numero_cuenta': ['12345', '67890'],
        'fecha_apertura': ['2022-01-01', '2022-06-15']
    })
    
    source_data_with_deps = {
        'AT03_CREDITOS': mock_at03,
        'AT02_CUENTAS': mock_at02
    }
    
    try:
        # Test with dependencies available
        df_with_deps = engine._phase1a_independent_operations(
            sample_data.copy(), context, result2, subtype='BASE_AT12'
        )
        df_with_deps = engine._phase1b_dependent_operations(
            df_with_deps, context, result2, source_data_with_deps, subtype='BASE_AT12'
        )
        
        has_at02 = 'AT02_CUENTAS' in source_data_with_deps
        has_at03 = 'AT03_CREDITOS' in source_data_with_deps
        print(f"Dependencies available - AT02: {has_at02}, AT03: {has_at03}")
        print(f"✓ Two-phase transformation completed with dependencies: {len(df_with_deps)} records")
        
    except Exception as e:
        print(f"✗ Test Case 2 failed: {e}")
        return False
    
    print("\n=== All Tests Passed! ===")
    return True

if __name__ == "__main__":
    success = test_two_phase_transformation()
    sys.exit(0 if success else 1)