"""
Type conversion utilities for database parameters.
"""
import logging

import numpy as np
import pandas as pd
import pyarrow as pa

logger = logging.getLogger(__name__)

# Type collections for conversion
NUMPY_FLOAT_TYPES = (np.float64, np.float32, np.float16)
NUMPY_INT_TYPES = (np.int64, np.int32, np.int16, np.int8)
NUMPY_UINT_TYPES = (np.uint64, np.uint32, np.uint16, np.uint8)
PYARROW_NUMERIC_TYPES = (pa.FloatScalar, pa.DoubleScalar)
PANDAS_NULLABLE_TYPES = (
    pd.Int64Dtype, pd.Int32Dtype, pd.Int16Dtype, pd.Int8Dtype,
    pd.UInt64Dtype, pd.UInt32Dtype, pd.UInt16Dtype, pd.UInt8Dtype,
    pd.Float64Dtype
)


class TypeConverter:
    """Universal type conversion for database parameters"""

    @staticmethod
    def convert_value(value):
        """Single-pass type conversion"""
        if value is None:
            return None

        # Handle numpy types efficiently
        if isinstance(value, NUMPY_FLOAT_TYPES):
            return None if np.isnan(value) else float(value)
        if isinstance(value, (NUMPY_INT_TYPES + NUMPY_UINT_TYPES)):
            return int(value)

        # Handle pandas types more comprehensively
        if pd.api.types.is_scalar(value) and pd.isna(value):
            return None

        # Handle pandas nullable types (Int64, Float64, etc.)
        if hasattr(value, 'dtype') and pd.api.types.is_dtype_equal(value.dtype, 'object'):
            if pd.isna(value):
                return None

        # Handle normal pandas nullable types
        if isinstance(value, PANDAS_NULLABLE_TYPES):
            return None if pd.isna(value) else value

        # PyArrow scalar value handling
        if hasattr(value, '_is_arrow_scalar') or isinstance(value, pa.Scalar):
            try:
                if pa.compute.is_null(value).as_py():
                    return None
                if hasattr(value, 'as_py'):
                    return value.as_py()
                return value.value
            except (AttributeError, ValueError) as e:
                # Log error with type info for troubleshooting
                logger.warning(f'Failed to convert PyArrow value of type {type(value)}: {e}')
                return None

        # Handle PyArrow arrays
        if isinstance(value, pa.Array):
            try:
                return value.to_pylist()
            except Exception as e:
                logger.warning(f'Failed to convert PyArrow array: {e}')
                return None

        # Handle PyArrow chunks
        if isinstance(value, pa.ChunkedArray):
            try:
                return value.to_pylist()
            except Exception as e:
                logger.warning(f'Failed to convert PyArrow chunked array: {e}')
                return None

        # Handle PyArrow table columns
        if isinstance(value, pa.Table):
            try:
                return value.to_pandas()
            except Exception as e:
                logger.warning(f'Failed to convert PyArrow table: {e}')
                return None

        return value

    @staticmethod
    def convert_params(params):
        """Convert parameter collection"""
        if params is None:
            return None

        if isinstance(params, dict):
            return {k: TypeConverter.convert_value(v) for k, v in params.items()}

        if isinstance(params, list | tuple):
            return [TypeConverter.convert_value(v) for v in params]

        return TypeConverter.convert_value(params)
