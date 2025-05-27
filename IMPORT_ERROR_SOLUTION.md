# Import Error Solution Documentation

## Problem Summary

When running `mono_ticketmaster.py` directly using Python, the following error occurred:

```
ImportError: attempted relative import beyond top-level package
```

This error originated from `utils/code_error_analysis.py` trying to import from `..prompts`.

## Root Cause Analysis

### 1. Package Structure Mismatch
- The package is named `scrapegraphai` in `pyproject.toml`
- The actual directory is named `skrrraped_graph`
- This mismatch prevents proper package installation

### 2. Python Module Resolution
When running a script directly (e.g., `python mono_ticketmaster.py`):
- Python adds the script's directory to `sys.path`
- This makes `utils` a top-level module instead of `skrrraped_graph.utils`
- Relative imports like `..prompts` try to go beyond the top-level, causing the error

### 3. Cascading Import Issues
- Importing from `utils` triggers `utils/__init__.py`
- This file imports from multiple modules including `code_error_analysis`
- `code_error_analysis.py` contains the problematic relative imports
- Additional dependencies like `free-proxy` may also be missing

## Solution Implemented

The fix bypasses the problematic import chain by directly loading the required module:

```python
# Add the current directory to sys.path to fix import issues
sys.path.insert(0, str(Path(__file__).parent))

# Import the specific module directly without going through __init__.py
import importlib.util
spec = importlib.util.spec_from_file_location("convert_to_md", 
                                               Path(__file__).parent / "utils" / "convert_to_md.py")
convert_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(convert_module)
convert_to_md = convert_module.convert_to_md
```

## Alternative Solutions

### 1. Run as a Module (Recommended for production)
```bash
# From the parent directory
python -m skrrraped_graph.mono_ticketmaster
```

### 2. Install Package in Development Mode
First, fix the package name mismatch in `pyproject.toml`:
```toml
[project]
name = "skrrraped_graph"  # Match the directory name
```

Then install:
```bash
pip install -e .
```

### 3. Set PYTHONPATH
```bash
export PYTHONPATH=/home/creekz/Projects:$PYTHONPATH
python skrrraped_graph/mono_ticketmaster.py
```

### 4. Fix All Relative Imports
Convert all relative imports to absolute imports:
```python
# Instead of: from ..prompts import TEMPLATE_SYNTAX_ANALYSIS
# Use: from skrrraped_graph.prompts import TEMPLATE_SYNTAX_ANALYSIS
```

## Diagnosis Steps

1. **Check Python Path**: Verify where Python is looking for modules
   ```python
   import sys
   print(sys.path)
   ```

2. **Test Import Isolation**: Try importing modules individually to identify problematic dependencies

3. **Verify Package Structure**: Ensure all directories have `__init__.py` files

4. **Check Dependencies**: Verify all required packages are installed

## Prevention

1. **Consistent Naming**: Keep package names consistent between `pyproject.toml` and directory names
2. **Use Absolute Imports**: Prefer absolute imports over relative imports for scripts meant to be run directly
3. **Entry Points**: Define proper entry points in `pyproject.toml` for command-line scripts
4. **Virtual Environments**: Always use virtual environments to isolate dependencies

## When to Use This Solution

This `importlib` solution is appropriate when:
- You need to run scripts directly without installing the package
- You want to avoid loading unnecessary imports from `__init__.py`
- You're dealing with circular import issues
- You need fine-grained control over module loading

## Limitations

- The solution is specific to each import and must be applied individually
- It bypasses the normal Python import system, which may cause issues with module caching
- It's more verbose than standard imports
- May not work well with complex module interdependencies

## Related Files

- `/home/creekz/Projects/skrrraped_graph/mono_ticketmaster.py` - Fixed script
- `/home/creekz/Projects/skrrraped_graph/utils/code_error_analysis.py` - Contains problematic imports
- `/home/creekz/Projects/skrrraped_graph/pyproject.toml` - Package configuration