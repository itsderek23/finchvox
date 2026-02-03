# Releasing FinchVox to PyPI

Step-by-step guide for manually releasing FinchVox to PyPI.

## Prerequisites

- [ ] PyPI account with API token configured
- [ ] `uv` installed (or `pip`)
- [ ] Clean git working directory

## Release Workflow

### 1. Update Version

Edit `pyproject.toml`:
```toml
version = "0.0.2"  # Increment version
```

### 2. Update Changelog (Optional)

Add release notes to `CHANGELOG.md`.

### 3. Commit and Tag

```bash
# Commit changes
git add -A
git commit -m "Release v0.0.2"

# Create git tag
git tag v0.0.2

# Push commits and tags
git push && git push --tags
```

## Build Package

### 1. Clean Previous Builds

```bash
rm -rf dist/
```

### 2. Build

```bash
uv build
```

Expected output: Creates `dist/finchvox-0.0.2-py3-none-any.whl` and `dist/finchvox-0.0.2.tar.gz`

### 3. Verify Build

```bash
ls -lh dist/
```

## Test Locally

### 1. Create Test Environment

```bash
python -m venv test-env
source test-env/bin/activate
```

### 2. Install from Wheel

```bash
pip install dist/finchvox-0.0.2-py3-none-any.whl
```

### 3. Verify Installation

```bash
# Test CLI
finchvox --help

# Test import
python -c "import finchvox; print('OK')"
```

### 4. Cleanup

```bash
deactivate
rm -rf test-env
```

## Upload to PyPI

```bash
 uv publish dist/*
```

Enter your PyPI API token when prompted.

### 3. Verify Upload

Visit: https://pypi.org/project/finchvox/

### 4. Test Install from PyPI

```bash
pip install finchvox==0.0.2
```

## Troubleshooting

**Build fails:**
- Check `pyproject.toml` syntax
- Verify `src/finchvox/` structure

**Upload fails:**
- Verify PyPI API token is correct
- Check if version already exists on PyPI (can't overwrite)
- Ensure `~/.pypirc` is configured if not using token prompt

**Import fails after install:**
- Verify `src/` directory structure matches `packages` in `pyproject.toml`
- Check `hatchling.build.targets.wheel` configuration

**Missing UI files in package:**
- Verify `force-include` section in `pyproject.toml`
- Check that `ui/` directory exists and contains files

**Version not updating:**
- Clear pip cache: `pip cache purge`
- Use `--no-cache-dir`: `pip install --no-cache-dir finchvox==0.0.2`
