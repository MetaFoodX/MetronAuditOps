# Code Quality Setup

This project includes comprehensive code quality checks and automated workflows to ensure high code standards.

## 🚀 Quick Start

### Install Pre-commit Hooks
```bash
# Install pre-commit
pip install pre-commit

# Install the git hook scripts
pre-commit install

# Run against all files (optional)
pre-commit run --all-files
```

### Run Code Quality Checks Locally
```bash
# Python linting and formatting
cd backend
black .
isort .
flake8 .
mypy app/

# Security checks
bandit -r app/
safety check

# Tests
pytest tests/ -v --cov=app
```

## 📋 Code Quality Tools

### Python Tools

#### **Black** - Code Formatter
- Automatically formats Python code to a consistent style
- Line length: 88 characters
- Configuration: `backend/pyproject.toml`

```bash
# Format code
black .

# Check formatting without changes
black --check --diff .
```

#### **isort** - Import Sorter
- Sorts and organizes import statements
- Compatible with Black formatting
- Configuration: `backend/pyproject.toml`

```bash
# Sort imports
isort .

# Check imports without changes
isort --check-only --diff .
```

#### **flake8** - Linter
- Checks for style guide enforcement and potential errors
- Configuration: `backend/.flake8`

```bash
# Run linter
flake8 .
```

#### **mypy** - Type Checker
- Static type checking for Python
- Configuration: `backend/pyproject.toml`

```bash
# Type check
mypy app/
```

#### **Bandit** - Security Linter
- Security vulnerability scanner
- Configuration: `backend/pyproject.toml`

```bash
# Security scan
bandit -r app/
```

#### **Safety** - Dependency Checker
- Checks for known security vulnerabilities in dependencies

```bash
# Check dependencies
safety check
```

### Frontend Tools

#### **ESLint** - JavaScript/TypeScript Linter
- Lints JavaScript and TypeScript code
- Configuration: `frontend/eslint.config.js`

```bash
cd frontend
npm run lint
```

#### **Prettier** - Code Formatter
- Formats JavaScript, TypeScript, CSS, HTML, JSON, and YAML
- Configuration: `frontend/.prettierrc`

```bash
cd frontend
npm run format
```

## 🔄 GitHub Actions Workflows

### Code Quality Workflow (`code-quality.yml`)
Triggers on:
- Push to `main` or `develop` branches
- Pull requests to `main` or `develop` branches

**Jobs:**
1. **Python Linting & Formatting**
   - Black, isort, flake8, mypy
   - Security checks (bandit, safety)
   - Uploads security reports as artifacts

2. **Python Tests**
   - Runs pytest with coverage
   - Uploads coverage reports

3. **Frontend Linting**
   - ESLint, Prettier, TypeScript checks

4. **Frontend Tests**
   - Runs tests and builds project

5. **Docker Build Test**
   - Tests Docker image builds

6. **Security Scan**
   - Trivy vulnerability scanner
   - Uploads results to GitHub Security tab

7. **Dependency Check**
   - Checks for outdated packages

### Deployment Workflow (`deploy.yml`)
Triggers on:
- Push to `main` branch (staging)
- Manual workflow dispatch (staging/production)

**Features:**
- Docker image building and pushing
- Multi-environment deployment
- Container registry integration
- Deployment notifications

## 🔧 Configuration Files

### Python Configuration
- `backend/pyproject.toml` - Black, isort, mypy, pytest, coverage, bandit, safety
- `backend/.flake8` - Flake8 linting rules

### Pre-commit Configuration
- `.pre-commit-config.yaml` - Git hooks for automated checks

### GitHub Actions
- `.github/workflows/code-quality.yml` - Code quality checks
- `.github/workflows/deploy.yml` - Deployment pipeline

## 🎯 Best Practices

### Before Committing
1. Run pre-commit hooks: `pre-commit run`
2. Ensure all tests pass: `pytest`
3. Check code coverage: `pytest --cov=app --cov-report=html`

### Code Review Checklist
- [ ] Code follows style guidelines (Black, flake8)
- [ ] Imports are properly organized (isort)
- [ ] Type hints are used where appropriate (mypy)
- [ ] No security vulnerabilities (bandit, safety)
- [ ] Tests are written and passing
- [ ] Documentation is updated

### Continuous Integration
- All checks run automatically on PR creation
- Required checks must pass before merge
- Security scans run on every push
- Coverage reports are generated

## 🛠️ Troubleshooting

### Common Issues

#### Pre-commit Hook Fails
```bash
# Skip hooks for this commit (emergency only)
git commit --no-verify

# Update pre-commit hooks
pre-commit autoupdate
```

#### MyPy Errors
```bash
# Ignore specific errors
# type: ignore[error-code]

# Skip type checking for specific files
# mypy: disable-file
```

#### Black Formatting Conflicts
```bash
# Format specific file
black path/to/file.py

# Show what would be changed
black --diff path/to/file.py
```

### Performance Tips
- Use pre-commit hooks to catch issues early
- Run specific tools only when needed
- Cache dependencies in CI/CD
- Use parallel execution where possible

## 📊 Monitoring

### Code Quality Metrics
- Test coverage percentage
- Number of linting errors
- Security vulnerabilities found
- Type checking errors

### Reports
- Coverage reports: `htmlcov/index.html`
- Security reports: GitHub Actions artifacts
- Test results: GitHub Actions logs

## 🔐 Security

### Automated Security Checks
- **Bandit**: Python security vulnerabilities
- **Safety**: Dependency vulnerabilities
- **Trivy**: Container image vulnerabilities
- **ESLint**: JavaScript security rules

### Manual Security Reviews
- Review dependency updates
- Audit third-party packages
- Check for sensitive data in commits
- Validate configuration files

## 📚 Additional Resources

- [Black Documentation](https://black.readthedocs.io/)
- [isort Documentation](https://pycqa.github.io/isort/)
- [flake8 Documentation](https://flake8.pycqa.org/)
- [mypy Documentation](https://mypy.readthedocs.io/)
- [Bandit Documentation](https://bandit.readthedocs.io/)
- [Pre-commit Documentation](https://pre-commit.com/)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)
