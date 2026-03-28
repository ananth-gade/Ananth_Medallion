# 🧪 DBT Test Automation Guide

This directory contains tools and templates for generating, executing, and reporting on DBT test cases using AI-powered test generation and automated HTML reporting.

## 📁 Directory Contents

```
tests/automation/
├── README.md                    # This guide
├── LLM_PROMPT.txt              # Template prompt for AI test generation
├── generate_test_report.py     # HTML report generator script
└── test_results/               # Generated HTML reports (auto-created)
    └── dbt_test_report.html   # Latest test results report
```

---

## 🚀 Quick Start Guide

Follow these 4 simple steps to generate comprehensive test cases and reports for your DBT models:

### **Step 1: Generate Test Cases with AI** 🤖

1. **Copy the LLM Prompt Template**
   - Open [LLM_PROMPT.txt](LLM_PROMPT.txt)
   - Copy the entire contents

2. **Customize for Your Model**
   - Replace `<DBT MODEL FILE NAME>` with your target model name
   - Replace `<DBT MODEL YML FILE NAME>` with the actual YAML content of your model's schema file
   - Paste this into your VS Code AI assistant chat window

3. **Generate Test Cases**
   - The AI will generate a complete YAML test suite
   - Copy the generated YAML and merge it into your model's schema file
   - The AI will also create any missing custom SQL test files in `tests/generic/`

### **Step 2: Execute DBT Tests** ⚡

Run your DBT tests to generate the results data:

```bash
cd /path/to/rsi_medallion_dbt
dbt test --select <Model_Name>
```

**Example:**
```bash
dbt test --select EMPLOYEE
```

This command will:
- Execute all tests for the specified model
- Generate `target/run_results.json` with detailed test results
- Show a summary in the terminal

### **Step 3: Generate HTML Report** 📊

Execute the Python script to create a beautiful HTML dashboard:

```bash
cd tests/automation
python generate_test_report.py
```

The script will:
- ✅ Read test results from `../../target/run_results.json`
- ✅ Create the `test_results/` directory if needed
- ✅ Generate `test_results/dbt_test_report.html`
- ✅ Display a summary in the terminal

### **Step 4: View Results** 🌐

Open the generated HTML report in your browser:

```bash
# Option 1: Let the script open it automatically
python generate_test_report.py
# When prompted, type 'y' to open in browser

# Option 2: Open manually
# Navigate to tests/automation/test_results/dbt_test_report.html
# Double-click or open with your preferred browser
```

---

## 📋 Detailed Instructions

### **AI Test Generation Best Practices**

When using the LLM prompt:

1. **Be Specific with Model Information**
   - Include the complete YAML schema
   - Mention any business rules or constraints
   - Specify relationships to other models

2. **Review Generated Tests**
   - Ensure test severity levels are appropriate
   - Verify relationship tests point to correct models
   - Check that accepted_values lists are complete

3. **Custom SQL Tests**
   - The AI will reference existing tests in `tests/generic/`
   - If new custom tests are needed, create them before referencing
   - Follow the existing naming patterns and functionality

### **DBT Test Execution Options**

```bash
# Test a specific model
dbt test --select model_name

# Test multiple models
dbt test --select model1 model2

# Test with specific tags
dbt test --select tag:data_quality

# Test with full refresh
dbt test --full-refresh --select model_name

# Test with threads for faster execution
dbt test --threads 4 --select model_name
```

### **HTML Report Features**

The generated HTML report includes:

- 📊 **Dashboard Overview**: Summary cards with pass/warn/error/fail counts
- 🎯 **Interactive Filtering**: Filter tests by status type
- 🔍 **Detailed Test Information**: Expandable sections with:
  - Test execution times
  - Failure counts and messages
  - Complete compiled SQL code
  - Database response details
  - Timing breakdowns
- 📱 **Mobile Responsive**: Works on desktop, tablet, and mobile
- 🎨 **Color-coded Status**: Visual indicators for easy scanning

---

## 🔧 Troubleshooting

### **Common Issues and Solutions**

#### **File Not Found Errors**
```
Error: Could not find ../../target/run_results.json
```
**Solution**: Ensure you've run `dbt test` from the correct directory and the test generated results.

#### **Python Script Errors**
```
ModuleNotFoundError: No module named 'json'
```
**Solution**: Ensure you're using Python 3.6+ with standard libraries.

#### **Empty Test Results**
**Solution**: 
- Verify your model has tests defined in the YAML schema
- Check that `dbt test --select <model>` actually executes tests
- Ensure the model exists and compiles successfully

#### **Permission Errors**
**Solution**: 
- Ensure write permissions in the `tests/automation/` directory
- Run the script from the correct working directory

### **Debugging Steps**

1. **Verify DBT Setup**
   ```bash
   dbt --version
   dbt list --select <model_name>
   ```

2. **Check Test Configuration**
   ```bash
   dbt test --select <model_name> --dry-run
   ```

3. **Validate JSON Output**
   ```bash
   # Check if run_results.json exists and is valid
   python -c "import json; print(json.load(open('target/run_results.json'))['metadata'])"
   ```

---

**Last Updated**: February 26, 2026  
**Version**: 1.0  
**Author**: Enterprise Data Engineering Team