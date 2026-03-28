#!/usr/bin/env python3
"""
DBT Test Results HTML Report Generator

This script parses dbt run_results.json files and generates a comprehensive
HTML report with color-coded status indicators and detailed test information.
"""

import json
import os
from datetime import datetime
from pathlib import Path


class DBTTestReportGenerator:
    def __init__(self, json_file_path="target/run_results.json"):
        self.json_file_path = json_file_path
        self.results_data = None
        
        # Color schemes for different test statuses
        self.status_colors = {
            'pass': {'bg': '#d4edda', 'border': '#c3e6cb', 'text': '#155724'},
            'warn': {'bg': '#fff3cd', 'border': '#ffeaa7', 'text': '#856404'},
            'error': {'bg': '#f8d7da', 'border': '#f5c6cb', 'text': '#721c24'},
            'fail': {'bg': '#f8d7da', 'border': '#f5c6cb', 'text': '#721c24'}
        }
        
        # Icons for different statuses
        self.status_icons = {
            'pass': '✅',
            'warn': '⚠️',
            'error': '❌',
            'fail': '💥'
        }

    def load_json_data(self):
        """Load and parse the run_results.json file."""
        try:
            with open(self.json_file_path, 'r', encoding='utf-8') as file:
                self.results_data = json.load(file)
            print(f"Successfully loaded {self.json_file_path}")
            return True
        except FileNotFoundError:
            print(f"Error: Could not find {self.json_file_path}")
            return False
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON format in {self.json_file_path}: {e}")
            return False

    def get_test_summary(self):
        """Generate summary statistics for all tests."""
        if not self.results_data or 'results' not in self.results_data:
            return {}
        
        results = self.results_data['results']
        summary = {'pass': 0, 'warn': 0, 'error': 0, 'fail': 0, 'total': 0}
        total_execution_time = 0
        
        for result in results:
            status = result.get('status', 'unknown')
            if status in summary:
                summary[status] += 1
            summary['total'] += 1
            total_execution_time += result.get('execution_time', 0)
        
        summary['total_execution_time'] = total_execution_time
        return summary

    def format_test_name(self, unique_id):
        """Extract and format a readable test name from unique_id."""
        # Extract the test name from unique_id like "test.rsi_medallion_dbt.accepted_values_EMPLOYEE_IS_DELETED__0__1.baa77f808c"
        parts = unique_id.split('.')
        if len(parts) >= 3:
            test_name = parts[2]  # Get the test name part
            # Replace underscores with spaces and title case
            formatted_name = test_name.replace('_', ' ').title()
            return formatted_name
        return unique_id

    def format_duration(self, seconds):
        """Format execution time in a readable way."""
        if seconds < 1:
            return f"{seconds * 1000:.0f}ms"
        elif seconds < 60:
            return f"{seconds:.2f}s"
        else:
            minutes = int(seconds // 60)
            seconds = seconds % 60
            return f"{minutes}m {seconds:.1f}s"

    def format_sql_code(self, sql_code):
        """Format SQL code for better readability in HTML."""
        if not sql_code:
            return "No compiled code available"
        
        # Basic SQL formatting - add line breaks and indentation
        formatted = sql_code.strip()
        # Replace multiple whitespace with single space, but preserve line breaks
        lines = formatted.split('\n')
        formatted_lines = []
        for line in lines:
            stripped_line = line.strip()
            if stripped_line:
                formatted_lines.append(stripped_line)
        
        return '\n'.join(formatted_lines)

    def generate_html_report(self):
        """Generate the complete HTML report."""
        if not self.results_data:
            return None

        summary = self.get_test_summary()
        metadata = self.results_data.get('metadata', {})
        results = self.results_data.get('results', [])
        
        # Generate timestamp
        generated_at = metadata.get('generated_at', datetime.now().isoformat())
        try:
            parsed_date = datetime.fromisoformat(generated_at.replace('Z', '+00:00'))
            formatted_date = parsed_date.strftime('%B %d, %Y at %I:%M:%S %p')
        except (ValueError, TypeError):
            formatted_date = generated_at

        html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DBT Test Results Report</title>
    <style>
        * {{ 
            margin: 0; 
            padding: 0; 
            box-sizing: border-box; 
        }}
        
        body {{ 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{ 
            max-width: 1200px; 
            margin: 0 auto; 
            background: white;
            border-radius: 10px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.1);
            overflow: hidden;
        }}
        
        .header {{ 
            background: linear-gradient(45deg, #2c3e50, #34495e);
            color: white; 
            padding: 30px;
            text-align: center;
        }}
        
        .header h1 {{ 
            font-size: 2.5em; 
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        
        .header p {{ 
            font-size: 1.1em; 
            opacity: 0.9;
        }}
        
        .summary {{ 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); 
            gap: 20px; 
            padding: 30px;
            background: #f8f9fa;
        }}
        
        .summary-card {{ 
            background: white; 
            padding: 20px; 
            border-radius: 8px; 
            text-align: center;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
            transition: transform 0.2s ease;
        }}
        
        .summary-card:hover {{ 
            transform: translateY(-5px);
        }}
        
        .summary-card h3 {{ 
            font-size: 2em; 
            margin-bottom: 5px;
        }}
        
        .summary-card.pass {{ border-left: 5px solid #28a745; }}
        .summary-card.warn {{ border-left: 5px solid #ffc107; }}
        .summary-card.error {{ border-left: 5px solid #dc3545; }}
        .summary-card.fail {{ border-left: 5px solid #dc3545; }}
        .summary-card.total {{ border-left: 5px solid #007bff; }}
        .summary-card.time {{ border-left: 5px solid #6f42c1; }}
        
        .results {{ 
            padding: 30px; 
        }}
        
        .results h2 {{ 
            margin-bottom: 20px; 
            color: #2c3e50;
            font-size: 1.8em;
        }}
        
        .test-item {{ 
            margin-bottom: 20px; 
            border-radius: 8px; 
            overflow: hidden;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        
        .test-header {{ 
            padding: 15px 20px; 
            cursor: pointer; 
            display: flex; 
            justify-content: space-between; 
            align-items: center;
            transition: all 0.2s ease;
        }}
        
        .test-header:hover {{ 
            opacity: 0.9;
        }}
        
        .test-title {{ 
            display: flex; 
            align-items: center; 
            font-weight: 600;
        }}
        
        .test-icon {{ 
            margin-right: 10px; 
            font-size: 1.2em;
        }}
        
        .test-info {{ 
            display: flex; 
            gap: 20px; 
            font-size: 0.9em;
        }}
        
        .test-details {{ 
            padding: 20px; 
            background: #fafafa; 
            display: none;
        }}
        
        .test-details.show {{ 
            display: block;
        }}
        
        .detail-grid {{ 
            display: grid; 
            gap: 15px;
        }}
        
        .detail-row {{ 
            display: grid; 
            grid-template-columns: 150px 1fr; 
            gap: 10px;
        }}
        
        .detail-label {{ 
            font-weight: 600; 
            color: #555;
        }}
        
        .detail-value {{ 
            background: white; 
            padding: 8px 12px; 
            border-radius: 4px;
            border: 1px solid #ddd;
            word-wrap: break-word;
        }}
        
        .code-block {{ 
            background: #2d3748; 
            color: #e2e8f0; 
            padding: 15px; 
            border-radius: 5px; 
            font-family: 'Courier New', monospace; 
            font-size: 0.9em; 
            white-space: pre-wrap; 
            overflow-x: auto;
            margin-top: 10px;
        }}
        
        .expand-arrow {{ 
            transition: transform 0.2s ease;
        }}
        
        .expand-arrow.rotated {{ 
            transform: rotate(180deg);
        }}
        
        .metadata {{ 
            background: #e9ecef; 
            padding: 20px; 
            border-top: 1px solid #ddd;
        }}
        
        .metadata h3 {{ 
            margin-bottom: 15px; 
            color: #495057;
        }}
        
        .metadata-grid {{ 
            display: grid; 
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr)); 
            gap: 15px;
        }}
        
        .filter-controls {{ 
            margin-bottom: 20px; 
            display: flex; 
            gap: 10px; 
            flex-wrap: wrap;
            align-items: center;
        }}
        
        .filter-btn {{ 
            padding: 8px 16px; 
            border: 2px solid #ddd; 
            background: white; 
            border-radius: 20px; 
            cursor: pointer; 
            transition: all 0.2s ease;
            font-size: 0.9em;
        }}
        
        .filter-btn:hover {{ 
            background: #f8f9fa;
        }}
        
        .filter-btn.active {{ 
            background: #007bff; 
            color: white; 
            border-color: #007bff;
        }}
        
        @media (max-width: 768px) {{ 
            .summary {{ 
                grid-template-columns: repeat(2, 1fr); 
            }}
            .test-header {{ 
                flex-direction: column; 
                align-items: flex-start; 
                gap: 10px;
            }}
            .test-info {{ 
                flex-direction: column; 
                gap: 5px;
            }}
            .detail-row {{ 
                grid-template-columns: 1fr; 
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🔍 DBT Test Results Report</h1>
            <p>Generated on {formatted_date}</p>
        </div>
        
        <div class="summary">
            <div class="summary-card pass">
                <h3>{summary.get('pass', 0)}</h3>
                <p>Passed Tests</p>
            </div>
            <div class="summary-card warn">
                <h3>{summary.get('warn', 0)}</h3>
                <p>Warnings</p>
            </div>
            <div class="summary-card error">
                <h3>{summary.get('error', 0)}</h3>
                <p>Errors</p>
            </div>
            <div class="summary-card fail">
                <h3>{summary.get('fail', 0)}</h3>
                <p>Failed Tests</p>
            </div>
            <div class="summary-card total">
                <h3>{summary.get('total', 0)}</h3>
                <p>Total Tests</p>
            </div>
            <div class="summary-card time">
                <h3>{self.format_duration(summary.get('total_execution_time', 0))}</h3>
                <p>Total Time</p>
            </div>
        </div>
        
        <div class="results">
            <h2>Test Results Details</h2>
            
            <div class="filter-controls">
                <span style="font-weight: 600; margin-right: 10px;">Filter by Status:</span>
                <button class="filter-btn active" data-status="all">All Tests</button>
                <button class="filter-btn" data-status="pass">Passed</button>
                <button class="filter-btn" data-status="warn">Warnings</button>
                <button class="filter-btn" data-status="error">Errors</button>
                <button class="filter-btn" data-status="fail">Failed</button>
            </div>
            
            <div class="test-results">
"""

        # Generate test items
        for i, result in enumerate(results):
            status = result.get('status', 'unknown')
            colors = self.status_colors.get(status, self.status_colors['error'])
            icon = self.status_icons.get(status, '❓')
            
            test_name = self.format_test_name(result.get('unique_id', 'Unknown Test'))
            execution_time = self.format_duration(result.get('execution_time', 0))
            failures = result.get('failures') or 0
            message = result.get('message', '')
            
            html_content += f"""
                <div class="test-item" data-status="{status}">
                    <div class="test-header" style="background-color: {colors['bg']}; border: 2px solid {colors['border']}; color: {colors['text']};" onclick="toggleDetails({i})">
                        <div class="test-title">
                            <span class="test-icon">{icon}</span>
                            <span>{test_name}</span>
                        </div>
                        <div class="test-info">
                            <span>Status: <strong>{status.upper()}</strong></span>
                            <span>Time: {execution_time}</span>
                            {f'<span>Failures: {failures}</span>' if failures > 0 else ''}
                            <span class="expand-arrow">▼</span>
                        </div>
                    </div>
                    <div class="test-details" id="details-{i}">
                        <div class="detail-grid">
                            <div class="detail-row">
                                <div class="detail-label">Test ID:</div>
                                <div class="detail-value">{result.get('unique_id', 'N/A')}</div>
                            </div>
                            <div class="detail-row">
                                <div class="detail-label">Status:</div>
                                <div class="detail-value">{status.upper()}</div>
                            </div>
                            <div class="detail-row">
                                <div class="detail-label">Execution Time:</div>
                                <div class="detail-value">{execution_time}</div>
                            </div>
                            {'<div class="detail-row"><div class="detail-label">Failures:</div><div class="detail-value">' + str(failures) + '</div></div>' if failures and failures > 0 else ''}
                            {'<div class="detail-row"><div class="detail-label">Message:</div><div class="detail-value">' + message + '</div></div>' if message else ''}
                            <div class="detail-row">
                                <div class="detail-label">Thread ID:</div>
                                <div class="detail-value">{result.get('thread_id', 'N/A')}</div>
                            </div>
"""
            
            # Add timing information
            timing_data = result.get('timing', [])
            if timing_data:
                html_content += f"""
                            <div class="detail-row">
                                <div class="detail-label">Timing Details:</div>
                                <div class="detail-value">
"""
                for timing in timing_data:
                    name = timing.get('name', 'Unknown')
                    started = timing.get('started_at', 'N/A')
                    completed = timing.get('completed_at', 'N/A')
                    html_content += f"<strong>{name.title()}:</strong> {started} → {completed}<br>"
                
                html_content += "</div></div>"
            
            # Add adapter response
            adapter_response = result.get('adapter_response', {})
            if adapter_response:
                html_content += f"""
                            <div class="detail-row">
                                <div class="detail-label">Database Response:</div>
                                <div class="detail-value">
                                    <strong>Code:</strong> {adapter_response.get('code', 'N/A')}<br>
                                    <strong>Message:</strong> {adapter_response.get('_message', 'N/A')}<br>
                                    <strong>Rows Affected:</strong> {adapter_response.get('rows_affected', 'N/A')}<br>
                                    <strong>Query ID:</strong> {adapter_response.get('query_id', 'N/A')}
                                </div>
                            </div>
"""
            
            # Add compiled SQL code
            compiled_code = result.get('compiled_code', '')
            if compiled_code:
                formatted_sql = self.format_sql_code(compiled_code)
                html_content += f"""
                            <div class="detail-row">
                                <div class="detail-label">Compiled SQL:</div>
                                <div class="detail-value">
                                    <div class="code-block">{formatted_sql}</div>
                                </div>
                            </div>
"""
            
            html_content += """
                        </div>
                    </div>
                </div>
"""

        # Add metadata section
        html_content += f"""
            </div>
        </div>
        
        <div class="metadata">
            <h3>📋 Execution Metadata</h3>
            <div class="metadata-grid">
                <div><strong>DBT Version:</strong> {metadata.get('dbt_version', 'N/A')}</div>
                <div><strong>Schema Version:</strong> {metadata.get('dbt_schema_version', 'N/A')}</div>
                <div><strong>Invocation ID:</strong> {metadata.get('invocation_id', 'N/A')}</div>
                <div><strong>Generated At:</strong> {formatted_date}</div>
                <div><strong>Total Execution Time:</strong> {self.format_duration(summary.get('total_execution_time', 0))}</div>
                <div><strong>Command:</strong> {self.results_data.get('args', {}).get('invocation_command', 'N/A')}</div>
            </div>
        </div>
    </div>
    
    <script>
        function toggleDetails(index) {{
            const details = document.getElementById('details-' + index);
            const arrow = document.querySelector(`[onclick="toggleDetails(${{index}})"] .expand-arrow`);
            
            if (details.classList.contains('show')) {{
                details.classList.remove('show');
                arrow.classList.remove('rotated');
            }} else {{
                details.classList.add('show');
                arrow.classList.add('rotated');
            }}
        }}
        
        // Filter functionality
        document.querySelectorAll('.filter-btn').forEach(btn => {{
            btn.addEventListener('click', function() {{
                const status = this.dataset.status;
                
                // Update active button
                document.querySelectorAll('.filter-btn').forEach(b => b.classList.remove('active'));
                this.classList.add('active');
                
                // Filter test items
                document.querySelectorAll('.test-item').forEach(item => {{
                    if (status === 'all' || item.dataset.status === status) {{
                        item.style.display = 'block';
                    }} else {{
                        item.style.display = 'none';
                    }}
                }});
            }});
        }});
        
        // Expand all failed/error tests by default
        document.addEventListener('DOMContentLoaded', function() {{
            document.querySelectorAll('.test-item').forEach((item, index) => {{
                const status = item.dataset.status;
                if (status === 'error' || status === 'fail') {{
                    toggleDetails(index);
                }}
            }});
        }});
    </script>
</body>
</html>"""

        return html_content

    def save_html_report(self, output_path="test_results/dbt_test_report.html"):
        """Save the HTML report to a file."""
        html_content = self.generate_html_report()
        if not html_content:
            print("Error: Could not generate HTML content")
            return False
        
        try:
            # Create the output directory if it doesn't exist
            output_dir = os.path.dirname(output_path)
            if output_dir and not os.path.exists(output_dir):
                os.makedirs(output_dir)
                print(f"Created directory: {output_dir}")
                
            with open(output_path, 'w', encoding='utf-8') as file:
                file.write(html_content)
            print(f"HTML report saved successfully to: {output_path}")
            return True
        except Exception as e:
            print(f"Error saving HTML report: {e}")
            return False

    def open_report_in_browser(self, file_path):
        """Open the generated HTML report in the default browser."""
        try:
            import webbrowser
            abs_path = os.path.abspath(file_path)
            file_url = f"file://{abs_path}"
            webbrowser.open(file_url)
            print(f"Report opened in browser: {file_url}")
        except Exception as e:
            print(f"Could not open browser automatically: {e}")
            print(f"Please manually open: {os.path.abspath(file_path)}")


def main():
    """Main function to run the report generator."""
    print("🔍 DBT Test Results HTML Report Generator")
    print("=" * 50)
    
    # Initialize the report generator
    generator = DBTTestReportGenerator()
    
    # Load the JSON data
    if not generator.load_json_data():
        return
    
    # Generate summary
    summary = generator.get_test_summary()
    print(f"\n📊 Test Summary:")
    print(f"   ✅ Passed: {summary.get('pass', 0)}")
    print(f"   ⚠️  Warnings: {summary.get('warn', 0)}")
    print(f"   ❌ Errors: {summary.get('error', 0)}")
    print(f"   💥 Failed: {summary.get('fail', 0)}")
    print(f"   📈 Total: {summary.get('total', 0)}")
    print(f"   ⏱️  Total Time: {generator.format_duration(summary.get('total_execution_time', 0))}")
    
    # Generate and save the HTML report
    output_file = "test_results/dbt_test_report.html"
    if generator.save_html_report(output_file):
        print(f"\n🎉 Success! Report generated: {output_file}")
        
        # Ask user if they want to open the report
        try:
            response = input("\n🌐 Would you like to open the report in your browser? (y/n): ").lower().strip()
            if response in ['y', 'yes']:
                generator.open_report_in_browser(output_file)
        except KeyboardInterrupt:
            print("\n\nReport generation completed!")
    else:
        print("\n❌ Failed to generate report")


if __name__ == "__main__":
    main()