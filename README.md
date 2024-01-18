# scripts Analytics BI Reports - Api GA 4 

Summary:
Develop new Automation Scripts for Generating GA4 Analytics Reports to being consumed and modeled by two different softwares.

Description:
Create Python scripts to automate the generation of analytical reports. The script retrieves data from Google Analytics 4 properties, processes the results, and exports them to CSV files. The script handles multiple sites, metrics, and dimensions, allowing flexible configurations for properties and reports.

Features:
- Uses the Google Analytics Data API to retrieve analytical data.
- Report configuration stored in JSON config files.
- Separates data into different CSV files for consumption in Power BI and QlikView.
- Handles dynamic date conditions for report generation, deletion, and hosting of newly generated reports.

Files and Structure:

Scripts:
analytics-QV-G4.py: Retrieves data from Google Analytics.
analytics-BI-G4.py: Main script for report generation and processing.

Configuration:
analytics-G4-QV.config.json and analytics-BI-G4.config: Stores configurations and the root directory for saving and deleting reports.
credentials.json: Google Analytics API credentials.

Utilities:
utils.py: Custom functions for data request and transformation, and report export.

Dependencies:
Python 3.x
Google Analytics Data API and credentials.
Libraries: pandas, numpy, datetime, logging, csv

Configure settings in config.json.