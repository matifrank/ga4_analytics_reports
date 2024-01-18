# utils.py
import numpy as np
import pandas as pd
import json
import datetime
from datetime import datetime, timedelta
import os
import time
import shutil
import logging
import csv 
from google.analytics.data_v1beta import BetaAnalyticsDataClient 
from google.analytics.data_v1beta.types import (
    BatchRunReportsRequest,
    RunReportRequest,
    Filter,
    FilterExpression,
    Dimension,
    Metric,
    DateRange,
    OrderBy,
)

def batch_report(config, client, startdate, endate):
    """
    Runs batch analytic reports on a Google Analytics 4 property for each site in the config.

    Parameters:
        config (dict): Configuration dictionary with batch report settings.
        client: The Google Analytics API client.
        start_date (str): Start date for the report in 'YYYY-MM-DD' format.
        end_date (str): End date for the report in 'YYYY-MM-DD' format.

    Returns:
        list: List of tuples containing site ID, report name, and report response.
    """
    logging.info('Running batch reports GA 4 ') 
    responses = [] 

    for report in config['REPORTS']:
        logging.info('loading parameters from reports in config file') 
        for site in report['SITES']:
            property_id = site['PROPID']
            site_id = site['IDSITE']
            report_name = report['NAME']

            dimensions = [Dimension(name=dim['name']) for dim in report['DIMENSIONS']]

            metrics = [Metric(name=met['name']) for met in report['METRICS']]

            date_range = DateRange(
            start_date=startdate,
            end_date=endate,)
            
            # Initialize limit and offset
            limit = 100000
            offset = 0
            
            while True:
                request = RunReportRequest(
                    property=f"properties/{property_id}",
                    dimensions=dimensions,
                    metrics=metrics,
                    date_ranges=[date_range],
                    limit=limit,
                    offset=offset, #https://developers.google.com/analytics/devguides/reporting/data/v1/basics#python_1
                )
                
                # Add the filter condition if present in the report
                if 'FILTER' in report:
                    filter_field_name = report['FILTER'][0]['filter']['fieldName']
                    filter_match_type = report['FILTER'][0]['filter']['stringFilter']['matchType']
                    filter_value = report['FILTER'][0]['filter']['stringFilter']['value']
                    filter_expression = FilterExpression(
                        filter=Filter(
                            field_name=filter_field_name,
                            string_filter=Filter.StringFilter(
                                match_type=filter_match_type,
                                value=filter_value,
                            ),
                        ),
                    )
                    request.dimension_filter = filter_expression
                
                logging.info('Generating Api request')

                # Generate request
                response = client.run_report(request=request)
                if 'rows' not in response:
                    logging.error(f"No data found in the response for report {report_name}")
                    break  # Skip further processing for this report

                # Append the site ID, report name, and response as a tuple
                responses.append((site_id, report_name, response))
                # Update the offset for the next iteration
                offset += limit
                 # Check if all data has been retrieved
                if offset >= response.row_count:
                    break
        
            logging.info('Response received')
                
    return responses


def df_report(resulting_response):
    """
    Process each response and create a DataFrame by ID (site) and ORIGEN(report name) 
    from config file.
    Parameters:
        resulting_response (list): List of tuples containing the site ID, report name, 
        and response.
    Returns:
        pandas.DataFrame: DataFrame containing the processed response data.
    """
    logging.info('Converting response into Dataframe')
    table_data = []

    # Process each response and add data to the DataFrame
    for site_id, report_name, response in resulting_response:
        for row in response.rows:
            row_data = [site_id, report_name]
            for dim_value in row.dimension_values:
                row_data.append(dim_value.value)
            for metric_value in row.metric_values:
                row_data.append(metric_value.value)
            table_data.append(row_data)

    # Define column names
    columns = ['IDSITE', 'ORIGEN']
    for dim in resulting_response[0][2].dimension_headers:
        columns.append(dim.name)
    for metric in resulting_response[0][2].metric_headers:
        columns.append(metric.name)

    # Create the DataFrame
    df = pd.DataFrame(table_data, columns=columns)
    logging.info('Dataframe created')
    return df


def df_bicsv(response_df, config, firstmonth, yesterday, byesterday):
    """
    Separate response records into different dataframes and export CSV 
    for being consumed in Power BI based on conditions and root path from config file.

    Parameters:
        response_df (pd.DataFrame): DataFrame containing the response data.
        config (dict): Configuration dictionary containing report settings.
        firstmonth (str): The first month date for the CSV file name.
        yesterday (str): The yesterday date for the CSV file name.
        byesterday (str): The day before yesterday date for deletion of files.

    Raises:
        AssertionError: If there is a mismatch in the number of sites and reports or if duplicate site IDs are found.

    Returns:
        csv on root location folder for each id site configurated
    """  
    logging.info('Creating DataFrame')
    
    df_lists = {}
    
    # Separate records in response_df into different DataFrames based on "IDSITE"
    for _, row in response_df.iterrows():
        site_id = row['IDSITE']
        if site_id not in df_lists:
            df_lists[site_id] = response_df.loc[response_df['IDSITE'] == site_id]

    # Export each DataFrame to CSV files based on config settings
    for site_id, df_site in df_lists.items():
        report_config = next((report for report in config['REPORTS'] if any(site['IDSITE'] == site_id for site in report['SITES'])), None)
        if report_config is None:
            continue
        # Find the specific site_info based on the site_id
        site_info = next((site for site in report_config['SITES'] if site['IDSITE'] == site_id), None)
        if site_info is None:
            continue

        config_header = config['REPORTS'][0]['HEADER']
        folder = site_info['FOLDER']
        file_path_delete = config['ROOT_PATH'] + folder + '\\' + (report_config['FILEPATH'] % (site_info['KEY'], firstmonth, byesterday))
        filepath = config['ROOT_PATH'] + folder + '\\' + (report_config['FILEPATH'] % (site_info['KEY'], firstmonth, byesterday)) 
      
        logging.info('Generating location files path..') 
        try:
            os.makedirs(config['ROOT_PATH'] + folder, exist_ok=True)

            logging.info('Removing file from yesterday if it already exists (optional)..') 
            if yesterday != firstmonth and os.path.exists(file_path_delete):
                os.remove(file_path_delete)

            logging.info('Exporting DataFrame to CSV in each location..')
            cols_del = [0,1]
            df_site = df_site.drop(df_site.columns[cols_del], axis=1)
            df_site = df_site.rename(columns=dict(zip(df_site.columns, config_header)))
            df_site.to_csv(filepath, index=False)
            
        except Exception as e:
            logging.error(f"Error while exporting DataFrame to CSV: {str(e)}")
            continue

    # Add assert statements to check conditions at the end of the function
    num_sites_in_df_lists = len(df_lists)
    num_sites_in_config_sites = sum(len(report['SITES']) for report in config['REPORTS'])
    assert num_sites_in_df_lists == num_sites_in_config_sites, f"Mismatch in the number of sites ({num_sites_in_df_lists}) and reports ({num_sites_in_config_sites})."
    assert len(df_lists) == len(set(site['IDSITE'] for report in config['REPORTS'] for site in report['SITES'])), "Duplicate site IDs found."
    
     
def df_qvcsv(response_df, config, firstmonth, yesterday, byesterday):
    """
    Separate response records into different dataframes and export CSV 
    for being consumed in Qlikview based on conditions and root path from config file.

    Parameters:
        response_df (pd.DataFrame): DataFrame containing the response data.
        config (dict): Configuration dictionary containing report settings.
        firstmonth (str): The first month date for the CSV file name.
        yesterday (str): The yesterday date for the CSV file name.
        byesterday (str): The day before yesterday date for deletion of files.

    Raises:
        AssertionError: If there is a mismatch in the number of sites and reports or if duplicate site IDs are found.

    Returns:
        csv on root location folder for each id site configurated
    """  
    logging.info('Creating DataFrame')
    
    df_lists = {}
    
    # Separate records in response_df into different DataFrames based on "IDSITE"
    for _, row in response_df.iterrows():
        site_id = row['IDSITE']
        if site_id not in df_lists:
            df_lists[site_id] = response_df.loc[response_df['IDSITE'] == site_id]

    # Export each DataFrame to CSV files based on config settings
    for site_id, df_site in df_lists.items():
        report_config = next((report for report in config['REPORTS'] if any(site['IDSITE'] == site_id for site in report['SITES'])), None)
        if report_config is None:
            continue
        # Find the specific site_info based on the site_id
        site_info = next((site for site in report_config['SITES'] if site['IDSITE'] == site_id), None)
        if site_info is None:
            continue

        config_header = config['REPORTS'][0]['HEADER']
        folder = site_info['FOLDER']
        file_path_delete = config['ROOT_PATH'] + folder + '\\' + (report_config['FILEPATH'] % (site_info['KEY'], firstmonth, byesterday))
        filepath = config['ROOT_PATH'] + folder + '\\' + (report_config['FILEPATH'] % (site_info['KEY'], firstmonth, yesterday)) 
      
        logging.info('Generating location files path..') 
        try:
            os.makedirs(config['ROOT_PATH'] + folder, exist_ok=True)

            logging.info('Removing file from yesterday if it already exists (optional)..') 
            if yesterday != firstmonth and os.path.exists(file_path_delete):
                os.remove(file_path_delete)

            logging.info('Exporting DataFrame to CSV in each location..')
            cols_del = [0,1]
            df_site = df_site.drop(df_site.columns[cols_del], axis=1)
            df_site = df_site.rename(columns=dict(zip(df_site.columns, config_header)))
            
            df_site.to_csv(filepath, sep=",", quotechar='"', index=False, quoting=csv.QUOTE_ALL)
            
            with open(filepath, 'r') as file:
                content = file.read()

            with open(filepath, 'w') as file:
                file.write('\n' * 6)
                file.write(content)
                
        except Exception as e:
            logging.error(f"Error while exporting DataFrame to CSV: {str(e)}")
            continue

    # Add assert statements to check conditions at the end of the function
    num_sites_in_df_lists = len(df_lists)
    num_sites_in_config_sites = sum(len(report['SITES']) for report in config['REPORTS'])
    assert num_sites_in_df_lists == num_sites_in_config_sites, f"Mismatch in the number of sites ({num_sites_in_df_lists}) and reports ({num_sites_in_config_sites})."
    assert len(df_lists) == len(set(site['IDSITE'] for report in config['REPORTS'] for site in report['SITES'])), "Duplicate site IDs found."
    