#!/usr/bin/env python
"""
Script to scrape NYC.gov agency list and save as structured data.

This script scrapes the NYC.gov agencies page to extract agency names,
URLs, and descriptions. The data is saved in a CSV format with proper
field naming conventions for the project.
"""

import os
from pathlib import Path
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def process_agency_info(li_tag):
    """
    Extract agency information from a list item tag.
    
    Args:
        li_tag: BeautifulSoup tag object containing agency information
        
    Returns:
        dict: Dictionary containing processed agency information
    """
    a_tag = li_tag.find('a', class_='name')
    name = a_tag.text.strip() if a_tag else ''
    url = a_tag.get('href', '') if a_tag else ''
    description = li_tag.get('data-desc', '')
    
    # Preprocess name for unique identification: lowercase and stripped
    name_processed = name.lower().strip()
    
    return {
        'Name': name_processed,
        'Name - NYC.gov Agency List': name,
        'URL': url,
        'Description-nyc.gov': description
    }

def scrape_agency_list(url):
    """
    Scrape agency information from the NYC.gov agencies page.
    
    Args:
        url (str): URL of the NYC.gov agencies page
        
    Returns:
        list: List of dictionaries containing agency information
    """
    headers = {
        'User-Agent': ('Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                      'AppleWebKit/537.36 (KHTML, like Gecko) '
                      'Chrome/58.0.3029.110 Safari/537.3')
    }
    
    try:
        logger.info(f"Fetching data from {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        agencies_info = []
        
        # Find all agency list items
        agency_items = soup.select('.alpha-list li')
        logger.info(f"Found {len(agency_items)} agencies")
        
        for li_tag in agency_items:
            agencies_info.append(process_agency_info(li_tag))
        
        return agencies_info
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
        return []

def main():
    """Main function to execute the scraping process."""
    # URL for the NYC.gov agencies page
    url = 'https://www.nyc.gov/nyc-resources/agencies.page'
    
    # Scrape agency information
    agencies_info = scrape_agency_list(url)
    
    if not agencies_info:
        logger.error("No data was scraped. Exiting.")
        return
    
    # Load the scraped data into a DataFrame
    df_nyc_gov = pd.DataFrame(agencies_info)
    
    # Create output directory if it doesn't exist
    output_path = Path('data/raw/nyc_gov_agency_list.csv')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Save the scraped data
    df_nyc_gov.to_csv(output_path, index=False, encoding='utf-8')
    logger.info(f"Scraped {len(df_nyc_gov)} agencies")
    logger.info(f"Data saved to {output_path}")
    
    # Display summary statistics
    logger.info("\nDataset Summary:")
    logger.info(f"Total agencies: {len(df_nyc_gov)}")
    logger.info(f"Agencies with descriptions: {df_nyc_gov['Description-nyc.gov'].notna().sum()}")
    logger.info(f"Agencies with URLs: {df_nyc_gov['URL'].notna().sum()}")

if __name__ == "__main__":
    main() 