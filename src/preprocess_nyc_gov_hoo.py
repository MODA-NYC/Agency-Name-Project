import pandas as pd

def preprocess_nyc_gov_hoo(df):
    # Create a new column to store the original 'Agency Name' before modifications
    df['Name - NYC.gov Redesign - Original Value'] = df['Agency Name']

    # Define the mapping directly within the script
    hoo_title_mapping = {
        "First Deputy Mayor": "Mayor's Office First Deputy Mayor",
        "Deputy Mayor for Operations": "Mayor's Office Deputy Mayor for Operations",
        "Deputy Mayor for Economic and Workforce Development": "Mayor's Office Deputy Mayor for Economic and Workforce Development",
        "Deputy Mayor for Health and Human Services": "Mayor's Office Deputy Mayor for Health and Human Services",
        "Deputy Mayor for Public Safety": "Mayor's Office Deputy Mayor for Public Safety",
        "Deputy Mayor for Strategic Initiatives": "Mayor's Office Deputy Mayor for Strategic Initiatives",
        "Deputy Mayor for Communications": "Mayor's Office Deputy Mayor for Communications",
        "Chief Advisor to the Mayor": "Mayor's Office Chief Advisor to the Mayor",
        "Chief Counsel to the Mayor and City Hall": "Mayor's Office Chief Counsel to the Mayor and City Hall",
        "Senior Advisor to the Mayor": "Mayor's Office Senior Advisor to the Mayor",
        "Chief of Staff": "Mayor's Office Chief of Staff",
        "Chief Efficiency Officer": "Mayor's Office Chief Efficiency Officer",
        "Director, Policy and Planning": "Mayor's Office Director of Policy and Planning",
        "Senior Advisor for External Affairs": "Mayor's Office Senior Advisor for External Affairs",
        "Senior Advisor and Director of Public Service Engagement": "Mayor's Office Senior Advisor and Director of Public Service Engagement",
        "Chief Housing officer (Resigned in July)": "Mayor's Office Chief Housing Officer",
        "Executive Director of Talent and Workforce Development": "Mayor's Office Executive Director of Talent and Workforce Development"
    }

    # Correct typos in the 'HoO Title' column
    typo_corrections = {
        "Deputy Mayor for Strategic Intitiatives": "Deputy Mayor for Strategic Initiatives"
    }
    df['HoO Title'] = df['HoO Title'].replace(typo_corrections)

    # Apply the mapping to the 'Agency Name' column
    def update_agency_name(row):
        if row['Agency Name'] == 'Mayor, Office of the':
            hoo_title = row['HoO Title']
            if pd.notna(hoo_title):
                hoo_title_cleaned = hoo_title.strip()  # Strip whitespace
                if hoo_title_cleaned in hoo_title_mapping:
                    return hoo_title_mapping[hoo_title_cleaned]
        return row['Agency Name']

    df['Agency Name'] = df.apply(update_agency_name, axis=1)

    # Remove duplicates based on 'Agency Name', keeping the first occurrence
    df = df.drop_duplicates(subset=['Agency Name'], keep='first')

    return df
