import aiohttp
import pandas as pd
from datetime import datetime, timedelta
import asyncio
import argparse

# Constants
API_URL = "https://api.data.gov.sg/v1/transport/carpark-availability"

# Asynchronous function to fetch carpark availability data
async def fetch_carpark_availability(session, date_time):
    try:
        async with session.get(API_URL, params={"date_time": date_time}) as response:
            response.raise_for_status()  # Raise an exception for HTTP errors
            return await response.json()
    except Exception as e:
        print(f"Error fetching data for {date_time}: {e}")
        return None

# Function to generate date range
def generate_date_range(start_date, end_date, time_of_day="09:00:00"):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_list = []
    while start <= end:
        date_time = start.strftime(f"%Y-%m-%dT{time_of_day}")
        date_list.append(date_time)
        start += timedelta(days=1)
    return date_list

# Asynchronous main function
async def main(start_date, end_date, time_of_day="09:00:00", output_csv="CarparkAvailability.csv"):
    # Generate date range
    date_range = generate_date_range(start_date, end_date, time_of_day)
    
    # Initialize an empty DataFrame to aggregate all data
    all_data = pd.DataFrame()

    # Create an aiohttp session
    async with aiohttp.ClientSession() as session:
        # Create a list of tasks to fetch data concurrently
        tasks = [fetch_carpark_availability(session, date_time) for date_time in date_range]
        
        # Wait for all tasks to complete and collect the results
        results = await asyncio.gather(*tasks)

        for result, date_time in zip(results, date_range):
            # Process only successful responses
            if result:
                try:
                    # Normalize JSON data
                    normalized_data = pd.json_normalize(
                        result['items'][0]['carpark_data'],  # Focus on carpark_data
                        'carpark_info',  # Flatten the carpark_info list
                        ['carpark_number', 'update_datetime'],  # Include these attributes from the parent
                        record_prefix='info_'  # Add a prefix to carpark_info fields
                    )

                    # Add the timestamp from the parent level
                    normalized_data['timestamp'] = result['items'][0]['timestamp']

                    # Append normalized data to the overall DataFrame
                    all_data = pd.concat([all_data, normalized_data], ignore_index=True)

                    print(f"Fetched data for {date_time}")

                except Exception as e:
                    print(f"Error processing data for {date_time}: {e}")
        
    # Save the consolidated DataFrame to a CSV file
    all_data.to_csv(output_csv, index=False)
    print(f"Data successfully saved to {output_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and save carpark availability data to CSV.")
    parser.add_argument("--start_date", type=str, required=True, help="Start date in YYYY-MM-DD format (e.g., 2024-01-01).")
    parser.add_argument("--end_date", type=str, required=True, help="End date in YYYY-MM-DD format (e.g., 2025-03-21).")
    parser.add_argument("--time_of_day", type=str, default="09:00:00", help="Time of day in HH:MM:SS format (default: 09:00:00).")
    
    args = parser.parse_args()

    # Run the asynchronous main function
    asyncio.run(main(start_date=args.start_date, end_date=args.end_date, time_of_day=args.time_of_day))
