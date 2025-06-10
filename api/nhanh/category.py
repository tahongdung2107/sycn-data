from datetime import datetime
from service.fetchDataAll import NhanhAPIClient
from service.createTable import create_table
from service.insertUpdateData import process_data

class CategoryService:
    def __init__(self):
        self.api_client = NhanhAPIClient()
        self.table_name = 'categories'  # Default table name for categories

    def get_categories(self, path='/product/category', params=None):
        """
        Get categories from Nhanh API
        :param path: API path (default: /product/category)
        :param params: Additional parameters dictionary
        :return: Dictionary containing data and table structure
        """
        return self.api_client.fetch_data(path, params)

    def run_demo(self):
        """
        Run demo to fetch categories and save to database
        """
        result = self.get_categories()
        print(result['table'])
        if result and 'table' in result:
            print("Table structure:", result['table'])
            # Create table if it doesn't exist
            create_table(result['table'], self.table_name)
            # Add/update data
            # if 'data' in result and result['data']:
            #     process_data(result['data'], self.table_name)
            # else:
            #     print("No data to process")
        else:
            print("No table structure found in response")
