'''Download raw data from source'''

## LIBRARIES AND SETTINGS
import requests, os, datetime, time, gzip

## CLASS DEFINITION

class ImportData:
    '''Download raw data from source'''

    def __init__(self, output_dir:str='a_in/cfr_raw', years:tuple=None, titles:tuple=None) -> None:
        '''Initialize the ImportData class'''

        # Validate desired years and store
        self.current_year = datetime.datetime.now().year
        self.years = years if years is not None else (2017, self.current_year)
        is_valid = (min(self.years) >= 2017) and (max(self.years) <= self.current_year)
        assert is_valid, f'Valid year range: 2017-{self.current_year}'

        # Validate and store desired CFR Titles
        self.titles = titles if titles is not None else (1,50)
        assert (min(self.titles) >= 1) and (max(self.titles) <= 50), 'Valid CFR titles: 1-50'

        # Store output directory
        self.output_dir = output_dir

        # Make checklist
        self.checklist = {i:False for i in ['make_roster', 'download_data']}


    def make_roster(self):
        '''Create a roster of data files to download'''

        # Ensure output directory exists
        if not os.path.exists(self.output_dir): os.makedirs(self.output_dir)

        # Create roster of desired data files
        roster = {}
        for i_year in range(self.years[0], self.years[1] + 1):
            for i_title in range(self.titles[0], self.titles[1] + 1):
                i_file_name = f'Title-{str(i_title).zfill(2)}-{str(i_year)}.xml.gzip'
                i_file_path = os.path.join(self.output_dir, i_file_name)
                roster.update({(i_title, i_year):{
                    'year': i_year, 'title': i_title, 'filename': i_file_name, 'file_path': i_file_path,
                    'url': f'https://www.ecfr.gov/api/versioner/v1/full/{str(i_year)}-12-31/title-{str(i_title)}.xml',
                    'status': 'Present' if os.path.exists(i_file_path) else 'Absent'
                }})
        
        # Conclude make_roster
        self.roster = roster
        self.checklist['make_roster'] = True
        return roster
    
    def download_data(self):
        '''Download data files as per the roster'''

        # Ensure roster is created
        if not self.checklist['make_roster']: self.make_roster()

        # Download files as needed to complete roster
        for i in self.roster.keys():
            if self.roster[i]['status'] == 'Absent':
                time.sleep(1)
                api_response = requests.get(self.roster[i]['url'])
                if api_response.status_code == 200:
                    with gzip.open(self.roster[i]['file_path'], 'wt') as conn: conn.write(api_response.text)
                    self.roster[i]['status'] = 'Acquired'
                else: 
                    self.roster[i]['status'] = 'Failed'

        # Conclude download_data
        self.checklist['download_data'] = True
        return self.roster
    
    def import_data(self):
        '''Execute full data import process'''
        if not self.checklist['make_roster']: self.make_roster()
        if not self.checklist['download_data']: self.download_data()
        return self.roster
    

## TEST EXECUTION
if __name__ == '__main__':
    imported_data = ImportData()
    imported_data.import_data()
    for i in imported_data.roster.keys():
        print(i, imported_data.roster[i]['status'])

##########==========##########==========##########==========##########==========##########==========##########==========
