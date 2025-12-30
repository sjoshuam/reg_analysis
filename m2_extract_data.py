'''2_extract_data.py: Parse downloaded xml files'''

## LIBRARIES AND SETTINGS
import gzip, os
import xml.etree.ElementTree as xml_et

import pyspark
import pyspark.sql.types as pst
import pyspark.sql.functions as psf
spark = pyspark.sql.SparkSession.builder.appName('ExtractData').config('spark.executor.memory', '8g') \
    .config('spark.driver.memory', '8g').getOrCreate()

from m1_import_data import ImportData

## DEFINE CLASS

class ExtractData:
    '''Parse downloaded xml files'''

    def __init__(self, input_dir:str='a_in/cfr_raw', output_dir:str='a_in/cfr_parsed') -> None:
        '''Initialize the ExtractData class'''

        # Save parameters
        self.input_dir = input_dir
        self.output_dir = output_dir

        # create slot of roster of output files
        self.roster = []

        # Checklist
        self.checklist = {i:False for i in ['make_roster', 'refine_data_from_files', 'compile_dataset']}
        if not os.path.exists(output_dir): os.makedirs(output_dir)

    def __str__(self) -> str:
        '''Print status information about class'''

        # Display execution status
        status = [f'{i}: {self.checklist[i]}' for i in self.checklist.keys()]
        status = ['\n==== ImportData ================', '--Status--------'] + status
        status = "\n".join(status)

        return status
    
    @staticmethod
    def extract_sections_from_title(title_file:str, input_dir:str, output_dir:str) -> dict:
        '''Extract all sections (DIV8) and appendices (DIV9) from a given Title (DIV5)'''

        ## define helper functions and settings
        @staticmethod
        def get_text(element) -> str:  #extract text from xml element
            if element is None: return None
            else: return ' '.join(element.itertext()) 
        
        @staticmethod
        def set_partitions(n): #set number of partitions based on dataframe size
            return min(max(n//10, 16), 1024)

        ## extract elements
        with gzip.open(os.path.join(input_dir, title_file), 'rb') as file: part = file.read()
        title_label = title_file.replace('.xml.gzip','')

        xml_data = xml_et.fromstring(part)
        xml_data = xml_data.findall('.//DIV5')

        ## PARSE PARTS ########

        # parse parts (div5) into tuples
        part = []
        for i in range(0, len(xml_data)):
            part.append((
                title_label,
                xml_data[i].attrib['TYPE'] +'-'+ xml_data[i].attrib['N'].zfill(4),
                get_text(xml_data[i].find('.//HEAD')),
                get_text(xml_data[i].find('.//AUTH')),
            ))

        # convert Parts to pyspark dataframe and save
        part_schema = pst.StructType([
            pst.StructField('title_id', pst.StringType(), False),
            pst.StructField('part_id', pst.StringType(), False),
            pst.StructField('part_heading', pst.StringType(), True),
            pst.StructField('part_authority', pst.StringType(), True),
            ])
        part = spark.sparkContext.parallelize(part, numSlices=set_partitions(len(part)))
        part = spark.createDataFrame(part, schema=part_schema)

        # write part file to disk
        part.write.mode('overwrite').parquet(os.path.join(output_dir, title_label + '-Part'))
        del part

        ## PARSE SECTIONS ########

        # parse sections (div8) and appendices (div9) within parts
        section = []
        while len(xml_data) > 0:
            part_sections = xml_data.pop(0)
            part_sections = part_sections.findall('.//DIV8') + part_sections.findall('.//DIV9')
            part_sections = [
                (title_label, i.attrib['TYPE'] +'-'+ i.attrib['N'], get_text(i)) for i in part_sections]
            section += part_sections
        del part_sections

        # convert Sections to dataframe
        section_schema = pst.StructType([
            pst.StructField('title_id', pst.StringType(), False),
            pst.StructField('section_id', pst.StringType(), False),
            pst.StructField('section_text', pst.StringType(), True),
            ])
        section = spark.sparkContext.parallelize(section, numSlices=set_partitions(len(section)))
        section = spark.createDataFrame(section, schema=section_schema)

        # divide sections and appendices
        section = section.withColumnRenamed('section_id', 'section_name')
        section = section.withColumn(
            'section_type',
            psf.split(
                psf.col('section_name'), '[-]'
                ).getItem(0)
            )
        section = section.withColumn(
            'section_name',
            psf.split(
                psf.col('section_name'), '[-]'
                ).getItem(1)
            )

        appendix = section.filter(psf.col('section_type') == 'APPENDIX')
        section  = section.filter(psf.col('section_type') == 'SECTION')

        # refine ids for appendices
        appendix = appendix.withColumn(
            'part_id',
            psf.regexp_replace(
                psf.regexp_replace(psf.col('section_name'), '.* to ', ''),
                '.*Part ', ''
                )
            )
        appendix = appendix.withColumn(
            'part_id',
            psf.concat(
                psf.lit('Part-'), psf.lpad(psf.col('part_id'),4,'0')
                )
            )
        appendix = appendix.withColumn(
            'section_id',
            psf.regexp_replace(
                psf.col('section_name'),
                ' to .*', ''
                )
            )
        appendix = appendix.withColumn(
            'section_id',
            psf.regexp_replace(
                psf.col('section_id'),
                r"Appendix |Appendixes ", 'Appendix-'
                )
            )
        
        # refine ids for sections
        section = section.withColumn(
            'section_name',
            psf.when(
                psf.col('section_name').rlike('[.]'),
                psf.col('section_name')
                ).otherwise(
                    psf.concat(
                        psf.lit('0.'), psf.col('section_name')
                        )
                    )
            )
        section = section.withColumn(
            'part_id',
            psf.concat(
                psf.lit('Part-'),
                psf.lpad(
                    psf.regexp_replace(psf.col('section_name'),'[.].+$',''),
                    4,'0')
                )
            )
        section = section.withColumn(
            'section_id',
            psf.concat(
                psf.lit('Section-'),
                psf.lpad(
                    psf.regexp_replace(psf.col('section_name'),'^.+[.]',''),
                    4,'0')
                )
            )

        # unify section and appendix dataframes
        section = section.unionByName(appendix)
        section = section.sort(['part_id', 'section_type', 'section_id'], ascending=[True, False, True])
        del appendix

        # write section file
        out_file = os.path.join(output_dir, title_label + '-Section')
        section.write.mode('overwrite').parquet(out_file)
        return section
    
    def make_roster(self) -> list:
        '''Create roster of title-year datafiles in input directory'''

        # Define helper function
        def inventory_directory(dir:str, ext:str, label:str) -> pyspark.sql.DataFrame:
            '''Inventory files in a given directory with a given extension and label'''
            files = [(i.replace(ext, ''), True) for i in os.listdir(dir) if i.endswith(ext) and i.startswith('Title')]
            schema = pst.StructType([
                pst.StructField('title_id', pst.StringType(), False),
                pst.StructField('file', pst.BooleanType(), False),
                ])
            files = spark.createDataFrame(files, schema=schema).sort('title_id').withColumnRenamed('file', label)
            return files

        # inventory files
        files = inventory_directory(dir=self.input_dir, ext='.xml.gzip', label='raw')
        parsed_files = inventory_directory(dir=self.output_dir, ext='-Section.parquet', label='parsed')
        files = files.join(parsed_files, on='title_id', how='left').sort('title_id').na.fill(False, subset=['parsed'])

        # update checklist and output
        self.checklist['make_roster'] = True
        self.roster = files
        return files
    
    def refine_data_from_files(self) -> pyspark.sql.DataFrame:
        '''Refine data from title-year datafiles in input directory'''

        # Ensure roster is created
        if not self.checklist['make_roster']: self.make_roster()

        # iterate through roster and extract data
        lines_read = []
        for i in self.roster.toLocalIterator():
            if not i['parsed']:
                try:
                    out_file = ExtractData.extract_sections_from_title(
                        title_file = i['title_id'] + '.xml.gzip', input_dir = self.input_dir, output_dir = self.output_dir)
                    lines_read.append((i['title_id'], out_file.count()))
                    del out_file
                except Exception as e:
                    print(f'Error parsing {i["title_id"]}: {e}')
                    lines_read.append((i['title_id'], -1))
            else:
                lines_read.append((i['title_id'], 0))

        # update roster
        lines_read_schema = pst.StructType([
            pst.StructField('title_id', pst.StringType(), False),
            pst.StructField('new_lines', pst.IntegerType(), True),
            ])
        lines_read = spark.createDataFrame(lines_read, schema=lines_read_schema)
        self.roster = self.roster.join(lines_read, on='title_id', how='left')
        self.roster = self.roster.withColumn('parsed', psf.when(psf.col('new_lines') != -1, True).otherwise(False))

        # checklist and conclusions
        self.checklist['refine_data_from_files'] = True
        return self.roster
    
    def compile_dataset(self) -> None:
        '''Compile all refined data to one parquet file'''
        for i in ['Section', 'Part']:
            pattern = self.output_dir + f'/Title*-{i}/*.parquet'
            df = spark.read.parquet(pattern)
            df = df.repartition(df.count()//100)
            print(i, ' count= ', df.count(), ' partitions= ', df.rdd.getNumPartitions())
            df.write.mode('overwrite').parquet(os.path.join(self.output_dir+f'_{i.lower()}'))
        self.checklist['compile_dataset'] = True
        os.system(f'rm -rf {self.output_dir}/*')
        return None

    def extract_data(self) ->  None:
        '''Extract full data extraction pipeline'''
        if not self.checklist['make_roster']: self.make_roster()
        if not self.checklist['refine_data_from_files']: self.refine_data_from_files()
        if not self.checklist['compile_dataset']: self.compile_dataset()
        return self.roster

## TEST EXECUTE CODE
if __name__ == '__main__':

    # Import data (m1_import_data.py)
    imported_data = ImportData()
    imported_data.import_data(query_server=False)
    print(imported_data)

    # Extract data (m2_extract_data.py)
    extracted_data = ExtractData()
    extracted_data.extract_data()
    

    ##########==========##########==========##########==========##########==========##########==========##########==========
