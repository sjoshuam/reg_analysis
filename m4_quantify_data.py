'''Calculate useful statistics for regulation text and embeddings'''

## IMPORTS AND SETTINGS
import datetime, numpy as np

from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as psf
import pyspark.sql.types as pst
import pyspark.ml.functions as pmf
import pyspark.ml.linalg as pml

spark = SparkSession.builder\
    .appName('QuantifyData')\
    .config('spark.executor.memory', '8g')\
    .config('spark.driver.memory', '8g')\
    .getOrCreate()

## DEFINE MODULE CLASS

class QuantifyData:

    def __init__(self, focal_year=2024):
        self.focal_year = focal_year

    def __str__(self) -> str:
        return 'QuantifyData class. TODO: make a real print method'

    def read_data(self):
        '''Read in refined regulatory text and embedding data'''

        # read in datasets
        self.section_embeddings = spark.read.parquet('a_in/cfr_embedded_section')
        self.section_text = spark.read.parquet('a_in/cfr_parsed_section')
        self.part_data = spark.read.parquet('a_in/cfr_embedded_part')

        # TECH DEBT:  Fix discovered tech debt
        print('TECH DEBT:  Fix this the right way in m3')
        temp = self.part_data\
            .filter(psf.col('part_embedding').isNotNull())\
            .select(['title_id', 'part_id', 'part_embedding'])
        temp = temp.withColumn('part_embedding', pmf.array_to_vector(temp["part_embedding"]))
        self.part_data = self.part_data.drop('part_embedding')\
            .join(temp, on=['title_id', 'part_id'], how='left')
        del temp

        return None
    
    # -- CALCULATE VARIANCE STATISTICS --------
    
    def measure_section_deviance(self): #'calc_section_deviance', 
        '''Measure deviance of section embeddings from part average'''

        # join part embeddings to section embeddings
        self.section_embeddings = self.section_embeddings.join(
            self.part_data.select(['title_id', 'part_id', 'part_embedding']), how='left', on=['title_id', 'part_id'])

        # define udf to calculate cosine distance
        @staticmethod
        def udf_cosine_distance(section_embed, part_embed):
            '''Calculate cosine distance between section and part embeddings'''

            # set up variables
            if isinstance(section_embed, pml.DenseVector) and isinstance(part_embed, pml.DenseVector): pass
            else: return None

            # calculate  dot product and norms
            dot_product = float(section_embed.dot(part_embed))
            section_norm = float(pml.Vectors.norm(section_embed, 2))
            part_norm = float(pml.Vectors.norm(part_embed, 2))
            if section_norm == 0.0 or part_norm == 0.0: return None

            # calculate cosine distance
            cosine_similarity = dot_product / (section_norm * part_norm)
            cosine_distance = 1.0 - cosine_similarity
            return cosine_distance
        
        udf_cosine_distance = psf.udf(udf_cosine_distance, pst.FloatType()) #

        # Calculate section's deviance from part average embedding
        self.section_embeddings = self.section_embeddings\
            .withColumn('section_deviance', udf_cosine_distance(psf.col('section_embedding'), psf.col('part_embedding')))
        return None

    def find_variance_outliers(self): # 'find_variance_outliers',
        '''Identify sections with unusually high deviation from part'''

        # calculate mean and stddev of section deviance by title
        mean_deviance = self.section_embeddings.groupby('title_id').agg(
            psf.mean('section_deviance').alias('mean'),
            psf.stddev('section_deviance').alias('stdev')
            )
        self.section_embeddings = self.section_embeddings.join(mean_deviance, on=['title_id'], how='left')
        
        # define z-score udf
        @staticmethod
        def udf_z_score(deviance, mean, stdev):
            '''Calculate z-score for section deviance'''
            if deviance is None or mean is None or stdev is None or stdev == 0.0:return None
            z = (deviance - mean) / stdev
            return round(z, 3)
        udf_z_score = psf.udf(udf_z_score, pst.FloatType())

        # calculate z-score for each section
        self.section_embeddings = self.section_embeddings\
            .withColumn('deviance_zscore', udf_z_score(psf.col('section_deviance'), psf.col('mean'), psf.col('stdev')))\
            .drop('mean', 'stdev')
        
        return None

    def least_deviant_section(self): # 'find_min_deviance_from_part', 
        '''Identify sections that are most representative of their part'''

        # calculate minimum deviance by part
        self.section_embeddings = self.section_embeddings\
            .withColumn('least_deviant', psf.min('section_deviance').over(Window.partitionBy(['title_id', 'part_id'])))
        self.section_embeddings = self.section_embeddings\
            .withColumn('least_deviant', psf.when(psf.col('section_deviance') == psf.col('least_deviant'), True)\
            .otherwise(False))
        self.section_embeddings = self.section_embeddings\
            .withColumn('least_deviant', psf.when(psf.col('deviance_zscore').isNull(), None)\
            .otherwise(psf.col('least_deviant')))
        return None

    def measure_part_variance(self): # 'calc_part_variance',
        '''Calculate standard deviation for sections within each part'''
        pass #TODO

    # -- CALCULATE WORD COUNT STATISTICS --------

    def count_section_words(self): #'count_sec_words', 
        '''Count word counts in each regulatory section'''
        pass #TODO
    
    def count_part_authority(self): #'count_part_authority', 
        '''Count word counts in each regulatory section'''
        pass #TODO

    def sum_word_counts_to_part(self): #'sum_word_count_to_part',
        '''Sum section word counts to part level'''
        pass #TODO

    # -- RECOMMEND SECTION CHANGES --------

    def best_fit_part(self): # KNN
        '''Use KNN to find best-fit part for each section'''
        pass #TODO


    # -- EXECUTE ALL FUNCTIONS --------
    def quantify_data(self) -> None:
        '''Calculate useful statistics for regulation text and embeddings'''





    pass


## TEST EXECUTION

if __name__ == '__main__':

    # Embed data (m3_embed_data.py)
    start_time = datetime.datetime.now()
    quantify_data = QuantifyData()
    quantify_data.read_data()
    quantify_data.measure_section_deviance()
    quantify_data.find_variance_outliers()
    quantify_data.least_deviant_section()
    print('Elapsed time:', datetime.datetime.now() - start_time)


##########==========##########==========##########==========##########==========##########==========##########==========
