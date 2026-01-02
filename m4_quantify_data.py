'''Calculate useful statistics for regulation text and embeddings'''

## IMPORTS AND SETTINGS
import datetime, numpy as np, pandas as pd

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

spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

## DEFINE MODULE CLASS

class QuantifyData:

    def __init__(self, focal_year=2024, deviants_n=1):
        self.focal_year = focal_year
        self.section_text_count = None
        self.section_embeddings_count = None
        self.part_data_count = None
        self.deviants_n = deviants_n

    def __str__(self) -> str:
        return 'QuantifyData class. TODO: make a real print method'

    def read_data(self):
        '''Read in refined regulatory text and embedding data'''

        # read in datasets
        self.section_embeddings = spark.read.parquet('a_in/cfr_embedded_section').repartition('title_id')
        self.section_embeddings_count = self.section_embeddings.count()
        self.section_text = spark.read.parquet('a_in/cfr_parsed_section').repartition('title_id')
        self.section_text_count = self.section_text.count()
        self.part_data = spark.read.parquet('a_in/cfr_embedded_part').repartition('title_id')
        self.part_data_count = self.part_data.count()

        return None
    
    
    def make_section_statistics(self):
        '''Measure deviance of section embeddings from part average'''

        # join part embeddings to section embeddings
        self.section_embeddings = self.section_embeddings.join(
            self.part_data.select(['title_id', 'part_id', 'part_embedding']), how='left', on=['title_id', 'part_id'])

        # calculate cosine distance between each section embedding and its part average embedding
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
            cosine_distance = ((cosine_similarity * -1.0) + 1.0) / 2.0
            return round(cosine_distance, 6)
        udf_cosine_distance = psf.udf(udf_cosine_distance, pst.FloatType()) #

        self.section_embeddings = self.section_embeddings\
            .withColumn('section_deviance', udf_cosine_distance(psf.col('section_embedding'), psf.col('part_embedding')))
        
        # count words in each section
        self.section_text = self.section_text.withColumn(
            'section_name',
            psf.when(psf.col('section_type') == 'SECTION', psf.concat_ws(' ', psf.lit('§'), psf.col('section_name')))\
                .otherwise(psf.col('section_name'))
            )
        self.section_text = self.section_text.withColumn(
            'section_text',
            psf.regexp_replace( psf.regexp_replace(
                psf.col('section_text'),
                ' *\\n *', ' '), psf.col('section_name'), '')
                )
        self.section_text = self.section_text.withColumn(
            'section_word_count', psf.size(psf.split(psf.col('section_text'), r'\s+')))
        
        # join text and word counts to embeddings object
        self.section_embeddings = self.section_embeddings.join(
            self.section_text.select(['title_id', 'part_id', 'section_id', 'section_text', 'section_word_count']),
            how='left', on=['title_id', 'part_id', 'section_id'])
        
        return None


    def note_deviant_extremes(self):
        '''Identify sections that are most/least deviant from part embedding'''

        # define windows to find the least/most deviant sections per part
        windows = {
            'least_deviant': Window.partitionBy('title_id', 'part_id').orderBy(psf.col('section_deviance').asc()),
            'most_deviant': Window.partitionBy('title_id', 'part_id').orderBy(psf.col('section_deviance').desc())
        }

        # find three least/most deviant sections per part
        for i in windows:
            self.section_embeddings = self.section_embeddings.withColumn('deviance_rank', psf.row_number().over(windows[i]))
            self.section_embeddings = self.section_embeddings.withColumn(
            i, psf.when(psf.col('deviance_rank') <= self.deviants_n, psf.lit(True)).otherwise(psf.lit(False)))

        # condense columns
        self.section_embeddings = self.section_embeddings.drop('deviance_rank')
        self.section_embeddings = self.section_embeddings.withColumn(
            'notable_deviant', 
                psf.when(psf.col('least_deviant'), psf.lit('Least'))\
                    .when(psf.col('most_deviant'), psf.lit('Most'))\
                    .when(psf.col('least_deviant') & psf.col('most_deviant'), psf.lit('least_deviant'))\
                    .otherwise(psf.lit('Neither')))\
            .drop('least_deviant', 'most_deviant')
        
        return None
    

    def make_part_statistics(self):
        '''Calculate average deviance for sections within each part'''

        # calculate average deviance per part and section count
        deviance_mean = self.section_embeddings.groupBy('title_id', 'part_id')\
            .agg(
                psf.round(psf.avg('section_deviance'), 6).alias('deviance_mean'),
                psf.count('section_id').alias('section_count')
                )\
            .select('title_id', 'part_id', 'deviance_mean', 'section_count')
        self.part_data = self.part_data.join(deviance_mean, on=['title_id', 'part_id'], how='left')
        del deviance_mean

        # count authorities
        self.part_data = self.part_data.withColumn(
            'part_authority',
            psf.regexp_replace( psf.regexp_replace( psf.regexp_replace( psf.regexp_replace(
                        psf.col('part_authority'),
                        ' *\\n *', ''), 'Authority: ', ''), ', and ', ', '), '.$', '')
            )
        self.part_data = self.part_data.withColumn(
            'authority_count', psf.size(psf.split(psf.col('part_authority'), ', ')))
        
        # sum part word counts from section word counts
        word_counts = self.section_embeddings.groupBy('title_id', 'part_id')\
            .agg(psf.sum('section_word_count').alias('part_word_count'))\
            .select('title_id', 'part_id', 'part_word_count')
        self.part_data = self.part_data.join(word_counts, on=['title_id', 'part_id'], how='left')
        del word_counts

        # calculate fraction of non-null hashes for each section in parts
        self.section_embeddings = self.section_embeddings.withColumn(
            'focal_year_match', psf.when(psf.col('section_embedding').isNotNull(), psf.lit(1)).otherwise(psf.lit(0)))
        matches_focal_year = self.section_embeddings\
            .groupBy('title_id', 'part_id')\
            .agg(psf.mean('focal_year_match').alias('focal_year_match'))
        self.part_data = self.part_data.join(matches_focal_year, on=['title_id', 'part_id'], how='left')

        return None


    def export_section_data(self):
        '''Export section-wise data to Excel'''

        # export notable sections for focal year
        notable_sections = self.section_embeddings.filter(
            (psf.col('notable_deviant') != 'Neither'))\
            .filter(psf.col('title_id').rlike('-' + str(self.focal_year)))\
            .select('title_id', 'part_id', 'section_id', 'section_name', 'section_text', 'section_deviance', 'notable_deviant')\
            .withColumn('year_id', psf.regexp_extract(psf.col('title_id'), r'-(\d{4})$', 1))\
            .withColumn('title_id', psf.regexp_replace(psf.col('title_id'), r'-\d{4}$', ''))
        print('[NOTABLE SECTION COUNT] ', notable_sections.count())
        notable_sections.toPandas().to_excel("b_io/notable_sections.xlsx", index=False)
        del notable_sections

        # TODO: export section embeddings as a prep step for UMAP visualization

        return None
    
    def export_part_data(self):
        '''Export part-wise data to Excel'''

        # separate out year id; drop embedding column
        self.part_data = self.part_data.withColumn('year_id', psf.regexp_extract(psf.col('title_id'), r'-(\d{4})$', 1))
        self.part_data = self.part_data.withColumn('title_id', psf.regexp_replace(psf.col('title_id'), r'-\d{4}$', ''))
        self.part_data = self.part_data.drop('part_embedding')

        # separate out focal year only components
        part_data_focal = self.part_data\
            .filter(psf.col('year_id') == str(self.focal_year))\
            .select('year_id', 'title_id', 'part_id', 'part_heading', 'part_authority', 'deviance_mean')
        part_data_focal.toPandas().to_excel("b_io/part_data_focal.xlsx", index=False)
        
        # filter time series data
        part_data_temporal = self.part_data\
            .select('year_id', 'title_id', 'part_id', 'section_count', 'authority_count', 'part_word_count', 'focal_year_match')\
            .join(
                part_data_focal.select('title_id', 'part_id'),
                on=['title_id', 'part_id'],
                how='left_semi'
            )
        part_data_temporal.toPandas().to_excel("b_io/part_data_temporal.xlsx", index=False)


    # -- EXECUTE ALL FUNCTIONS --------
    def quantify_data(self) -> None:
        '''Calculate useful statistics for regulation text and embeddings'''
        quantify_data.read_data()
        quantify_data.make_section_statistics()
        quantify_data.note_deviant_extremes()
        quantify_data.make_part_statistics()
        quantify_data.export_section_data()
        quantify_data.export_part_data()

    pass

## TEST EXECUTION

if __name__ == '__main__':

    # Quantify data 
    start_time = datetime.datetime.now()
    quantify_data = QuantifyData()
    quantify_data.quantify_data()
    print('Elapsed time:', datetime.datetime.now() - start_time)


##########==========##########==========##########==========##########==========##########==========##########==========
