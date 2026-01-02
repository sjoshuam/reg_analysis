'''Calculate embeddings and statistics from regulatory section text data'''

## IMPORTS AND SETTINGS
import os, datetime, numpy as np

import torch
assert torch.cuda.is_available(), "ERROR:  torch did not connect to CUDA"
torch.cuda.empty_cache()
from sentence_transformers import SentenceTransformer

from pyspark.sql import SparkSession
import pyspark.sql.functions as psf
import pyspark.sql.types as pst
from pyspark.ml.linalg import VectorUDT, Vectors

spark = SparkSession.builder\
    .appName('EmbedData')\
    .config('spark.executor.memory', '8g')\
    .config('spark.driver.memory', '8g')\
    .getOrCreate()

## DEFINE CLASS

class EmbedData:
    '''Calculate emeddings and statistics from regulation text'''

    def __init__(self, test_mode=False, focal_year=2024) -> None:

        # set attributes
        self.test_mode = test_mode
        self.focal_year = focal_year
        self.checklist = {i:False for i in [
            'read_data', 'make_embeddings',
        ]}

        # Ensure embedding model is present
        if not os.path.exists('.legal-bert-base-uncased'):
            x = SentenceTransformer('nlpaueb/legal-bert-base-uncased')
            x.save('.legal-bert-base-uncased')
            del x

    def __str__(self) -> str:
        '''Print status information about class'''

        # Display execution status
        status = [f'{i}: {self.checklist[i]}' for i in self.checklist.keys()]
        status = ['\n==== QuantifyData ================', '--Status--------'] + status

        # Data counts
        status += ['-- Data Counts --------']
        status += [f'Sections (Total): {self.section_count}']
        status += [f'Sections (Now): {self.section_count_now}']
        status += [f'Parts (Total): {self.part_count}']
        status += [f'Parts (Now): {self.part_count_now}']
        status = "\n".join(status)

        return status
    
    def read_data(self) -> None:
        '''Read in refined regulatory text data'''

        # Read in data
        self.part_data = spark.read.parquet('a_in/cfr_parsed_part')
        self.section_data = spark.read.parquet('a_in/cfr_parsed_section')
        self.section_data = self.section_data.repartition('title_id')

        # for test mode, filter to a subset of the dataset (300,000 sections -- about 20% of the total)
        if self.test_mode:
            print(('!'*16) + ' WARNING: RUNNING IN TEST MODE ' + ('!'*16))
            self.section_data = self.section_data.limit(100*1000)
        self.section_data = self.section_data

        # Do checklists and tallies
        self.section_count = self.section_data.count()
        self.part_count = self.part_data.count()
        self.section_count_now = self.section_data.where(self.section_data['title_id'].rlike(str(self.focal_year))).count()
        self.part_count_now = self.part_data.where(self.part_data['title_id'].rlike(str(self.focal_year))).count()
        self.checklist['read_data'] = True
        return None

    def make_embeddings(self) -> None:
        '''Calculate embeddings for each regulatory section'''

        # make hashes to represent text (for joining later)
        self.section_data = self.section_data.withColumn(
            'section_hash', psf.substring(psf.sha2(psf.col('section_text'), 256), 1, 128))
        
        # load embedding model
        sent_trans = SentenceTransformer('./.legal-bert-base-uncased', device='cuda')

        # section - calculate embeddings
        embedded_text = self.section_data\
            .filter(psf.col('title_id').rlike(str(self.focal_year)))\
                .select(['title_id', 'part_id', 'section_hash', 'section_text'])\
                .collect()
        section_hash = [i['section_hash'] for i in embedded_text]
        part_id = [(i['title_id'], i['part_id']) for i in embedded_text]
        embedded_text = [i['section_text'] for i in embedded_text]
        embedded_text = sent_trans.encode(embedded_text, batch_size=int(2**5))
        embedded_text = np.array(embedded_text, dtype=np.float32).astype(np.float16)
        embedded_text = embedded_text.tolist()

        ## part - calculate average of section vectors
        part_vectors = {}
        for i in range(len(part_id)):
            pid = part_id[i]
            if pid not in part_vectors:
                part_vectors[pid] = []
            part_vectors[pid].append(embedded_text[i])
        for i in part_vectors.keys():
            part_vectors[i] = np.mean(part_vectors[i], axis=0).tolist()
        del part_id

        # part - merge part vectors into part data
        schema = pst.StructType([
            pst.StructField('title_id', pst.StringType(), False),
            pst.StructField('part_id', pst.StringType(), False),
            pst.StructField('part_embedding', VectorUDT(), False),
        ])
        part_vectors = [(i[0], i[1], Vectors.dense(part_vectors[i])) for i in part_vectors.keys()]
        part_vectors = spark.createDataFrame(part_vectors, schema=schema)
        part_vectors = part_vectors.repartition('title_id')
        self.part_data = self.part_data.join(part_vectors, on=['title_id', 'part_id'], how='left')

        del part_vectors

        # section - reshape embeddings to a join-ready state
        schema = pst.StructType([
            pst.StructField('section_hash', pst.StringType(), False),
            pst.StructField('section_embedding', VectorUDT(), False),
        ])
        embedded_text = [Vectors.dense(i) for i in embedded_text]
        #embedded_text = [i.tolist() for i in embedded_text]
        embedded_text = spark.createDataFrame(list(zip(section_hash, embedded_text)), schema=schema)\
            .repartition(self.section_data.count() // 1000) # repartition change
        del section_hash

        # section - join embeddings back to section data
        embedded_text = self.section_data.drop('section_text')\
            .join(psf.broadcast(embedded_text), on=['section_hash'], how='left') # prevents oversized task
        embedded_text = embedded_text.repartition('title_id') # repartition change

        # write files to disk
        self.part_data.write.mode('overwrite').parquet('a_in/cfr_embedded_part')
        embedded_text.write.mode('overwrite').parquet('a_in/cfr_embedded_section')

        # finalize
        self.checklist['make_embeddings'] = True
        return None
  
    def embed_data(self) -> None:
        '''Calculate embeddings for regulation text'''
        if not self.checklist['read_data']: self.read_data()
        if not self.checklist['make_embeddings']: self.make_embeddings()
        return None

# Test class
if __name__ == '__main__':

    # Quantify data (m3_quantify_data.py)
    start_time = datetime.datetime.now()
    embed_data = EmbedData(test_mode=False)
    embed_data.embed_data()
    print('Elapsed time:', datetime.datetime.now() - start_time)


    #pyspark --conf "spark.driver.extraJavaOptions=-Dlog4j.configurationFile=/home/sjoshuam/code/log4j2.properties"


##########==========##########==========##########==========##########==========##########==========##########==========
