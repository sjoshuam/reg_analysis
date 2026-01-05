#Project: Code of Federal Regulations (CFR) Analysis

Only Congress can pass laws, but Congress typically delegates rule-making authority to federal agencies, which have the expertise to implement and enforce those laws.
The United States Code of Federal Regulations (CFR) is the official legal compilation for the rules federal agencies issue.
This project searches for CFR sections that may be candidates for simplification or removal.

You can view the final product from this project [on this webpage.](https://cfr-analysis-sjoshuam.plotly.app) Scroll to the bottom of the page to see more details on the project's methodology.  In brief, the project consists of five sequential models:

+ m1_import_data.py - Downloads the end-of-year version of the CFR from the eCFR API for every year between 2017 and 2024.
+ m2_extract_data.py - Extracts all CFR sections (div8 & div9 tags) from all parts and appendices (div5 tags) in the raw XML files.
+ m3_embed_data.py - Applies SBERT text embeddings to represent each section's meaning as mathematical coordinates.
+ m4_quantify_data.py - Uses PySpark and NumPy to quantify key aspects of the CFR.
+ m5_display_data.py - Builds a mildly interactive data dashboard using Plotly's Dash framework.

The project relies on three sub-directories:
+ a_in - The early steps of the pipeline download or make large amounts of raw data.  All 6+ Gb of it gets stored in here.
+ b_io - The final data products from this pipeline go here at the end of m4.  12Mb total.
+ c_out - This contains everything needed to deploy the data dashboard app specified in m5.  This includes copies of the data files from b_io, the m5 script, and a simplifed Python package requirements file.

## Development Environment

I developed this code in a environment with specifications below and have not cross-tested it elsewhere:

+ 16+ GB RAM
+ CUDA 13.1 (for GPU processing)
+ Ubuntu 24.04
+ Java 21 (for PySpark processing)
+ Python 3.12
+ The packages specified in the requirements.txt file.

setup.sh may help you in recreating this environment, but you should treat it as recommendations.  It has not been tested and hardened enough to be executed end-to-end.
