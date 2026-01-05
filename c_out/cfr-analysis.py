import dash, os, pandas as pd
from dash import html

##########==========##########==========##########==========##########==========##########==========##########==========
########## Long Text and Basic Global Style Paramters ======##########

page_colors = {
    'bg':  '#cfdae6', # (210,0.1,0.9)
    'text':'#030e1a', # (210,0.9,0.1)
    'mg':  '#406080', # (210,0.5,0.5)
    'fg':  '#08111a', # (210,0.7,0.1)
    'void': '#ffffff', # white
}

page_text = {
    "Intro-Context": "  ".join([
        "Only Congress can pass laws, but Congress typically delegates rule-making authority to federal agencies, which have the expertise to implement and enforce those laws.",
        "The United States Code of Federal Regulations (CFR) is the official legal compilation for the rules federal agencies issue.",
        "In 2024, the CFR consisted of 50 titles, which contain 9,359 parts, which contain 403,922 sections, which contain about 300 million words of regulations.",
        "Detailed regulation is likely unavoidable because of the complexity and scale of a modern country, especially a high-capability country like the United States.",
        "However, the sheer volume of regulation can burden individuals and businesses, detracting from the intended societal benefits of the rules.",
        ]),
    "Intro-Theory":"  ".join([
        "This project searches for CFR sections that may be candidates for simplification or removal.",
        "Rules are often added to address specific situations, which may lead to similar rules appearing in different places within the CFR.",
        "My theory is that incomplete consolidation of similar rules may be a contributing factor to regulatory bulk.",
        "In this project, I measure how well each section fits with others sections in the same part.",
        "Parts with consistently similar sections may be well-consolidated.",
        "Conversely, parts with dissimilar sections may indicate opportunities for reorganization and consolidation across sections.",

        ]),
    "Intro-Approach":"  ".join([
        "Text embeddings represent the meaning of text as coordinates in a mathematical space.",
        "They are a core technology underlying AI language models like ChatGPT.",
        "In this project, I use text embeddings to measure the semantic dissimilarity between each CFR section and the average section in its part.",
        "My dissimilarity score ranges from 0 (perfectly similar) to 0.5 (unrelated) to 1.0 (contradictory), but most sections fall between 0 and 0.1.",
        "For the technically inclined, my measure is a rescaled version of cosine distance, and my embedding model is Hugging Face's 'legal-bert-base-uncased' SBERT model.",
        "See Chapter III for more methodological details (bottom of the page).",
        ]),
    "Dissimilarity-Histogram-1":"  ".join([
        "The bar chart (right) shows the dissimilarity scores for all parts in the selected title.",
        "The height of each bar shows the dissimilarity score for a part.",
        "The selected part is highlighted in a darker shade.",
        "If the selected part has one of the higher scores (i.e., a tall bar), it may be a good candidate for review.",
        ]),
    "Dissimilarity-Histogram-2":"  ".join([
        "To illustrate what these scores mean substantively, select Title 31, Part 586.",
        "This part has a score of 0.41 (high for a CFR part) and concerns Chinese military-industrial complex sanctions.",
        "The part (1) implements an unusually large number of statutes, (2) opposes a highly-adaptive adversary, and (3) is more of a National Security matter despite being a Treasury responsibility.",
        "Fragmented legistration, wily opposition, and divided responsibility -- These are all risk factors for regulatory complexity.",
        ]),
    "Chapter-Part": "  ".join([
        "Use the dashboard panels below to explore individual CFR parts.",
        "Select a title and part from the dropdown menus, or leave them unchanged to explore 31 CFR 586.",
        ]),
    "Chapter-Temporal": "  ".join([
        "The CFR regulations evolve in response to legislative events, executive priorities, trends, and innovations, etc.",
        "The charts below show how the selected part changed over time and how part size relates to executive/legislative mandates.",
        "For an example, select 31 CFR 586.",
        "This part was introducted in 2022 to implement two executitive orders, which, in turn, address weaknesses in previous sanction legistration.",
        "The text has remained unchanged since them.",
        "Looking across Title 31 as a whole, we can see that parts with many authorities tend to have more sections, but parts with many sections may or may not cite many authorities.",
        "This suggests that legislative/executive mandates are one of the factors driving regulatory size, but not the only one."
        ]),
    "Chapter-Methodology": "  ".join([
        "This chapter overviews the basic steps I took to execute this project, and provides GitHub links to the relevant code.",
        "You can see my other non-work coding projects at my GitHub gallery page: ",
        ]),
    "Step-1": " ".join([
        " executes the first step of the project.",
        "It downloads the end-of-year version of the CFR from the eCFR API for every year between 2017 and 2024.",
        "The script can be stopped and started as needed; it only downloads files that are still missing from the a_in/cfr_raw directory.",
        "For a production pipeline, the script could be scheduled to run periodically to keep the data up to date.",
        "The key underlying technology is Python's built-in \"requests\" library, which handles the API REST interactions.",
        "During initial code development, the script successfully downloaded 981 Mb of data.",
        "Future development (high priority) will address server timeout issues when attempting to download the massive Title 40 files.",
        ]),
    "Step-2": " ".join([
        " executes the second step of the project.",
        "It extracts all CFR sections (div8 & div9 tags) from all parts and appendices (div5 tags) in the raw XML files using Python's built-in \"xml\" library.",
        "From there, the script parses the data into PySpark dataFrames, and cleans the data with extensive CPU parallelism.",
        "The final parquet files are stored in a_in/cfr_parsed_section, and a_in/cfr_parsed_part directories.",
        "During initial code development, the script successfully processed 981 Mb of raw xml files into 1.8 Gb of cleaned parquet files.",
        "Future development (low priority) will centralize pyspark partition management to squeeze a little extra performance out of minimizing cross-partition shuffles."
        ]),
    "Step-3": " ".join([
        " executes the third step of the project, applying SBERT text embeddings to represent each section's meaning as mathematical coordinates.",
        "It generates individual text embeddings for 1.5M CFR sections using a pre-trained transformer model.",
        "The model is 'Hugging Face's legal-bert-base-uncased' SBERT model, which is specifically designed for legal text.",
        "To accelerate the embedding calculations, the script executes Pytorch modeling with batching and CUDA-based GPU parallelism.",
        "The final parquet files are stored in a_in/cfr_embedded_section, and a_in/cfr_embedded_part directories, and total to 2.0 Gb of labeled vector data.",
        "Future development (medium priority) will implement cleaner separations between PySpark dataset operations and PyTorch vector operations, as PySpark's lazy evaluation can cause issues when mixed with heavy external computation steps.",
        ]),
    "Step-4": " ".join([
        " executes the fourth step of the project, using PySpark and NumPy to quantify key aspects of the CFR.",
        "Quantified aspects include:",
        "(1) Section and part word counts for each title in each year, totalling to roughly 300M words in each year's XML files.",
        "(2) Part authority counts for each title in each year, where an authority is a statute or executive order cited in a part's authority section.",
        "(3) Section counts for each part for each title and year, totaling to around 400k sections in each year's XML files.",
        "(4) Percentage of part's sections that are identical to the most recent downloaded year of text, as measured using exact text hash match.",
        "(5) Semantic dissimilarity scores comparing each section to typical sections in its part, as measured using cosine distance between text embeddings.",
        "In addition, the script identifies which sections are the most and least typical section in its part.",
        "The statistical abstracts are stored in the b_io directory, and efficiently characterize the data in just 12 Mb of Excel files.",
        "Future development (high priority) will calculate UMAP projections of text embeddings, so that the data dashboard can show semantic similarity between sections with intuitive scatterplot visualizations.",
        "Note: Of all my development to-dos for this project, I am the most excited about this one! Just didn't quite have enough time to implement it before the initial release...",
        ]),
    "Step-5": " ".join([
        " executes the fifth step of the project.",
        "It builds a mildly interactive data dashboard using Plotly's Dash framework.",
        "The core interactive functionality is two dropdown menus, which enable users to select a CFR title/part for examination.",
        "However, all plots also provide additional information if the user hovers over plot elements.",
        "Future development (high priority) will add plot-based analysis of the factors contributing to regulatory bulk, especially those relating to dissimilarity scores",
        "Future development (low priority) may also rehost this app on AWS via Elastic Beanstalk, instead of the current Plotly Cloud hosting.",
        ]),
}

page_css = {
    'row': {'display': 'flex', 'flexDirection': 'row', 'maxWidth': '2048px'},
    'col': {'display': 'flex', 'flexDirection': 'column', 'maxHeight': '60px'},
    'Div': {
        "backgroundColor": page_colors['bg'],
        "margin": "0px", # outside
        "border": f"2px solid {page_colors['void']}", "borderRadius": "8px",
        "paddingBottom": "2px", "paddingTop": "2px", "paddingLeft": "2px", "paddingRight": "2px", # inside
    },
    'Text': {
        "fontFamily": "Helvetica, sans-serif", "fontSize": "14px", "color": page_colors['text'],
        "paddingBottom": "2px", "paddingTop": "2px", "paddingLeft": "8px", "paddingRight": "8px", # inside
        },
    'A': {'color': page_colors['mg']},
}

page_css = page_css | {
    'chp-flex': page_css['Div'] | {"backgroundColor": page_colors['mg'], 'flex':1, 'min-width': '604px'},
    'flex-1': page_css['Div'] | {'flex': 1, 'min-width': '300px'},
    'flex-2': page_css['Div'] | {'flex': 2, 'min-width': '604px'},
    'chapter': page_css['row'] | {"textAlign": "center"},
    'spacer': page_css['row'] | {'backgroundColor': page_colors['void'], 'height':'32px'},
    }

page_css = page_css | {
    'H1': page_css['Text'] | {"fontSize": "16px", "fontWeight": "bold"},
    'H2': page_css['Text'] | {"fontSize": "14px", "fontWeight": "bold"},
    'P': page_css['Text']  | {'line-height':'1.5'},
    'Chp-P': page_css['Text'] | {'color': page_colors['void']},
    'SM': page_css['Text']  | {'fontSize':'10px'},
}

page_css = page_css | {
    'Fig': {'minHeight':'300px'},
    'Chp-H1': page_css['H1'] | {'color': page_colors['void']},
}


##########==========##########==========##########==========##########==========##########==========##########==========
########## Class Methods: Basics and Data Unpacking ======= ##########

class DisplayData:
    '''Build dashboards using the data extracts'''


    def __init__(self, debug_mode=False, text=None, css=None):

        # initialize data slots
        self.notable_sections, self.data_focal, self.data_temporal = None, None, None
        self.temporal_section,self.temporal_authority,self.temporal_word,self.temporal_match = None, None, None, None
        self.text = text

        # initialize plotly Dash app slots
        self.default_title = 'Title-31'
        self.default_part = 'Part-0586'
        self.text = text
        self.css = css
        self.layout = []
        self.dir = 'b_io'


    def __str__(self) -> str:
        return 'TODO: Build a proper print method'


    def read_data(self):
        '''Read in data for dashboard'''

        self.notable_sections = pd.read_excel(
            io=os.path.join(self.dir, 'notable_sections.xlsx'),
            dtype={'year_id':str, 'title_id':str, 'part_id':str, 'section_id':str, 'notable_deviant':str,
                   'section_names':str, 'section_text':str, 'section_deviance':float},
            )
        self.data_focal = pd.read_excel(
            io=os.path.join(self.dir, 'part_data_focal.xlsx'),
            dtype={'year_id':str, 'title_id':str, 'part_id':str,
                   'part_heading':str, 'part_authority':str, 'deviance_mean':float}
            )
        self.data_temporal = pd.read_excel(
            io=os.path.join(self.dir, 'part_data_temporal.xlsx'),
            dtype={'year_id':str, 'title_id':str, 'part_id':str,
                   'section_count':float, 'authority_count':float, 'part_word_count':float,
                   'focal_year_match':float}
            )
        self.titles = pd.read_excel(
            io=os.path.join(self.dir, 'titles.xlsx'),
            dtype={'title_id':str, 'title_name':str, 'title_num':int, 'title_name_full':str}
            )
        return None
        

    def unpack_temporal_data(self):
        '''Unpack temporal data for dashboard'''

        self.temporal_section = self.data_temporal[['title_id', 'part_id', 'year_id', 'section_count']]\
            .pivot(index=['title_id', 'part_id',], columns='year_id', values='section_count')
        self.temporal_authority = self.data_temporal[['title_id', 'part_id', 'year_id', 'authority_count']]\
            .pivot(index=['title_id', 'part_id',], columns='year_id', values='authority_count')
        self.temporal_word = self.data_temporal[['title_id', 'part_id', 'year_id', 'part_word_count']]\
            .pivot(index=['title_id', 'part_id',], columns='year_id', values='part_word_count')
        self.temporal_match = self.data_temporal[['title_id', 'part_id', 'year_id', 'focal_year_match']]\
            .pivot(index=['title_id', 'part_id',], columns='year_id', values='focal_year_match')
        return None
    
    def most_noteworthy_part(self):
        '''Identify the most noteworthy part for dashboard default'''

        # find title with thighest median part deviance_mean (excluding RESERVED parts)
        i = self.data_focal['deviance_mean'].notnull() & ~self.data_focal['part_heading'].str.contains('RESERVED')
        self.default_title = self.data_focal.loc[i, ['title_id','deviance_mean']]\
            .groupby('title_id').median().sort_values('deviance_mean', ascending=False).index[0]
        
        # find part with highest deviance_mean within that title
        i = i & (self.data_focal['title_id'] == self.default_title)
        self.default_part = self.data_focal.loc[i, ['part_id', 'deviance_mean']]\
            .sort_values('deviance_mean', ascending=False).iloc[0,0]
        
        # round part's deviance_mean for display consistency
        self.data_focal['deviance_mean'] = self.data_focal['deviance_mean'].round(3)

        return None


    def enrich_title_names(self):
        '''Enrich data with full title names'''
        self.data_focal = self.data_focal.merge(
            right=self.titles[['title_id', 'title_name_full']],
            how='left', on='title_id'
        )
        return None
    
##########==========##########==========##########==========##########==========##########==========##########==========
########## Class: Introduction Chapter ===##########


    def add_intro_row(self):
        '''Add introductory text to dashboard'''

        self.layout.append(
            html.Div([
                html.Div([
                    html.H1('Context', style=self.css['H1']),
                    html.P(self.text['Intro-Context'], style=self.css['P']),
                    ], style=self.css['flex-1']),
                html.Div([
                    html.H1('Theory', style=self.css['H1']),
                    html.P(self.text['Intro-Theory'], style=self.css['P']),
                    ], style=self.css['flex-1']),
                html.Div([
                    html.H1('Approach', style=self.css['H1']),
                    html.P(self.text['Intro-Approach'], style=self.css['P']),
                    ], style=self.css['flex-1']),
                ], style=self.css['row']
            )
        )
        
        return None
    
##########==========##########==========##########==========##########==========##########==========##########==========
########## Class Methods: Part Analysis Chapter ##

    def add_part_chapter_heading(self):
        '''Add a text banner row introducing the temporal trends plots below'''

        self.layout.append(html.Div([html.P(' ')], style=self.css['spacer']))

        layout  = html.Div([
            html.Div([
                html.H1('Chapter I: Examining the CFR, One Part at a Time', style=self.css['Chp-H1']),
                html.P(self.text['Chapter-Part'], style=self.css['Chp-P']),
            ], style=self.css['chp-flex']),
        ], style=self.css['chapter'])

        self.layout.append(layout)
        return None


    def add_part_selector_row(self):
        '''Add part selection dropdowns to dashboard'''

        # function: set dropdown options
        def dropdown_options(title_id=None, part_id=None):
            '''Output properly populated dropdown options'''

            # Validate title_id
            if title_id is None:
                title_id = self.default_title
                part_id  = self.default_part


            # make lists of valid titles and parts
            valid_parts = self.data_focal.loc[self.data_focal['title_id'] == title_id, 'part_id'].unique().tolist()
            valid_parts.sort()
            valid_titles = self.data_focal['title_id'].unique().tolist()
            valid_titles.sort()
                
            # Validate part_id
            if (part_id is None) or (part_id not in valid_parts):
                if title_id == self.default_title:
                    part_id = self.default_part
                else:
                    part_id = valid_parts[0]

            # Generate display names- TODO
            def formatter(x):
                x = x.replace('Part-', 'Part ')
                x = x.replace('Title-', 'Title ')
                return x

            valid_titles = {i:formatter(i) for i in valid_titles}
            valid_parts  = {i:formatter(i) for i in valid_parts}

            return title_id, valid_titles, part_id, valid_parts

        # layout: append two flex-1 dropdowns in a row

        self.layout.append(
            html.Div([
                html.Div([
                    dash.dcc.Dropdown(
                        id='title-dropdown', options=dropdown_options(title_id=self.default_title)[1],
                        value=self.default_title, clearable=False, style=self.css['H2'],
                        )
                    ], style=self.css['flex-1']),
                html.Div([
                    dash.dcc.Dropdown(
                        id='part-dropdown', options=dropdown_options(title_id=self.default_title)[3],
                        value=dropdown_options(title_id=self.default_title)[2], clearable=False, style=self.css['H2'],
                    )
                    ], style=self.css['flex-1']),
                html.Div([
                    html.P('', style=self.css['P']|{'text-align':'left'}),
                    ], style=self.css['flex-1']),
                ], style=self.css['row']
            )
        )

        # callback: update part options based on title selection
        @dash.callback(
            dash.Output('title-dropdown', 'value'),
            dash.Output('title-dropdown', 'options'),
            dash.Output('part-dropdown', 'value'),
            dash.Output('part-dropdown', 'options'),
            dash.Input('title-dropdown', 'value'),
            dash.Input('part-dropdown', 'value'),
        )
        def update_dropdowns(title_value, part_value):
            return dropdown_options(title_id=title_value, part_id=part_value)


    def add_part_focal_row(self):
        '''Add focal part's basic details to dashboard'''

        # define data extraction function
        def get_focal_details(title_value=None, part_value=None, df=self.data_focal):
            '''helper function to get focal part details'''
            i = (df['title_id'] == title_value) & (df['part_id'] == part_value)
            df = df.loc[i, ['title_name_full', 'part_heading', 'part_authority', 'deviance_mean']].copy()
            df['part_heading'] = df['part_heading'].str.title()
            df = df.iloc[0].to_list()
            return df


        # layout: append three flex-1 divs in a row
        self.layout.append(
            html.Div([
                html.Div([
                    html.H2('Part Heading', style=self.css['H2'], id='title-name-full'),
                    html.P(get_focal_details(
                        self.default_title, self.default_part)[0], style=self.css['P'], id='part-heading'),
                ], style=self.css['flex-1']),
                html.Div([
                    html.H2('Law granting rule-making authority', style=self.css['H2']),
                    html.P(get_focal_details(
                        self.default_title, self.default_part)[1], style=self.css['P'], id='part-authority'),
                ], style=self.css['flex-1']),
                html.Div([
                    html.H2('Rule dissimilarity score (Lower is better)', style=self.css['H2']),
                    html.P(get_focal_details(
                        self.default_title, self.default_part)[2], style=self.css['P']|{'font-size':'18pt'}, id='deviance-mean'),
                ], style=self.css['flex-1']),
                ], style=self.css['row']
            )
        )

        # callback: update part focal details based on title and part selection
        @dash.callback(
            dash.Output('title-name-full', 'children'),
            dash.Output('part-heading', 'children'),
            dash.Output('part-authority', 'children'),
            dash.Output('deviance-mean', 'children'),
            dash.Input('title-dropdown', 'value'),
            dash.Input('part-dropdown', 'value'),

        )
        def update_part_focal(title_value, part_value):
            return get_focal_details(title_value, part_value)

        return None


    def add_dissimilarity_distribution_row(self):
        '''Add dissimilarity distribution plot to dashboard'''

        # define data extraction function
        def get_deviance_distribution(title_id=self.default_title, part_id=self.default_part, df=self.data_focal):
            '''helper function to get dissimilarity distribution plot'''
            highlight = (title_id, part_id)
            df = df.loc[df['title_id'] == highlight[0], ['title_id', 'part_id', 'deviance_mean']].copy()
            df['color'] = page_colors['mg']+'88'
            i = (df['title_id'] == highlight[0]) & (df['part_id'] == highlight[1])
            df.loc[i, 'color'] = page_colors['fg']
            df = df.sort_values('deviance_mean', ascending=False).reset_index(drop=True)
            return df
        
        # define plot generation function
        def show_deviance_distribution(title_id=self.default_title, part_id=self.default_part):
            '''generate plot instructions for dissimilarity distribution plot'''
            dist_data = get_deviance_distribution(title_id=title_id, part_id=part_id)
            return dash.dcc.Graph(
                id='deviance-distribution-plot',
                figure={
                    'data': [
                        {
                            'type': 'bar',
                            'x': dist_data.index,
                            'y': dist_data['deviance_mean'],
                            'text': dist_data['part_id'].str.replace('Part-', ''),
                            'marker': {'color': dist_data['color'], 'line': {'width':0}},
                            'hoverinfo': 'text+y', 'textposition': 'none',
                            'hovertemplate': 'Part: <b>%{text}</b><br>Score: <b>%{y}</b><extra></extra>',
                            'hovermode': 'x',
                        }
                    ],
                    'layout': {
                        'title': {'text': 'Dissimilarity Scores for All Parts in Title (Selected Part Highlighted)'},
                        'xaxis': {'title': {'text': 'Parts (Hover to see part number)'}, 'showticklabels': False},
                        'yaxis': {'title': {'text': 'Dissimilarity Score (Lower is better)'}, 'showticklabels': True},
                        'plot_bgcolor': page_colors['bg'], 'paper_bgcolor': page_colors['bg'],
                        'font': {'color': page_colors['text']},
                        'margin': {'l': int(2**6.0), 'r': 0, 't': int(2**5.5), 'b': int(2**5.5)},
                        'bargap': 0,
                        'annotations': [{
                            'x': dist_data[dist_data['color'] == page_colors['fg']].index[0],
                            'y': dist_data[dist_data['color'] == page_colors['fg']].deviance_mean.values[0],
                            'text': 'Selected Part', 'bgcolor': page_colors['bg']+'88',
                            'showarrow': True, 'arrowhead': 2, 'arrowcolor': page_colors['fg'],
                            'font': {'color': page_colors['fg'], 'size':12},
                        }]
                    }
                }
            ,style=self.css['Fig'])
        
        # define layout addition
        self.layout.append(
            html.Div([
                html.Div([
                    html.H2('Comparing Dissimilarity Scores', style=self.css['H2']),
                    html.P(self.text['Dissimilarity-Histogram-1'], style=self.css['P']),
                    html.P(self.text['Dissimilarity-Histogram-2'], style=self.css['P']),
                ], style=self.css['flex-1']),
                html.Div([
                    show_deviance_distribution(),
                ], style=self.css['flex-2']),
            ], style=self.css['row'])
        )

        # define callback
        @dash.callback(
            dash.Output('deviance-distribution-plot', 'figure'),
            dash.Input('title-dropdown', 'value'),
            dash.Input('part-dropdown', 'value'),
        )
        def update_deviance_distribution(title_value, part_value):
            return show_deviance_distribution(title_id=title_value, part_id=part_value).figure

        return None
    

    def add_examples_row(self):
        '''Add temporal trends plots to dashboard'''
        
        # function: extract example text
        def get_sample_text(part_id, title_id):
            '''helper function to get most congruent and dissimilar sections'''

            # make container for results
            results = ['N/A', 'No noteworthy sections in this part.', 'N/A', 'No noteworthy sections in this part.']

            # extract relevant sections of the dataset
            i = (self.notable_sections['title_id'] == title_id) & (self.notable_sections['part_id'] == part_id)
            if i.sum() != 2:
                return results
            df = self.notable_sections.loc[i].copy()
            df = df.round({'section_deviance':3}).astype({'section_deviance': str})

  
            # format sections
            results[0] = 'Score: ' + str(
                df.loc[df['notable_deviant'] == 'Least', 'section_deviance'].values[0])
            results[2] = 'Score: ' + str(
                df.loc[df['notable_deviant'] == 'Most', 'section_deviance'].values[0])
            results[1] = df.loc[df['notable_deviant'] == 'Least', 'section_name'].values[0] + '--' + \
                df.loc[df['notable_deviant'] == 'Least', 'section_text'].values[0]
            results[3] = df.loc[df['notable_deviant'] == 'Most', 'section_name'].values[0] + '--' + \
                df.loc[df['notable_deviant'] == 'Most', 'section_text'].values[0]

            
            return results
        # layout:
        self.layout.append(
            html.Div([
                html.Div([
                    html.H2('Most Characteristic Section In This Part', style=self.css['H2']),
                    html.P('Score: SCORE', style=self.css['P']|{'lineHeight':'1.0'}, id='congruent-score'),
                    html.P('SECTION TEXT', style=self.css['SM'], id='congruent-section'),
                ], style=self.css['flex-1']),
                html.Div([
                    html.H2('Most Dissimilar Section In This Part', style=self.css['H2']),
                    html.P('Score: SCORE', style=self.css['P']|{'lineHeight':'1.0'}, id='dissimilar-score'),
                    html.P('SECTION TEXT', style=self.css['SM'], id='dissimilar-section'),
                ], style=self.css['flex-1']),
            ], style=self.css['row'])
        )

        # define callback
        @dash.callback(
            dash.Output('congruent-score', 'children'),
            dash.Output('congruent-section', 'children'),
            dash.Output('dissimilar-score', 'children'),
            dash.Output('dissimilar-section', 'children'),
            dash.Input('title-dropdown', 'value'),
            dash.Input('part-dropdown', 'value'),
        )
        def update_sample_text(title_value, part_value):
            return get_sample_text(part_id=part_value, title_id=title_value)

        return None
    
##########==========##########==========##########==========##########==========##########==========##########==========
########## Class Methods: Temporal Analysis Chapter ========##########
    
    def add_temporal_chapter_heading(self):
        '''Add a text banner row introducing the temporal trends plots below'''

        self.layout.append(html.Div([html.P(' ')], style=self.css['spacer']))

        layout  = html.Div([
            html.Div([
                html.H1('Chapter II: Understanding Trends Over Time', style=self.css['Chp-H1']),
                html.P(self.text['Chapter-Temporal'], style=self.css['Chp-P']),
            ], style=self.css['chp-flex']),
        ], style=self.css['chapter'])

        self.layout.append(layout)
        return None
    

    def add_temporal_row(self):
        '''Add temporal trends plots to dashboard'''

        # function: word count over time (section count too?)
        def show_trend(title_id=self.default_title, part_id=self.default_part, switch=None):
            ''' plot word count over time'''

            # add switches
            if switch == 'temporal_word':
                df = self.temporal_word.loc[(title_id, part_id), :]
                id = 'word-count-trend-plot'
                y_title = 'Number of Words'
                tickformat='~s'
            elif switch == 'temporal_section':
                df = self.temporal_section.loc[(title_id, part_id), :]
                id = 'section-count-trend-plot'
                y_title = 'Number of Sections'
                tickformat='d'
            elif switch == 'temporal_authority':
                df = self.temporal_authority.loc[(title_id, part_id), :]
                id = 'authority-count-trend-plot'
                y_title = 'Rulemaking Authorities Cited'
                tickformat='d'
            elif switch == 'temporal_match':
                df = self.temporal_match.loc[(title_id, part_id), :]
                id = 'focal-match-trend-plot'
                y_title = f'Sections Identical to Final Year'
                tickformat='.0%'
            else:
                return html.Div(['Error: No valid switch provided.'], style=self.css['Fig'])


            # calculate x range
            end_year = int(df.index.max()) + 0.5
            start_year = int(df.index.min()) - 0.5
            x_range = (start_year, end_year)

            # plot data
            the_plot = dash.dcc.Graph(
                id=id,
                figure={
                    'data': [
                        {
                            'type': 'line',
                            'x': df.index,
                            'y': df.values,
                            'mode': 'lines+markers',
                            'line': {'color': page_colors['fg']},
                            'marker': {'size': 8, 'color': page_colors['fg']},
                            'hoverinfo': 'x+y',
                            'hovertemplate': f'Year: <b>%{{x}}</b><br>{y_title}: <b>%{{y}}</b><extra></extra>',
                            'fill': 'tozeroy',
                            'fillcolor': page_colors['mg']+'88',
                        }
                    ],
                    'layout': {
                        'title': {'text': f'{part_id.replace("-", " ")}: {y_title} Over Time'},
                        'xaxis': {'showticklabels': True, 'range':x_range},
                        'yaxis': {'title': {'text': y_title}, 'showticklabels': True, 'tickformat':tickformat},
                        'plot_bgcolor': page_colors['bg'], 'paper_bgcolor': page_colors['bg'],
                        'font': {'color': page_colors['text']},
                        'margin': {'l': int(2**6.0), 'r': 0, 't': int(2**5.5), 'b': int(2**5.5)},
                    }
                }
            ,style=self.css['Fig'])
            return the_plot

        # function: authority count x section
        def show_authority_impact(title_id=self.default_title, part_id=self.default_part):
            ''' plot authority count vs section count'''

            # get data
            df1 = self.temporal_authority.loc[(title_id, ), :].reset_index()\
                .melt(id_vars='part_id', var_name='year_id', value_name='authority_count')
            df2 = self.temporal_section.loc[(title_id, ), :].reset_index()\
                .melt(id_vars='part_id', var_name='year_id', value_name='section_count')
            df = df2.merge(right=df1, how='left', on=['part_id', 'year_id'])
            del df1, df2

            # remove outliers
            df['z_section'] = (df['section_count'] - df['section_count'].mean()) / df['section_count'].std()
            df = df.drop(df[ df['z_section'].abs() > 3 ].index)

            
            # plot data
            the_plot = dash.dcc.Graph(
                id='authority-section-scatter',
                figure={
                    'data': [
                        {
                            'type': 'scatter',
                            'x': df['authority_count'],
                            'y': df['section_count'],
                            'mode': 'markers',
                            'marker': {'size': 8, 'color': page_colors['fg']+'88'},
                            'hoverinfo': 'x+y',
                            'hovertemplate': f'Authorities: <b>%{{x}}</b><br>Sections: <b>%{{y}}</b><extra></extra>',
                        }
                    ],
                    'layout': {
                        'title': {'text': f'{title_id.replace("-", " ")}: Authorities Cited vs. Number of Sections'},
                        'xaxis': {'title': {'text':'Number of Rulemaking Authorities Cited (Log Scale)'}, 'showticklabels': True, 'tickformat':'d', 'type':'log'},
                        'yaxis': {'title': {'text':'Number of Sections (Log Scale)'}, 'showticklabels': True, 'tickformat':'d','type':'log'},
                        'plot_bgcolor': page_colors['bg'], 'paper_bgcolor': page_colors['bg'],
                        'font': {'color': page_colors['text']},
                        'margin': {'l': int(2**6.0), 'r': 0, 't': int(2**5.5), 'b': int(2**5.5)},
                    }
                }
            ,style=self.css['Fig'])
            return the_plot
        
        show_authority_impact()

        # layout: three chart panels in a row
        self.layout.append(
            html.Div([


            html.Div([
                show_trend(switch='temporal_word'),
            ], style=self.css['flex-1']),


            html.Div([
                show_trend(switch='temporal_match'),
            ], style=self.css['flex-1']),


            html.Div([
                show_authority_impact()
            ], style=self.css['flex-1']),


        ], style=self.css['row']|{'minHeight':'300px'})
        )


        # callback: update three chart panels based on title and part selection
        @dash.callback(
            dash.Output('word-count-trend-plot', 'figure'),
            dash.Output('focal-match-trend-plot', 'figure'),
            dash.Output('authority-section-scatter', 'figure'),
            dash.Input('title-dropdown', 'value'),
            dash.Input('part-dropdown', 'value'),
        )
        def update_temporal_trends(title_value, part_value):
            return (
                show_trend(title_id=title_value, part_id=part_value, switch='temporal_word').figure,
                show_trend(title_id=title_value, part_id=part_value, switch='temporal_match').figure,
                show_authority_impact(title_id=title_value).figure,
            )

        return None

##########==========##########==========##########==========##########==========##########==========##########==========
########## Class Methods: Methodology Chapter ####

    def add_methodology_chapter_heading(self):
        '''Add a text banner row introducing the methodology content below'''

        self.layout.append(html.Div([html.P(' ')], style=self.css['spacer']))

        layout  = html.Div([
            html.Div([
                html.H1('Chapter III: Methodology', style=self.css['Chp-H1']),
                html.P([
                    self.text['Chapter-Methodology'],
                    html.A(href='https://sjoshua.github.io/gallery.html', children='sjoshua.github.io', style=page_css['A']),
                    ], style=self.css['Chp-P']),
            ], style=self.css['chp-flex']),
        ], style=self.css['chapter'])

        self.layout.append(layout)
        return None

    def add_methodology_row(self):
        '''Add methodology content to dashboard'''
        self.layout.append(
            html.Div([
                html.Div([
                    html.H2('Step 1. Import Data (requests)', style=self.css['H2']),
                    html.P([
                        html.A(href="https://github.com/sjoshuam/reg_analysis/blob/main/m1_import_data.py", children="m1_import_data.py", style=page_css['A']),
                        self.text['Step-1']], style=self.css['P']),
                ], style=self.css['flex-1']),
                html.Div([
                    html.H2('Step 2. Extract Data (xml + pyspark)', style=self.css['H2']),
                    html.P([
                        html.A(href="https://github.com/sjoshuam/reg_analysis/blob/main/m2_extract_data.py", children="m2_extract_data.py", style=page_css['A']),
                        self.text['Step-2']], style=self.css['P']),
                ], style=self.css['flex-1']),
                html.Div([
                    html.H2('Step 3. Embed Text (pyspark + torch on GPU)', style=self.css['H2']),
                    html.P([
                        html.A(href="https://github.com/sjoshuam/reg_analysis/blob/main/m3_embed_data.py", children="m3_embed_data.py", style=page_css['A']),
                        self.text['Step-3']], style=self.css['P']),
                ], style=self.css['flex-1']),
            ], style=self.css['row']
        )
        )
        self.layout.append(
            html.Div([
                html.Div([
                    html.H2('Step 4. Quantify Data (pyspark + numpy)', style=self.css['H2']),
                    html.P([
                        html.A(href="https://github.com/sjoshuam/reg_analysis/blob/main/m4_quantify_data.py", children="m4_quantify_data.py", style=page_css['A']),
                        self.text['Step-4']], style=self.css['P']),
                ], style=self.css['flex-1']),
                html.Div([
                    html.H2('Step 5. Display Data (pandas + dash)', style=self.css['H2']),
                    html.P([
                        html.A(href="https://github.com/sjoshuam/reg_analysis/blob/main/m5_display_data.py", children="m5_display_data.py", style=page_css['A']),
                        self.text['Step-5']], style=self.css['P']),
                ], style=self.css['flex-1']),
            ], style=self.css['row']
        )
        )
        return None

##########==========##########==========##########==========##########==========##########==========##########==========
########## Class Methods: Pipeline Assembly ######
    

    def package_app(self):
        '''Package the Dash app for deployment'''
        working_dir = os.path.basename(os.getcwd())
        if working_dir == 'reg_analysis':
            if not os.path.exists('c_out/b_io'):
                os.makedirs('c_out/b_io')
            os.system('cp b_io/* c_out/b_io/')
            os.system('cp m5_display_data.py c_out/cfr-analysis.py')
        return None


    def display_data(self):
        '''Display data for dashboard'''

        # execute data methods
        self.read_data()
        self.unpack_temporal_data()
        self.most_noteworthy_part()
        self.enrich_title_names()

        # execute pipeline methods
        self.add_intro_row()

        self.add_part_chapter_heading()
        self.add_part_selector_row()
        self.add_part_focal_row()
        self.add_dissimilarity_distribution_row()
        self.add_examples_row()

        self.add_temporal_chapter_heading()
        self.add_temporal_row()

        self.add_methodology_chapter_heading()
        self.add_methodology_row()

        self.layout.append(html.Div([html.P(' ')], style=self.css['spacer']))

        # package app for deployment
        self.package_app()

        return None
    

# TODO/Tech Debt:
# - Write readme !!
# - Write method section


# ASSEMBLE APP
app = dash.Dash(__name__, title='CFR Analysis Dashboard')
server = app.server
display_data = DisplayData(text=page_text, css=page_css)
display_data.display_data()
app.layout = html.Main(display_data.layout)

# TEST EXECUTE CODE 
if __name__ == '__main__':
    app.run(debug=True)
    #app.run(debug=True, use_reloader=True)


##########==========##########==========##########==========##########==========##########==========##########==========
