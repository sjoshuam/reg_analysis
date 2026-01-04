import dash, os, pandas as pd
from dash import html

# Define page elements

page_colors = {
    'bg':  '#cfdae6', # (210,0.1,0.9)
    'text':'#030e1a', # (210,0.9,0.1)
    'mg':  '#406080', # (210,0.5,0.5)
    'fg':  '#08111a', # (210,0.7,0.1)
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
        "For the technically inclined, my measure is a rescaled version of cosine distance, and my embedding model is Hugging Face's 'legal-bert-base-uncased' SBERT model."
        ]),
    "Dissimilarity-Histogram-1":"  ".join([
        "The bar chart (right) shows the dissimilarity scores for all parts in the selected title.",
        "The height of each bar shows the dissimilarity score for a part.",
        "The selected part is highlighted in a darker shade.",
        "If the selected part has one of the higher scores (i.e., a tall bar), it may be a good candidate for review.",
        ]),
    "Dissimilarity-Histogram-2":"  ".join([
        "To illustrate what these scores mean substantively, consider Title 31, Part 586.",
        "This part has a score of 0.41 (high for a CFR part) and concerns Chinese military-industrial complex sanctions.",
        "The part (1) implements an unusually large number of statutes, (2) opposes a highly-adaptive adversary, and (3) is more of a National Security matter despite being a Treasury responsibility.",
        "Fragmented legistration, wily opposition, and divided responsibility -- These are all risk factors for regulatory complexity."
        ])
}

page_css = {
    'row': {'display': 'flex', 'flexDirection': 'row', 'maxWidth': '2048px',},
    'Div': {
        "backgroundColor": page_colors['bg'],
        "margin": "0px", # outside
        "border": "2px solid #FFFFFF", "borderRadius": "8px",
        "paddingBottom": "2px", "paddingTop": "2px", "paddingLeft": "2px", "paddingRight": "2px", # inside

    },
    'Text': {
        "fontFamily": "Helvetica, sans-serif", "fontSize": "14px", "color": page_colors['text'],
        "paddingBottom": "2px", "paddingTop": "2px", "paddingLeft": "8px", "paddingRight": "8px", # inside
        },
}

page_css = page_css | {
    'flex-1': page_css['Div'] | {'flex': 1, 'min-width': '300px'},
    'flex-2': page_css['Div'] | {'flex': 2, 'min-width': '604px'},
}

page_css = page_css | {
    'H1': page_css['Text'] | {"fontSize": "16px", "fontWeight": "bold"},
    'H2': page_css['Text'] | {"fontSize": "14px", "fontWeight": "bold"},
    'P': page_css['Text']  | {'line-height':'1.5'},
    'SM': page_css['Text']  | {'fontSize':'10px'},
}

page_css = page_css | {
    'Fig': {'minHeight':'300px'},
}


# DEFINE CLASS

class DisplayData:
    '''Build dashboards using the data extracts'''

    # -- Basic Methods --------


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


    # -- Data Methods --------


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


    # -- Dashboard Methods --------



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


    def add_part_selector_row(self):
        '''Add part selection dropdowns to dashboard'''
        
        def format_dropdown_options(col_name, df=self.data_focal, filter: tuple=None) -> dict:
            '''helper function to format dropdown options'''
            if filter is not None: df = df.loc[df[filter[0]] == filter[1], [col_name]].copy()
            else: df = df[[col_name]].copy()
            df = df.drop_duplicates().sort_values(col_name).reset_index(drop=True).copy()
            df['name'] = df[col_name].str.replace('^([A-Za-z]+)-0*','\\1 ',regex=True)
            options = [{'label':row['name'], 'value':row[col_name]} for i, row in df.reset_index(drop=True).iterrows()]
            return options

        # layout: append two flex-1 dropdowns in a row
        self.layout.append(
            html.Div([
                html.Div([
                    dash.dcc.Dropdown(
                        id='title-dropdown',
                        options=format_dropdown_options('title_id'),
                        value=self.default_title, clearable=False, style=self.css['H2'],
                    )
                    ], style=self.css['flex-1']),
                html.Div([
                    dash.dcc.Dropdown(
                        id='part-dropdown',
                        options=format_dropdown_options('part_id', filter=('title_id', self.default_title)),
                        value=self.default_part, clearable=False, style=self.css['H2'],
                    )
                    ], style=self.css['flex-1']),
                html.Div([
                    html.P('<-- Select a CFR Title and Part to examine it', style=self.css['P']|{'text-align':'left'}),
                    ], style=self.css['flex-1']),
                ], style=self.css['row']
            )
        )

        # callback: update part options based on title selection
        @dash.callback(
            dash.Output('part-dropdown', 'options'),
            dash.Input('title-dropdown', 'value'),
        )
        def update_part_dropdown(title_value):
            return format_dropdown_options('part_id', filter=('title_id', title_value))


    def add_part_focal_row(self):
        '''Add focal part's basic details to dashboard'''

        # define data extraction function
        def get_focal_details(title_value=None, part_value=None, df=self.data_focal):
            '''helper function to get focal part details'''
            i = (df['title_id'] == title_value) & (df['part_id'] == part_value)
            df = df.loc[i, ['title_name_full', 'part_heading', 'part_authority', 'deviance_mean']].copy()
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
                        self.default_title, self.default_part)[2], style=self.css['P'], id='deviance-mean'),
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
            df['color'] = page_colors['mg']
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
        # TODO

        # layout:
        self.layout.append(
            html.Div([
                html.Div([
                    html.H2('SECTION TITLE (Least Dissimilar)', style=self.css['H2']),
                    html.P('Score: SCORE', style=self.css['P']),
                    html.P('SECTION TEXT', style=self.css['SM']),
                ], style=self.css['flex-1']),
                html.Div([
                    html.H2('SECTION TITLE (Most Dissimilar)', style=self.css['H2']),
                    html.P('Score: SCORE', style=self.css['P']),
                    html.P('SECTION TEXT', style=self.css['SM']),
                ], style=self.css['flex-1']),
            ], style=self.css['row'])
        )

        # callback:
        # TODO


        return None


    # -- Pipeline Executor --------
    

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
        self.add_part_selector_row()
        self.add_part_focal_row()
        self.add_dissimilarity_distribution_row()
        self.add_examples_row()

        # package app for deployment
        self.package_app()

        return None
    

# TODO/Tech Debt:
# - Fix silent errors in callbacks when no part selection made !!
# - Finish examples row
# - Add time series (for at least word count)
# - Build proper __str__ method

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
