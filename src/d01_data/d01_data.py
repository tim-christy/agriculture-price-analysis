from urllib.request import urlopen
from bs4 import BeautifulSoup as soup
import pandas as pd 
import sqlite3
import numpy as np


def scrape_page_data(url):
    '''
    Descripton: Grabs one page of html data from http://www.producepriceindex.com/
    Parameters: Web page URL (string)
    Returns: html soup of web page. Html tags still need to be removed.
    '''
    
    from urllib.request import urlopen
    uClient = urlopen(url) # Opens first page
    html = uClient.read() # Reads in all HTML for that page
    uClient.close() # Close connection
    page_soup = soup(html, 'html.parser') # Create soup object
    data = page_soup.find_all('td') # Grab the table data section
    return data # Shows the first page of website


def collect_all_urls(page_start, page_end):
    '''
    Description: Creates a url list that contains all the pages to scrape
                
    Parameters: page_start - page number to start on (int)
                page_end - page number to end on (int)
                
    Returns: List of url pages
    
    '''
    # Grab all page urls. There are 637 pages
    urls = []
    for i in range(page_start, page_end+1):
        urls.append(f'http://www.producepriceindex.com/produce-price-index?field_ppi_commodity_target_id=All&field_ppi_date_value%5Bmin%5D=&field_ppi_date_value%5Bmax%5D=&page={i}')
        
    return urls


def scraped_to_dictionary(url_list):
    
    '''
    Description: Takes a list of urls to be scraped, and places data in a dictionary based on commodity. The result is a 
                 dictionary with a key of produce and a value of the data corresponding to that produce.
    Parameters: a list of the webpages to be scraped
    Returns: Dictionary of data by commodity
    '''
    
    produce = dict() # create an empty dictionary to hold all data

    # For each page on the website, read in the soup object and find all table data
    for page in url_list:
        data = scrape_page_data(page)

        # When you have the html table data, loop through all 25 rows
        for j in range(0, 25):
            commodity = []
            for i in data[0 + 8*j].contents[0].split(' '): # Pick out the name of the commodity (happens every 8 iterations)
                if (i != '') & (i != '\n'):
                    commodity.append(i) # Pick out only the name, ditch the white spaces and newline


            # If the commodity name is one word long, set the dictionary with that word. If not already set, create an empty list
            if len(commodity) == 1: 
                word = commodity[0]
                if word not in produce:
                    produce[word] = []
                produce[word].append(data[1 + 8*j].span.string) # append the date to the list. Found at position 1 and every 8 iterations thereafter

            # otherwise if the commodity is 2 words, fix this into a single item to serve as key. Then do the same as above
            elif len(commodity) == 2: 
                word = commodity[0] + ' ' + commodity[1]
                if word not in produce:
                    produce[word] = []
                produce[word].append(data[1 + 8*j].span.string)

            # 3 words...
            elif len(commodity) == 3:
                word = commodity[0] + ' ' + commodity[1] + ' ' + commodity[2]
                if word not in produce:
                    produce[word] = []
                produce[word].append(data[1 + 8*j].span.string)

            # 4 words...
            elif len(commodity) == 4:
                word = commodity[0] + ' ' + commodity[1] + ' ' + commodity[2] + ' ' + commodity[3]
                if word not in produce:
                    produce[word] = []
                produce[word].append(data[1 + 8*j].span.string)

            # If there's anything longer than 4 words in the commodities column, show me (there's not)
            else:
                print(len(commodity))


            # Now collect the values for each commodity key
            # Everything repeats after 8 iterations, hence the 8*j.
            # data[2 + 8*j] holds the farm price
            for i in data[2 + 8*j].string.split(' '):
                if (i != '') & (i != '\n'):
                    produce[word].append(i)

            # data[3 + 8*j] holds the Atlanta Retail Price
            for i in data[3 + 8*j].string.split(' '):
                if (i != '') & (i != '\n'):
                    produce[word].append(i)

            # Holds the Chicago Retail Price
            for i in data[4 + 8*j].string.split(' '):
                if (i != '') & (i != '\n'):
                    produce[word].append(i)

            # Los Angeles Retail Price
            for i in data[5 + 8*j].string.split(' '):
                if (i != '') & (i != '\n'):
                    produce[word].append(i)


            # New York Retail Price
            for i in data[6 + 8*j].string.split(' '):
                if (i != '') & (i != '\n'):
                    produce[word].append(i)


            # Average Spread
            for i in data[7 + 8*j].string.split(' '):
                if (i != '') & (i != '\n'):
                    produce[word].append(i)
                    
    return produce



def scraped_to_dataframe(url_list):
    '''
    Description: Takes a list of urls and creates a dictionary of dataframes containing all data scraped from urls. The 
                 dictionary has the key-value pair of produce name for the key and dataframe of produce for its corresponding 
                 value.
    Parameters: a list of the url pages
    Returns: Dictionary of dataframes
    '''
    produce = scraped_to_dictionary(url_list)
    # Start an empty dictionary
    produce_df_dict = dict()

    # Create an empty list to store dataframes for concatenation
    df_list = []

    # iterate through every key in the produce dictionary and create a data frame from its data. Every 7 elements
    # in the values for a given key becomes a row in the df. There are 985 total items in each list
    for name in list(produce.keys()):
        for i in range(986):
            df_list.append(pd.DataFrame(produce[str(name)][7*i: 7*(i+1)]).T)

        produce_df_dict[str(name)] = pd.concat(df_list)
        produce_df_dict[str(name)].columns = ['Date', 'Farm Price', 'Atlanta Retail', 'Chicago Retail', 'LA Retail', 'NYC Retail', 'Avg Spread']
        produce_df_dict[str(name)].index = produce_df_dict[str(name)]['Date']
        produce_df_dict[str(name)].drop(columns =['Date'], inplace=True)
        produce_df_dict[str(name)]['Farm Price'] = pd.to_numeric(produce_df_dict[str(name)]['Farm Price'].str.replace('$', ''))
        produce_df_dict[str(name)]['Atlanta Retail'] = pd.to_numeric(produce_df_dict[str(name)]['Atlanta Retail'].str.replace('$', ''))
        produce_df_dict[str(name)]['Chicago Retail'] = pd.to_numeric(produce_df_dict[str(name)]['Chicago Retail'].str.replace('$', ''))
        produce_df_dict[str(name)]['LA Retail'] = pd.to_numeric(produce_df_dict[str(name)]['LA Retail'].str.replace('$', ''))
        produce_df_dict[str(name)]['NYC Retail'] = pd.to_numeric(produce_df_dict[str(name)]['NYC Retail'].str.replace('$', ''))
        produce_df_dict[str(name)]['Commodity'] = str(name)
        
    return produce_df_dict 





def create_one_dataframe(dictionary_of_dataframes):
    '''
    Description: Takes in a dictionary of dataframes and concatenates all dataframes into one
    Parameters: dictionary_of_dataframes - a dictionary of dataframes
    returns: One single concatenated dataframe
    
    '''
    
    full_df = []
    for name in list(dictionary_of_dataframes.keys()):
        full_df.append(dictionary_of_dataframes[name])

    grand_df = pd.concat(full_df)
    
    return grand_df


def insert_row(cursor, connection, row_list):
    '''
    Description: Inserts a row of data into a table in a database
    Parameters: cursor - the cursor object from sqlite3
                connection - the connection object from sqlite3
                row_list - the data to be placed in the row
    Returns: None
    '''
    
    with connection:
        cursor.execute('''INSERT INTO produce_prices VALUES (:farm_price, :atlanta_retail, :chicago_retail, :los_angeles_retail, :nyc_retail, :avg_spread, :commodity, :date)''', 
                 {'farm_price': row_list[0], 'atlanta_retail': row_list[1], 'chicago_retail': row_list[2], 'los_angeles_retail':row_list[3], 'nyc_retail': row_list[4], 'avg_spread': row_list[5], 'commodity': row_list[6], 'date': row_list[7]})
    
    
    
def make_db(cursor, connection, dataframe):
    '''
    Description: Places an entire dataframe into a database
    Parameters: cursor - the cursor object from sqlite3
                connection - the connection object from sqlite3
                dataframe - the dataframe to be stored in the database
    Returns: None
    '''
    for row in range(0, len(dataframe)-1):
        insert_row(cursor, connection, dataframe.iloc[row])
        

def download_data_from_db(path):
    '''
    Description: Downloads all data in database and stores it in a dataframe
    Parameters: path to database
    Returns: dataframe containing all data in database
    '''
    conn = sqlite3.connect(path)
    c = conn.cursor()
    produce_df = pd.DataFrame(c.execute('''SELECT * FROM agriculture_prices''').fetchall())
    produce_df.columns = ['Farm Price', 'Atlanta Retail', 'Chicago Retail', 'Los Angeles Retail', 'NYC Retail', 'Avg Spread', 'Commodity', 'Date']
    produce_df.index = pd.to_datetime(produce_df['Date'])
    produce_df.drop(columns=['Date'], inplace=True)
    conn.close()
    return produce_df



def make_dictionary_of_dataframes(dataframe):
    '''
    Description: Splits dataframe up by commodity and stores in a dictionary. This creates a dictionary of 
                 dataframes where the key is the produce name and value is the dataframe corresponding to that
                 produce.
    Parameters: Dataframe to be split
    Returns: Dictionary of dataframes
    '''
    produce_list = list(dataframe['Commodity'].unique())
    produce_dict = dict()
    for produce in produce_list:
        df_copy = dataframe[dataframe['Commodity'] == produce].copy() # Copying by slicing gives warning error
        produce_dict.setdefault(produce, df_copy)
        
    return produce_dict



def count_na(df):
    '''
    Description: Counts the number of NaN values in a data frame
    Parameters: df - The dataframe to be checked
    Returns: None. Prints our the percentage of each column that is nan
    '''
    # Anywhere a price is equal to or less than zero, assign it to NaN
    df[df.loc[:, ['Farm Price', 'Atlanta Retail', 'Chicago Retail', 'Los Angeles Retail', 'NYC Retail']] <= 0] = np.nan
    print(f'Percentage NaN for {df.iloc[0, -1]}: \n {round((df.isna().sum())/len(df), 3)*100}')
    print(' ')
    
    
    
def drop_all_na(df):
    '''
    Description: Drops all rows in a dataframe that have values of NaN
    Parameters: df - the dataframe to have NaN values dropped
    Returns: the dataframe with all NaN values taken out
    '''
    
    df_return = df.dropna(inplace=True)
    return df_return



def drop_all_dupes(df):
    '''
    Description: Drops all duplicates in a dataframe
    Parameters: dataframe to drop all duplicates
    returns: deduplicated dataframe
    '''
    df_return = df.drop_duplicates(inplace=True)
    
    return df_return




def inflation_adjustment_for_df(df):
    '''
    Description: Adjusts all individual prices in a dataframe to December 2019. That is, it adjusts for inflation and 
                 reflects the value of a dollar in December 2019. Also creates an average retail column with stdev
    Parameter: Dataframe to be adjusted
    Returns: Inflation adjusted dataframe and an appended average column and stdev column
    '''
    cpi_df = pd.read_csv('../../data/00_raw/cpi.csv', index_col=0, header=1)
    cpi_cols = ['1', '2', '3' , '4', '5', '6', '7', '8', '9', '10', '11', '12', 'Avg', 'Dec-Dec', 'Avg-Avg']
    cpi_df.columns = cpi_cols
    CPI_2019 = cpi_df.loc[2019][10]
    farm = []
    atl = []
    chi=[]
    la=[]
    nyc =[]

    for index_row in df.index:
        count = 0
        for column in df.columns:
            conversion = (CPI_2019/cpi_df.loc[index_row.year][index_row.month - 1])
            value = df[str(index_row)][str(column)].values[0]*conversion
            if column == 'Farm Price':
                farm.append(round(value, 2))
            elif column == 'Atlanta Retail':
                atl.append(round(value,2))

            elif column == 'Chicago Retail':
                chi.append(round(value, 2))

            elif column == 'Los Angeles Retail':
                la.append(round(value, 2))

            elif column == 'NYC Retail':
                nyc.append(round(value,2))

            count+=1
            if count == 5:
                break



    adj_2019_dict = {}

    adj_2019_dict.setdefault('2019 Farm Price', farm)
    adj_2019_dict.setdefault('2019 Atlanta retail', atl)
    adj_2019_dict.setdefault('2019 Chicago Retail', chi)
    adj_2019_dict.setdefault('2019 Los Angeles Retail',la)
    adj_2019_dict.setdefault('2019 NYC Retail', nyc)
    adj_2019_dict.setdefault('Avg Spread', list(df['Avg Spread']))
    adj_2019_dict.setdefault('Commodity', list(df['Commodity']))
    df_2019_adj = pd.DataFrame(adj_2019_dict)
    df_2019_adj.index = df.index
    
    
    avg_retail = [round(np.mean(x[1:5]),2) for x in df.values]
    avg_retail_var = [round(np.var(x[1:5],ddof=1), 2) for x in df.values] 
    df_2019_adj['Avg_Retail'] = avg_retail
    df_2019_adj['Avg_Retail_Var'] = avg_retail_var

    
    return df_2019_adj
    