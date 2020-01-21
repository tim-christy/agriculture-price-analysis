from urllib.request import urlopen
from bs4 import BeautifulSoup as soup
import pandas as pd 



def scrape_page_data(url):
    '''
    Descripton: Grabs one page of html data from http://www.producepriceindex.com/
    Parameters: Web page URL (string)
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
    Description: 
                Returns a list of strings containing all urls for which data is to be collected.
                
    Parameters:
                page_start - page number to start on (int)
                
                page_end - page number to end on (int)
    
    '''
    # Grab all page urls. There are 637 pages
    urls = []
    for i in range(page_start, page_end+1):
        urls.append(f'http://www.producepriceindex.com/produce-price-index?field_ppi_commodity_target_id=All&field_ppi_date_value%5Bmin%5D=&field_ppi_date_value%5Bmax%5D=&page={i}')
        
    return urls


def scraped_to_dictionary(url_list):
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
    full_df = []
    for name in list(produce_df_dict.keys()):
        full_df.append(produce_df_dict[name])

    grand_df = pd.concat(full_df)
    
    return grand_df


def insert_row(row_list):
    with conn:
        c.execute('''INSERT INTO agriculture_prices VALUES (:farm_price, :atlanta_retail, :chicago_retail, :los_angeles_retail, :nyc_retail, :avg_spread, :commodity, :date)''', 
                 {'farm_price': row_list[0], 'atlanta_retail': row_list[1], 'chicago_retail': row_list[2], 'los_angeles_retail':row_list[3], 'nyc_retail': row_list[4], 'avg_spread': row_list[5], 'commodity': row_list[6], 'date': row_list[7]})
    
    
    
def make_db(n):
    for row in range(0, n+1):
        insert_row(grand_df.iloc[row])