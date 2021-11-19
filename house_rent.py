import re
import csv
import requests
import bs4
import pickle
import psycopg2 as pg

from typing import List
from bs4 import BeautifulSoup



class Database(object):
    def __init__(self):
        """
        Contain the functionalities need to setup, insert information into and close the
        database
        """
        # information required to setup the remote database
        self.__password = "Nkopuruk"
        self.__dbname = "postgres"
        self.__user = "postgres"
        self.__connected_to_db = False
        self.__conn = None
        self.__cursor = None

    @property
    def database_name(self):
        return self.__dbname

    @database_name.setter
    def database_name(self, value):
        self.__dbname = value

    @property
    def password(self):
        raise AttributeError("Password are confidential, they are set only you can't view them")

    @password.setter
    def password(self, value):
        self.__password = value

    @property
    def user(self):
        return self.__user

    @user.setter
    def user(self, value):
        self.__user = value

    @property
    def database_status(self) -> bool:
        return self.__connected_to_db

    @database_status.setter
    def database_status(self, value: bool):
        self.__connected_to_db = value

    def setup_database_connection(self):
        """Connects to the database using the secret information provided
        by the user.
        Return an open connection to the database"""
        try:
            self.__conn = pg.connect(database=self.__dbname, user=self.__user,
                                     password=self.__password)
            self.database_status = True
        except pg.Error as error:
            print(error)
            print("Unable to connect to database")

    def query(self, query) -> List[tuple]:
        """Accepts a query string that get to be executed by the database"""
        cursor = self.__conn.cursor()
        try:
            cursor.execute(query)
        except pg.Error as e:
            self.__conn.rollback()
        else:
            self.__conn.commit()
            if cursor.description is not None:
                return cursor.fetchall()

    def close_database_connection(self):
        self.__conn.close()


class Content:
    def __init__(self, title, url, address, no_of_bedrooms,
                 no_of_bathrooms, no_of_toilets, agent_contact, price,
                 description):
        """A structure (or record) to contain information about an entity, i.e. the
        information to be scraped from the website"""
        self.title = title
        self.url = url
        self.address = address
        self.no_of_bedrooms = no_of_bedrooms
        self.no_of_bathrooms = no_of_bathrooms
        self.no_of_toilets = no_of_toilets
        self.agent_contact = agent_contact
        self.price = price
        self.description = description

    def print(self):
        """Display the information contained in the structural container"""
        print(f"URL listing:\t{self.url}")
        print(f"Address:\t{self.address}")
        print(f"No of bedrooms:\t {self.no_of_bedrooms}")
        print(f"No of bathrooms:\t {self.no_of_bathrooms}")
        print(f"No of toilets:\t{self.no_of_toilets}")
        print(f"Agent contact:\t{self.agent_contact}")
        print(f"Price: \t {self.price}")
        print(f"Description: \t {self.description}")


class Website:
    """Contains instruction about how information are to be accessed in a particular
    website. Each website have unique ways of accessing the same type of information"""

    def __init__(self, filename, url, search_url, result_listing, result_url, use_patterns,
                 absolute_url, title_tag, url_tag, address_tag, no_of_bedrooms_tag,
                 no_of_bathrooms_tag, no_of_toilets_tag, agent_contact_tag, price_tag,
                 description_tag, get_parent):
        self.filename = filename
        self.url = url
        self.search_url = search_url
        self.result_listing = result_listing
        self.result_url = result_url
        self.use_patterns = use_patterns
        self.absolute_url = absolute_url

        self.title_tag = title_tag
        self.url_tag = url_tag

        self.address_tag = address_tag
        self.no_of_bedrooms_tag = no_of_bedrooms_tag
        self.no_of_bathrooms_tag = no_of_bathrooms_tag
        self.no_of_toilets_tag = no_of_toilets_tag
        self.agent_contact_tag = agent_contact_tag
        self.price_tag = price_tag
        self.description_tag = description_tag

        self.get_parent = get_parent


class Crawler:
    """The Crawler object, does majority of the work"""
    def __init__(self, site, database):
        self.site = site
        self.visited = set()
        self.content = []

        # initialize and connect to a database
        self.database = database
        self.database.setup_database_connection()

    def set_site_structure(self, site):
        """Setup the website structure information that the crawler will use in
        crawling the site"""
        self.site = site

    def get_page(self, url) -> BeautifulSoup or None:
        """Using the url download the page link and convert to
        a Beautiful object"""
        try:
            req = requests.get(url)
        except requests.exceptions.RequestException:
            return None
        return BeautifulSoup(req.content, 'html5lib')

    def safe_get(self, page_obj, selector, pattern, parent=False, inner_page_link=None) -> bs4.ResultSet or str:
        """Retrieve the information from the page object safely (using appropriate checks)
         using the selector

         Return a ResultSet
         """

        # if the information to be retrieved can't be gotten from the current page
        # go deeper into the page using the provided link and scraping the required information
        if selector['access_inner_page']:
            if inner_page_link is None:
                return
            absolute_link = f"{self.site.url}{inner_page_link}"
            page_obj = self.get_page(absolute_link)

        if page_obj is not None:
            # there are two ways of accessing information from a page
            # using the more powerful css selector style or the more conventional
            # html tag, attributes retrieval and or using regex

            if not pattern:
                child_obj = page_obj.select(selector['selector'])

                # in rare cases the information (tag) you are trying to access
                # can only be reached through asking the children who their parent is

                # (note this can happen if the tag you wish to access do not have a
                # definite class or id attributed to it, but it immediate children
                # contains definite attributes
                if parent:
                    child_obj = page_obj.select(selector['selector'])

                    # if the tag doesn't contain the element
                    if child_obj:
                        child_obj = child_obj[0]
                        child_obj = child_obj.parent
            else:
                child_obj = page_obj.find_all('a', href=re.compile(selector['selector']))
            if child_obj is not None and len(child_obj) > 0:
                return child_obj
        return ""

    def get_links(self, page_url, selector) -> List[str]:
        """Get the links needed to navigate to the next page. This is a special function
        that ensures the whole website is fully searched"""
        links = []
        page = self.get_page(page_url)

        if page is not None:
            for tag in self.safe_get(page, selector, True):
                if 'href' in tag.attrs:
                    link = tag.attrs['href']
                    links.append(link)
        return links

    def parse(self, url) -> list or None:
        """Process the page to access and save the needed information as defined in the
        Website information object"""
        bs = self.get_page(url)
        if bs is not None:
            contents = []

            housing_list = self.safe_get(bs, self.site.result_listing, self.site.use_patterns)

            if housing_list is not None:
                for house in housing_list:
                    title = self.safe_get(house, self.site.title_tag, self.site.use_patterns)
                    url = self.safe_get(house, self.site.url_tag, self.site.use_patterns)
                    address = self.safe_get(house, self.site.address_tag, self.site.use_patterns)

                    no_of_bedroom = self.safe_get(house, self.site.no_of_bedrooms_tag, self.site.use_patterns,
                                                  self.site.get_parent)
                    no_of_bathroom = self.safe_get(house, self.site.no_of_bathrooms_tag, self.site.use_patterns,
                                                   self.site.get_parent)
                    no_of_toilet = self.safe_get(house, self.site.no_of_toilets_tag, self.site.use_patterns,
                                                 self.site.get_parent)
                    agent_contact = self.safe_get(house, self.site.agent_contact_tag, self.site.use_patterns)
                    price = self.safe_get(house, self.site.price_tag, self.site.use_patterns)
                    description = self.safe_get(house, self.site.description_tag, self.site.use_patterns)

                    # temporarily store the scraped information of a particular entity into the Content object
                    content = Content(title, url, address, no_of_bedroom,
                                      no_of_bathroom, no_of_toilet, agent_contact, price,
                                      description
                                      # building_type
                                      )
                    contents.append(content)
            return contents

    def clean_text(self, text) -> str:
        """Removing trailing whitespace, commas, from text"""
        clean_text = re.sub("[,\n+]", ' ', text).strip()

        return clean_text

    def save_content_to_file(self, contents) -> None:
        """Handles the task of permanently saving the stored Content into the database,
        performing any necessary preprocessing and error handling in the process"""
        for info in contents:
            try:
                Title = self.clean_text(info.title[0].get_text())
            except:
                Title = ""

            try:
                URL = f"{self.site.url}{info.url[0].attrs['href']}"
            except:
                URL = ""

            try:
                Address = self.clean_text(info.address[0].get_text())
            except:
                Address = "0"

            try:
                if self.site.get_parent:
                    No_of_Bedrooms = self.clean_text(info.no_of_bedrooms.get_text())
                else:
                    No_of_Bedrooms = self.clean_text(info.no_of_bedrooms[0].get_text())
            except:
                No_of_Bedrooms = "0"

            try:
                if self.site.get_parent:
                    No_of_Bathrooms = self.clean_text(info.no_of_bathrooms.get_text())
                else:
                    No_of_Bathrooms = self.clean_text(info.no_of_bathrooms[0].get_text())
            except:
                No_of_Bathrooms = "0"

            try:
                if self.site.get_parent:
                    No_of_Toilets = self.clean_text(info.no_of_toilets.get_text())
                else:
                    No_of_Toilets = self.clean_text(info.no_of_toilets[0].get_text())
            except:
                No_of_Toilets = "0"

            try:
                Price = self.clean_text(info.price[0].get_text()).strip()[1:]
            except:
                Price = ""

            try:
                Agent_Contact = self.clean_text(info.agent_contact[0].get_text())
            except:
                Agent_Contact = ""

            try:
                Description = self.clean_text(info.description[0].get_text())
            except:
                Description = ""


            compile_info = [Title, URL, Address, No_of_Bedrooms,
                            No_of_Bathrooms, No_of_Toilets, Price, Agent_Contact]

            # further format the strings to be a valid database str literal
            # thereby preventing the database to throw an error
            Title = Title.replace("'", "''")
            Address = Address.replace("'", "''")
            Agent_Contact = Agent_Contact.replace("'", "''")
            Price = Price.replace("'", "''")
            Description = Description.replace("'", "''")

            # create a query string containing the well formatted strings (information)
            # to be executed by the database
            query = fr"""INSERT INTO house_rent (title, url, address, no_of_bedrooms, no_of_bathrooms, no_of_toilets, agent_contact, price, description)
                VALUES ('{Title}', '{URL}', '{Address}', '{No_of_Bedrooms}', '{No_of_Bathrooms}', '{No_of_Toilets}', '{Agent_Contact}', '{Price}', '{Description}')
                        """

            self.database.query(query)

    def crawl(self) -> None:
        """The true workhorse of the Crawl object. It crawls through the website
         and retrieving the necessary information using the appropriate method"""
        print("Crawler started")

        # for the first page current be searched
        content = self.parse(self.site.search_url)
        if content is not None and len(content) > 0:
            # save the information to the database
            self.save_content_to_file(content)

        links_to_visit = []
        # retrieve the links to be further scraped from the current page
        links = self.get_links(self.site.search_url, self.site.result_url)

        if len(links) > 0:
            links_to_visit.extend(links)

        self.visited.add(self.site.search_url)

        # subsequent page after the first page
        for link in links_to_visit:
            if link in self.visited:
                continue
            else:
                if not self.site.absolute_url:
                    absolute_link = f"{self.site.url}{link}"
                else:
                    absolute_link = link

                # remove the needed information from the current page
                content = self.parse(absolute_link)

                try:
                    if content is not None or len(content) >= 0:
                        # add the link of the current page to the list of page
                        # already visited
                        self.visited.add(link)

                        # save the parsed content
                        # self.content.extend(content)

                        # get new links from the current page to add to the list
                        # of pages to be visited
                        links = self.get_links(absolute_link, self.site.result_url)
                        links_to_visit.extend(links)

                        self.save_content_to_file(content)

                        print("Done parsing {}".format(absolute_link))
                    else:
                        print("Problem reading from \t{}".format(link))
                except TypeError:
                    print(f"Have encounter an error at {link}")


def create_options_dict(selector, access_inner_page=False) -> dict:
    """A simple utilty function. Returns a dict

    selector: str
        The way in which the crawler can have access to the information
    access_inner_page: bool
        Instruct the crawler on whether to go deeper into a page to retrieve the
        information using the selector
    """
    return {"selector": selector, 'access_inner_page': access_inner_page}


nigeria_property = {
    "filename": "nigeria_property.csv",
    'url': "https://nigeriapropertycentre.com",
    'search_url': "https://nigeriapropertycentre.com/for-rent",
    "result_listing": create_options_dict('div[itemtype*=ListItem]'),
    "result_url": create_options_dict("/for-rent\?.*"),
    "use_patterns": False,
    "absolute_url": False,

    "title_tag": create_options_dict("div[class*=wp-block-title] a"),
    "url_tag": create_options_dict("div[class*=wp-block-title] a"),
    "address_tag": create_options_dict("address[class=voffset-bottom-10]"),
    "no_of_bedrooms_tag": create_options_dict(".fa-bed"),
    "no_of_bathrooms_tag": create_options_dict(".fa-bath"),
    "no_of_toilets_tag": create_options_dict(".fa-toilet"),
    "agent_contact_tag": create_options_dict(".marketed-by"),
    "price_tag": create_options_dict(".pull-sm-left"),
    "description_tag": create_options_dict(".description"),
    # "type_tag": create_options_dict(".table-bordered tr td", True),

    "get_parent": True,
}

property_pro = {
    "filename": "propertypro.csv",
    "url": "https://www.propertypro.ng",
    "search_url": "https://www.propertypro.ng/property-for-rent",
    "result_listing": create_options_dict(".listings-property"),
    "result_url": create_options_dict("/.+\?.+\d"),
    "use_patterns": False,
    "absolute_url": False,

    "title_tag": create_options_dict(".listings-property-title"),
    "url_tag": create_options_dict(".single-room-text > a"),
    "address_tag": create_options_dict(".single-room-text > h4"),
    "no_of_bedrooms_tag": create_options_dict(".fur-areea span:nth-child(1)"),
    "no_of_bathrooms_tag": create_options_dict(".fur-areea span:nth-child(2)"),
    "no_of_toilets_tag": create_options_dict(".fur-areea span:nth-child(3)"),
    "agent_contact_tag": create_options_dict(".phone-icon"),
    "price_tag": create_options_dict(".n50 h3"),
    "description_tag": create_options_dict(".description-text", True),

    "get_parent": False,
}

websites = [nigeria_property, property_pro]


def start_crawler(sites):
    """Setup the website structure and launch the crawler
    The crawler crawls through the websites using the website search utility
    """

    # create a database instance
    # you can the default database connection settings
    # to one of your setting using the database instance methods
    database = Database()
    try:
        for site in sites:
            site_structure = Website(**site)
            site_crawler = Crawler(site_structure, database)
            site_crawler.crawl()
            print("\n\nDone with a website\n\n\n")
    except BaseException:
        print("Closing database...", end=" ")
        # close the database connection only if it is open
        if database.database_status:
            database.close_database_connection()
        print("closed")


start_crawler(websites)
